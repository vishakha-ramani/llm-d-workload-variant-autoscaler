package queueingmodel

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/engines/analyzers/queueingmodel/tuner"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/interfaces"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/pkg/analyzer"
	"gonum.org/v1/gonum/mat"
	ctrl "sigs.k8s.io/controller-runtime"
)

// QueueingModelAnalyzer implements interfaces.Analyzer.
// It performs SLO-driven capacity analysis by:
//  1. Learning model parameters (alpha, beta, gamma) online via Kalman filter
//  2. Using queueing model to predict max request rate that meets TTFT/ITL SLOs
//  3. Computing capacity signals for scaling decisions

type QueueingModelAnalyzer struct { // all data that is stored -- map of variants?
	// paramStore caches learned parameters per variant
	paramStore *ParameterStore
}

// NewQueueingModelAnalyzer creates a new queueing model analyzer instance.
func NewQueueingModelAnalyzer() *QueueingModelAnalyzer {
	return &QueueingModelAnalyzer{
		paramStore: NewParameterStore(),
	}
}

// Name implements interfaces.Analyzer.
func (a *QueueingModelAnalyzer) Name() string {
	return "queueing-model"
}

// Analyze implements interfaces.Analyzer.
// Called for each model.
//
// If we fail to analyze a model (bad config, no learned parameters, no
// variant capacities), Analyze returns an error. The caller is expected
// to retry on subsequent reconcile cycles; the error persists until the
// underlying condition is resolved (e.g., tuner succeeds, metrics become
// available). The one exception is missing SLO targets — that yields an
// empty result rather than an error, since capacity cannot be defined
// without an SLO but the situation is not necessarily erroneous.
func (a *QueueingModelAnalyzer) Analyze(
	ctx context.Context,
	input interfaces.AnalyzerInput,
) (*interfaces.AnalyzerResult, error) {
	logger := ctrl.LoggerFrom(ctx)

	// Extract config
	qConfig, ok := input.Config.(*QueueingModelConfig)
	if !ok {
		return nil, fmt.Errorf("expected *QueueingModelConfig, got %T", input.Config)
	}

	// Update parameters (tuner) for all variants associated with the model
	if qConfig.TuningEnabled {
		a.updateVariantParameters(ctx, input.Namespace, input.ReplicaMetrics, qConfig)
	}

	// Get SLO targets
	sloTarget := a.getSLOTarget(ctx, input.Namespace, input.ModelID, qConfig, input.ReplicaMetrics)
	if sloTarget == nil {
		logger.Info("No SLO targets", "modelID", input.ModelID)
		return a.emptyResult(input), nil
	}

	// Compute capacities
	variantCapacities := a.computeAllVariantCapacities(
		ctx, input.Namespace, input.ReplicaMetrics, input.VariantStates, sloTarget,
	)
	if len(variantCapacities) == 0 {
		return nil, fmt.Errorf("could not compute variant capacities for model %q", input.ModelID)
	}

	// Aggregate and build result
	totalSupply, totalDemand := aggregateCapacities(variantCapacities)
	utilization := 0.0
	if totalSupply > 0 {
		utilization = totalDemand / totalSupply
	}

	return &interfaces.AnalyzerResult{
		AnalyzerName:      a.Name(),
		ModelID:           input.ModelID,
		Namespace:         input.Namespace,
		AnalyzedAt:        time.Now(),
		VariantCapacities: variantCapacities,
		TotalSupply:       totalSupply,
		TotalDemand:       totalDemand,
		Utilization:       utilization,
		RequiredCapacity:  max(0, totalDemand-totalSupply),
		SpareCapacity:     max(0, totalSupply-totalDemand),
	}, nil
}

func (a *QueueingModelAnalyzer) getSLOTarget(
	ctx context.Context,
	namespace string,
	modelID string,
	config *QueueingModelConfig,
	metrics []interfaces.ReplicaMetrics,
) *SLOTarget {
	// First try explicit config
	if slo := config.GetSLOForModel(namespace, modelID); slo != nil {
		return slo
	}
	// Infer SLO from the queueing model and observed metrics
	return a.guessSLOFromMetrics(ctx, namespace, config, metrics)
}

func (a *QueueingModelAnalyzer) updateVariantParameters(
	ctx context.Context,
	namespace string,
	metrics []interfaces.ReplicaMetrics,
	config *QueueingModelConfig,
) {
	logger := ctrl.LoggerFrom(ctx)

	// Group metrics by variant
	variantMetrics := groupMetricsByVariant(metrics)

	// Run tuner for each variant
	for variantName, replicaMetrics := range variantMetrics {
		// Build environment from aggregated replica metrics
		env, err := a.buildEnvironmentFromMetrics(ctx, variantName, replicaMetrics)
		if err != nil {
			logger.V(1).Info("Failed to build environment for variant",
				"variant", variantName,
				"namespace", namespace,
				"error", err)
			continue
		}

		// Create a tuner for this variant
		tuner, err := a.createTuner(ctx, namespace, variantName, env, config.FilterConfig)
		if err != nil {
			logger.V(1).Info("Failed to get/create tuner for variant",
				"variant", variantName,
				"namespace", namespace,
				"error", err)
			continue
		}

		// Run tuner to learn parameters
		results, err := tuner.Run()
		if err != nil {
			logger.V(1).Info("Tuner failed for variant",
				"variant", variantName,
				"namespace", namespace,
				"error", err)
			continue
		}

		// Store tuned parameters
		a.storeParametersFromResults(namespace, variantName, results)

		// Log tuning results
		if results.ValidationFailed {
			logger.Info("Tuner validation failed, using previous state",
				"variant", variantName,
				"namespace", namespace,
				"NIS", results.NIS)
		} else {
			logger.V(1).Info("Parameters tuned successfully",
				"variant", variantName,
				"namespace", namespace,
				"alpha", results.ServiceParms.Alpha,
				"beta", results.ServiceParms.Beta,
				"gamma", results.ServiceParms.Gamma,
				"NIS", results.NIS)
		}
	}
}

// calculate capacities for all variants of a model in a given namespace
func (a *QueueingModelAnalyzer) computeAllVariantCapacities(
	ctx context.Context,
	namespace string,
	replicaMetrics []interfaces.ReplicaMetrics,
	variantStates []interfaces.VariantReplicaState,
	sloTarget *SLOTarget,
) []interfaces.VariantCapacity {
	logger := ctrl.LoggerFrom(ctx)

	variantCapacities := make([]interfaces.VariantCapacity, 0)
	for _, variantState := range variantStates {
		variantName := variantState.VariantName

		// Accumulate data over all pod replicas of the variant
		totalInputTokens := float32(0.0)
		totalOutputTokens := float32(0.0)
		totalArrivalRate := float32(0.0)
		var maxBatchSize int64
		numPods := 0
		for _, rm := range replicaMetrics {
			if rm.VariantName != variantName || rm.Namespace != namespace {
				continue
			}

			// Skip pods with zero arrival rate (no traffic being dispatched)
			if rm.ArrivalRate <= 0 {
				continue
			}

			totalArrivalRate += float32(rm.ArrivalRate)
			totalInputTokens += float32(rm.AvgInputTokens)
			totalOutputTokens += float32(rm.AvgOutputTokens)

			// MaxBatchSize is per-deployment (same for all pods of a variant),
			// so any pod's value is representative
			if rm.MaxBatchSize > 0 {
				maxBatchSize = rm.MaxBatchSize
			}
			numPods++
		}
		if numPods == 0 {
			logger.Info("No replicas with traffic to calculate capacity for variant", "variant", variantName)
			continue
		}

		// Fall back to default if MaxBatchSize was not parsed from deployment args
		if maxBatchSize <= 0 {
			maxBatchSize = DefaultMaxBatchSize
		}

		// Prefill and decode parameters
		params := a.paramStore.Get(namespace, variantName)
		if params == nil {
			logger.Info("No parameters found for variant", "variant", variantName)
			continue
		}

		// Create queue analyzer
		config := &analyzer.Configuration{
			MaxBatchSize: int(maxBatchSize),
			MaxQueueSize: DefaultMaxQueueSize,
			ServiceParms: &analyzer.ServiceParms{
				Alpha: params.Alpha,
				Beta:  params.Beta,
				Gamma: params.Gamma,
			},
		}

		requestSize := &analyzer.RequestSize{
			AvgInputTokens:  totalInputTokens / float32(numPods),
			AvgOutputTokens: totalOutputTokens / float32(numPods),
		}

		targetPerf := &analyzer.TargetPerf{
			TargetTTFT: sloTarget.TargetTTFT,
			TargetITL:  sloTarget.TargetITL,
		}

		queueAnalyzer, err := analyzer.NewQueueAnalyzer(config, requestSize)
		if err != nil {
			logger.Info("Failed to create queue analyzer for variant", "variant", variantName, "error", err)
			continue
		}

		var maxRequestRate float32
		if _, metrics, _, err := queueAnalyzer.Size(targetPerf); err != nil {
			logger.Info("Failed to calculate max request rate for variant", "variant", variantName, "error", err)
			continue
		} else {
			maxRequestRate = metrics.Throughput
		}

		if maxRequestRate == 0 {
			logger.Info("Failed to calculate max request rate for variant", "variant", variantName)
			continue
		}

		desiredNumReplicas := math.Ceil(float64(totalArrivalRate) / float64(maxRequestRate))
		if desiredNumReplicas == 0 {
			desiredNumReplicas = 1
		}
		arrivalRatePerReplica := totalArrivalRate / float32(desiredNumReplicas)

		replicaCount := variantState.CurrentReplicas
		variantCapacity := interfaces.VariantCapacity{
			VariantName:        variantName,
			AcceleratorName:    replicaMetrics[0].AcceleratorName,
			Cost:               replicaMetrics[0].Cost * float64(replicaCount),
			TotalCapacity:      desiredNumReplicas * float64(maxRequestRate),
			PerReplicaCapacity: float64(maxRequestRate),
			TotalDemand:        float64(totalArrivalRate),
			Utilization:        float64(arrivalRatePerReplica / maxRequestRate),

			ReplicaCount:    variantState.CurrentReplicas,
			PendingReplicas: variantState.PendingReplicas,
		}
		variantCapacities = append(variantCapacities, variantCapacity)
	}

	return variantCapacities
}

func (a *QueueingModelAnalyzer) emptyResult(input interfaces.AnalyzerInput) *interfaces.AnalyzerResult {
	return &interfaces.AnalyzerResult{
		AnalyzerName: a.Name(),
		ModelID:      input.ModelID,
		Namespace:    input.Namespace,
		AnalyzedAt:   time.Now(),
	}
}

func aggregateCapacities(capacities []interfaces.VariantCapacity) (supply, demand float64) {
	for _, c := range capacities {
		supply += c.TotalCapacity
		demand += c.TotalDemand
	}
	return
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// guessSLOFromMetrics infers SLO targets from the queueing model when no
// explicit SLO configuration is provided.
//
// The SLO is defined using the idle-latency multiplier approach: the queueing
// delay (T_iter) is allowed to inflate by a fixed multiplier k relative to the
// idle baseline α, while deterministic work components remain at their true cost.
//
// Formulas (all values in milliseconds):
//
//	TargetTTFT = k×α + (β+γ)×i_l
//	TargetITL  = k×α + β + γ×(i_l + (o_l+1)/2)
//
// This gives exact utilization correspondence: ρ = 1 - 1/k.
// k is a fixed constant (not dependent on system state) because SLOs are contracts.
//
// When learned parameters are unavailable, falls back to observed latencies
// with a headroom multiplier, capped at reasonable maximums.
func (a *QueueingModelAnalyzer) guessSLOFromMetrics(
	ctx context.Context,
	namespace string,
	config *QueueingModelConfig,
	metrics []interfaces.ReplicaMetrics,
) *SLOTarget {
	logger := ctrl.LoggerFrom(ctx)

	wm := aggregateWorkloadMetrics(metrics)
	if wm == nil {
		return nil
	}

	// Get the SLO multiplier k
	k := config.SLOMultiplier
	if k <= 1.0 {
		k = DefaultSLOMultiplier
	}

	// Try theory-based SLO: find a variant with learned parameters
	variantMetrics := groupMetricsByVariant(metrics)
	for variantName := range variantMetrics {
		params := a.paramStore.Get(namespace, variantName)
		if params == nil {
			continue
		}

		alpha := float64(params.Alpha)
		beta := float64(params.Beta)
		gamma := float64(params.Gamma)

		if alpha <= 0 || beta <= 0 || gamma <= 0 {
			continue
		}

		// T_iter at SLO utilization: k × α = α/(1-ρ) where ρ = 1-1/k
		tIterSLO := k * alpha

		// Deterministic work — NOT inflated by k
		prefillWork := (beta + gamma) * wm.avgInputTokens
		decodeWork := beta + gamma*(wm.avgInputTokens+(wm.avgOutputTokens+1.0)/2.0)

		ttftSLO := tIterSLO + prefillWork
		itlSLO := tIterSLO + decodeWork

		logger.V(1).Info("Inferred SLO from queueing model",
			"variant", variantName,
			"k", k,
			"alpha", alpha, "beta", beta, "gamma", gamma,
			"avgInputTokens", wm.avgInputTokens,
			"avgOutputTokens", wm.avgOutputTokens,
			"TargetTTFT_ms", ttftSLO,
			"TargetITL_ms", itlSLO,
		)

		return &SLOTarget{
			TargetTTFT: float32(ttftSLO),
			TargetITL:  float32(itlSLO),
		}
	}

	// Fallback: use observed latencies with headroom when learned params
	// are not yet available (cold start / early tuning cycles)
	return a.fallbackSLOFromObservations(ctx, wm)
}

// fallbackSLOFromObservations creates SLO targets from observed TTFT/ITL
// with a headroom multiplier and reasonable caps. Used during cold start
// before the Kalman filter has learned hardware parameters.
func (a *QueueingModelAnalyzer) fallbackSLOFromObservations(
	ctx context.Context,
	wm *workloadMetrics,
) *SLOTarget {
	if wm.avgTTFT <= 0 || wm.avgITL <= 0 {
		return nil
	}

	logger := ctrl.LoggerFrom(ctx)

	// Convert seconds → milliseconds and apply headroom
	ttft := math.Min(wm.avgTTFT*1000.0*DefaultFallbackHeadroom, DefaultMaxFallbackTTFT)
	itl := math.Min(wm.avgITL*1000.0*DefaultFallbackHeadroom, DefaultMaxFallbackITL)

	logger.V(1).Info("Using fallback SLO from observations",
		"observedTTFT_s", wm.avgTTFT,
		"observedITL_s", wm.avgITL,
		"TargetTTFT_ms", ttft,
		"TargetITL_ms", itl,
	)

	return &SLOTarget{
		TargetTTFT: float32(ttft),
		TargetITL:  float32(itl),
	}
}

// workloadMetrics holds aggregated workload characteristics across replicas.
type workloadMetrics struct {
	avgInputTokens  float64
	avgOutputTokens float64
	avgTTFT         float64 // seconds
	avgITL          float64 // seconds
}

// aggregateWorkloadMetrics averages token sizes and latencies across replicas
// that have active traffic. Returns nil if no replicas have traffic.
func aggregateWorkloadMetrics(metrics []interfaces.ReplicaMetrics) *workloadMetrics {
	var totalInputToks, totalOutputToks float64
	var totalTTFT, totalITL float64
	validPods := 0

	for _, rm := range metrics {
		if rm.ArrivalRate <= 0 {
			continue
		}
		totalInputToks += rm.AvgInputTokens
		totalOutputToks += rm.AvgOutputTokens
		totalTTFT += rm.AvgTTFT
		totalITL += rm.AvgITL
		validPods++
	}

	if validPods == 0 {
		return nil
	}

	return &workloadMetrics{
		avgInputTokens:  totalInputToks / float64(validPods),
		avgOutputTokens: totalOutputToks / float64(validPods),
		avgTTFT:         totalTTFT / float64(validPods),
		avgITL:          totalITL / float64(validPods),
	}
}

// groupMetricsByVariant groups replica metrics by variant name.
func groupMetricsByVariant(metrics []interfaces.ReplicaMetrics) map[string][]interfaces.ReplicaMetrics {
	grouped := make(map[string][]interfaces.ReplicaMetrics)
	for _, m := range metrics {
		grouped[m.VariantName] = append(grouped[m.VariantName], m)
	}
	return grouped
}

// buildEnvironmentFromMetrics creates a tuner Environment from aggregated replica metrics.
// Aggregates per-replica metrics (arrival rate, avg tokens, TTFT, ITL, max batch size)
// into a single Environment representing the variant's current operating state.
// Returns error if required metrics are unavailable.
func (a *QueueingModelAnalyzer) buildEnvironmentFromMetrics(
	ctx context.Context,
	variantName string,
	metrics []interfaces.ReplicaMetrics,
) (*tuner.Environment, error) {
	if len(metrics) == 0 {
		return nil, fmt.Errorf("no replica metrics for variant %s", variantName)
	}

	// MaxBatchSize is per-deployment (same for all replicas of a variant),
	// so we extract it once from the first replica that has it.
	var maxBatchSize int64
	for _, rm := range metrics {
		if rm.MaxBatchSize > 0 {
			maxBatchSize = rm.MaxBatchSize
			break
		}
	}

	// Aggregate per-pod traffic metrics across replicas
	var totalArrivalRate float64
	var totalInputToks, totalOutputToks float64
	var totalTTFT, totalITL float64
	validPods := 0

	for _, rm := range metrics {
		// Skip pods without arrival rate (no traffic)
		if rm.ArrivalRate <= 0 {
			continue
		}

		totalArrivalRate += rm.ArrivalRate
		totalInputToks += rm.AvgInputTokens
		totalOutputToks += rm.AvgOutputTokens
		totalTTFT += rm.AvgTTFT
		totalITL += rm.AvgITL
		validPods++
	}

	if validPods == 0 {
		return nil, fmt.Errorf("no replicas with traffic for variant %s", variantName)
	}

	avgInputToks := totalInputToks / float64(validPods)
	avgOutputToks := totalOutputToks / float64(validPods)
	avgTTFT := totalTTFT / float64(validPods)
	avgITL := totalITL / float64(validPods)

	if maxBatchSize <= 0 {
		maxBatchSize = DefaultMaxBatchSize
	}

	// Convert arrival rate from requests/sec to requests/min for tuner
	lambdaPerMinute := totalArrivalRate * 60.0

	// Convert TTFT and ITL from seconds to milliseconds for tuner
	avgTTFTMs := avgTTFT * 1000.0
	avgITLMs := avgITL * 1000.0

	env := &tuner.Environment{
		Lambda:        float32(lambdaPerMinute),
		AvgInputToks:  float32(avgInputToks),
		AvgOutputToks: float32(avgOutputToks),
		MaxBatchSize:  int(maxBatchSize),
		AvgTTFT:       float32(avgTTFTMs),
		AvgITL:        float32(avgITLMs),
	}

	if !env.Valid() {
		return nil, fmt.Errorf("invalid environment for variant %s: lambda=%.2f, inputToks=%.2f, outputToks=%.2f, TTFT=%.2fms, ITL=%.2fms, maxBatch=%d",
			variantName, lambdaPerMinute, avgInputToks, avgOutputToks, avgTTFTMs, avgITLMs, maxBatchSize)
	}

	return env, nil
}

// createTuner creates a new tuner instance for a variant.
// If parameters exist in the store, uses the stored state and covariance.
// Otherwise, attempts to guess initial state from environment metrics.
func (a *QueueingModelAnalyzer) createTuner(
	ctx context.Context,
	namespace string,
	variantName string,
	env *tuner.Environment,
	filterConfig *tuner.FilterData,
) (*tuner.Tuner, error) {
	logger := ctrl.LoggerFrom(ctx)

	// Check if we have existing parameters
	existingParams := a.paramStore.Get(namespace, variantName)

	// Get base tuner config (uses user config or defaults)
	tunerConfig := a.getTunerConfig(filterConfig, env)

	if existingParams != nil {
		// Restore state and covariance from previous tuning cycle
		logger.V(1).Info("Restoring tuner state from parameter store",
			"variant", variantName,
			"namespace", namespace,
			"alpha", existingParams.Alpha,
			"beta", existingParams.Beta,
			"gamma", existingParams.Gamma)

		tunerConfig.ModelData.InitState = existingParams.State
		flatCov := flattenCovariance(existingParams.Covariance)
		if flatCov != nil {
			tunerConfig.ModelData.InitCovarianceMatrix = flatCov
		}
	} else {
		// No existing parameters - attempt to guess initial state from metrics
		logger.V(1).Info("No existing parameters found, attempting to guess initial state",
			"variant", variantName,
			"namespace", namespace)

		state, err := a.guessInitState(ctx, env)
		if err != nil {
			logger.V(1).Info("Failed to guess initial state, using defaults",
				"variant", variantName,
				"namespace", namespace,
				"error", err)
			// tunerConfig already has default InitState, so we can proceed
		} else {
			logger.V(1).Info("Using guessed initial state",
				"variant", variantName,
				"namespace", namespace,
				"alpha", state[tuner.StateIndexAlpha],
				"beta", state[tuner.StateIndexBeta],
				"gamma", state[tuner.StateIndexGamma])
			tunerConfig.ModelData.InitState = state
			// Update bounds based on guessed state
			tunerConfig.ModelData.MinState = getFactoredState(state, tuner.DefaultMinStateFactor)
			tunerConfig.ModelData.MaxState = getFactoredState(state, tuner.DefaultMaxStateFactor)
		}
	}

	// Create new tuner instance with environment
	t, err := tuner.NewTuner(tunerConfig, env)
	if err != nil {
		return nil, fmt.Errorf("failed to create tuner: %w", err)
	}

	return t, nil
}

// getTunerConfig builds a TunerConfigData for a specific variant.
// FilterData is shared for all variants (from filterConfig or defaults). TunerModelData is always
// built per-variant from the environment because different variants run on
// different accelerators with different latency characteristics.
func (a *QueueingModelAnalyzer) getTunerConfig(filterConfig *tuner.FilterData, env *tuner.Environment) *tuner.TunerConfigData {
	// FilterData: use user-provided config or defaults
	filterData := tuner.FilterData{
		GammaFactor: tuner.DefaultGammaFactor,
		ErrorLevel:  tuner.DefaultErrorLevel,
		TPercentile: tuner.DefaultTPercentile,
	}
	if filterConfig != nil {
		filterData = *filterConfig
	}

	// TunerModelData: always built per-variant from environment
	// State vector: [alpha, beta, gamma]
	// Using reasonable initial estimates (will be refined by filter)
	initState := []float64{10.0, 0.5, 0.1} // alpha=10ms, beta=0.5ms/token, gamma=0.1ms/token

	// Percent change per parameter (using DefaultPercentChange)
	percentChange := []float64{
		tuner.DefaultPercentChange,
		tuner.DefaultPercentChange,
		tuner.DefaultPercentChange,
	}

	// State bounds using factors
	minState := getFactoredState(initState, tuner.DefaultMinStateFactor)
	maxState := getFactoredState(initState, tuner.DefaultMaxStateFactor)

	// Expected observations [TTFT, ITL] in milliseconds
	// Use actual observations from environment if valid, otherwise use typical values
	expectedObservations := []float64{50.0, 5.0} // Typical TTFT=50ms, ITL=5ms
	if env != nil && env.Valid() {
		expectedObservations = []float64{float64(env.AvgTTFT), float64(env.AvgITL)}
	}

	return &tuner.TunerConfigData{
		FilterData: filterData,
		ModelData: tuner.TunerModelData{
			InitState:            initState,
			InitCovarianceMatrix: nil, // Will use defaults in tuner
			PercentChange:        percentChange,
			BoundedState:         true,
			MinState:             minState,
			MaxState:             maxState,
			ExpectedObservations: expectedObservations,
		},
	}
}

// storeParametersFromResults saves tuned results to the parameter store.
func (a *QueueingModelAnalyzer) storeParametersFromResults(
	namespace string,
	variantName string,
	results *tuner.TunedResults,
) {
	// Extract state vector and covariance matrix
	// State vector has 3 elements: [alpha, beta, gamma]
	stateVec := make([]float64, 3)
	stateVec[tuner.StateIndexAlpha] = float64(results.ServiceParms.Alpha)
	stateVec[tuner.StateIndexBeta] = float64(results.ServiceParms.Beta)
	stateVec[tuner.StateIndexGamma] = float64(results.ServiceParms.Gamma)

	covariance := matrixToSlice2D(results.Covariance)

	params := &LearnedParameters{
		Alpha:       results.ServiceParms.Alpha,
		Beta:        results.ServiceParms.Beta,
		Gamma:       results.ServiceParms.Gamma,
		LastUpdated: time.Now(),
		NIS:         results.NIS,
		State:       stateVec,
		Covariance:  covariance,
	}

	a.paramStore.Set(namespace, variantName, params)
}

// flattenCovariance converts a 2D covariance matrix to a flat slice.
func flattenCovariance(cov [][]float64) []float64 {
	if len(cov) == 0 {
		return nil
	}
	n := len(cov)
	flat := make([]float64, 0, n*n)
	for i := range n {
		flat = append(flat, cov[i]...)
	}
	return flat
}

// matrixToSlice2D converts a gonum mat.Dense to a 2D slice.
func matrixToSlice2D(m *mat.Dense) [][]float64 {
	if m == nil {
		return nil
	}
	rows, cols := m.Dims()
	result := make([][]float64, rows)
	for i := range rows {
		result[i] = make([]float64, cols)
		for j := 0; j < cols; j++ {
			result[i][j] = m.At(i, j)
		}
	}
	return result
}

// getFactoredState multiplies each element in state by multiplier and returns the new slice.
func getFactoredState(state []float64, multiplier float64) []float64 {
	result := make([]float64, len(state))
	for i, val := range state {
		result[i] = val * multiplier
	}
	return result
}

// guessInitState makes an initial guess of the state estimates based on observed metrics.
// Uses the queueing model from the paper to derive parameters alpha, beta, gamma from observed TTFT and ITL.
//
// From the queueing model:
//
//	T_p (TTFT) = T_iter + (beta + gamma) × i_l                    ... (eq 12)
//	T_g (ITL)  = T_iter + beta + gamma × (i_l + (o_l + 1)/2)     ... (eq 13)
//
// Where:
//   - alpha: baseline iteration overhead (embedded in T_iter)
//   - beta: compute time per token
//   - gamma: KV cache memory access time per token
//   - i_l: average input tokens
//   - o_l: average output tokens
func (a *QueueingModelAnalyzer) guessInitState(ctx context.Context, env *tuner.Environment) ([]float64, error) {
	// Validate environment
	if env == nil || !env.Valid() {
		return nil, fmt.Errorf("invalid environment for guessing initial state")
	}

	// Extract observed metrics
	ttft := float64(env.AvgTTFT)             // T_p in paper
	itl := float64(env.AvgITL)               // T_g in paper
	inputToks := float64(env.AvgInputToks)   // i_l in paper
	outputToks := float64(env.AvgOutputToks) // o_l in paper

	// Validate inputs
	if ttft <= 0 || itl <= 0 || inputToks <= 0 || outputToks <= 0 {
		return nil, fmt.Errorf("invalid metrics: TTFT=%.2f, ITL=%.2f, inputToks=%.2f, outputToks=%.2f",
			ttft, itl, inputToks, outputToks)
	}

	// Step 1: Estimate alpha (baseline iteration overhead) as a fraction of ITL
	// The iteration time T_iter is embedded in both TTFT and ITL observations.
	// At light-to-moderate load, T_iter is approximately alpha + small_overhead
	// We use ITL as a proxy since it includes T_iter plus minimal decode work
	alpha := tuner.BaseFactor * itl // BaseFactor ≈ 0.9

	// Step 2: From TTFT equation (eq 12), solve for (beta + gamma)
	// TTFT = T_iter + (beta + gamma) × i_l
	// Assuming T_iter ≈ α at the observed load:
	// (beta + gamma) = (TTFT - alpha) / i_l
	sumBetaGamma := (ttft - alpha) / inputToks

	if sumBetaGamma < 0 {
		return nil, fmt.Errorf("invalid derived sum(beta+gamma)=%.6f < 0, check BaseFactor or metrics", sumBetaGamma)
	}

	// Step 3: From ITL equation (eq 13), solve for the beta and gamma relationship
	// ITL = T_iter + beta + gamma × (i_l + (o_l + 1)/2)
	// Assuming T_iter is approximately alpha:
	// beta + gamma × (i_l + (o_l + 1)/2) = ITL - alpha
	//
	// Substitute beta = sumBetaGamma - gamma:
	// (sumBetaGamma - gamma) + gamma × (i_l + (o_l + 1)/2) = ITL - alpha
	// sumBetaGamma + gamma × (i_l + (o_l + 1)/2 - 1) = ITL - alpha
	//
	// Solve for gamma:
	denominator := inputToks + (outputToks+1)/2 - 1
	if denominator <= 0 {
		return nil, fmt.Errorf("invalid denominator=%.6f for gamma calculation", denominator)
	}

	gamma := ((itl - alpha) - sumBetaGamma) / denominator

	// Step 4: Solve for beta
	beta := sumBetaGamma - gamma

	// Validate results: all parameters must be positive
	if alpha <= 0 {
		return nil, fmt.Errorf("derived alpha=%.6f <= 0 (ITL=%.2f, BaseFactor=%.2f)",
			alpha, itl, tuner.BaseFactor)
	}
	if beta <= 0 {
		return nil, fmt.Errorf("derived beta=%.6f <= 0 (TTFT=%.2f, ITL=%.2f, i_l=%.2f, o_l=%.2f)",
			beta, ttft, itl, inputToks, outputToks)
	}
	if gamma <= 0 {
		return nil, fmt.Errorf("derived gamma=%.6f <= 0 (TTFT=%.2f, ITL=%.2f, i_l=%.2f, o_l=%.2f)",
			gamma, ttft, itl, inputToks, outputToks)
	}

	// Return state vector [alpha, beta, gamma]
	return []float64{alpha, beta, gamma}, nil
}
