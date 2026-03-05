package queueingmodel

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/engines/analyzers/queueingmodel/tuner"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/interfaces"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/pkg/analyzer"
	ctrl "sigs.k8s.io/controller-runtime"
)

// QueueingModelAnalyzer implements interfaces.Analyzer.
// It performs SLO-driven capacity analysis by:
//  1. Learning model parameters (alpha, beta, gamma) online via Kalman filter
//  2. Using queueing model to predict max request rate that meets TTFT/ITL SLOs
//  3. Computing capacity signals for scaling decisions

type QueueingModelAnalyzer struct {
	// modelsParameterStore stores learned parameters of variants for all models
	modelsParameterStore map[string]*ParameterStore // key: modelKey (namespace/modelID)
}

// NewQueueingModelAnalyzer creates a new queueing model analyzer instance.
func NewQueueingModelAnalyzer() *QueueingModelAnalyzer {
	return &QueueingModelAnalyzer{
		modelsParameterStore: make(map[string]*ParameterStore),
	}
}

// Name implements interfaces.Analyzer.
func (a *QueueingModelAnalyzer) Name() string {
	return interfaces.QueueingModelAnalyzerName
}

// Update deletes non-existing models from paramStore[models]
// and adds new models to the store
func (a *QueueingModelAnalyzer) Update(currentModels map[string]bool) {
	// delete non-existing models
	deletedModels := []string{}
	for modelKey := range a.modelsParameterStore {
		if _, exists := currentModels[modelKey]; !exists {
			deletedModels = append(deletedModels, modelKey)
		}
	}
	for _, modelKey := range deletedModels {
		delete(a.modelsParameterStore, modelKey)
	}

	// add new models
	for modelKey := range currentModels {
		if _, exists := a.modelsParameterStore[modelKey]; !exists {
			a.modelsParameterStore[modelKey] = NewParameterStore()
		}
	}
}

// get parameters for a given model, namespace, and variant (nil if does not exist)
func (a *QueueingModelAnalyzer) getParams(modelID, namespace, variantName string) (params *LearnedParameters) {
	modelKey := MakeModelKey(namespace, modelID)
	if pStore, exists := a.modelsParameterStore[modelKey]; exists {
		params = pStore.Get(namespace, variantName)
	}
	return params
}

// set parameters for a given model, namespace, and variant
func (a *QueueingModelAnalyzer) setParams(modelID, namespace, variantName string, params *LearnedParameters) {
	modelKey := MakeModelKey(namespace, modelID)
	pStore := a.modelsParameterStore[modelKey]
	// this shouldn't happen as Update() makes sure that there are entries for all current models
	if pStore == nil {
		a.modelsParameterStore[modelKey] = NewParameterStore()
		pStore = a.modelsParameterStore[modelKey]
	}
	pStore.Set(namespace, variantName, params)
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
	modelID := input.ModelID
	namespace := input.Namespace

	// Extract config
	qConfig, ok := input.Config.(*QueueingModelConfig)
	if !ok {
		return nil, fmt.Errorf("expected *QueueingModelConfig, got %T", input.Config)
	}

	// Update parameters (tuner) for all variants associated with the model
	if qConfig.TuningEnabled {
		a.updateVariantParameters(ctx, namespace, modelID, input.ReplicaMetrics, qConfig)
	}

	// Get SLO targets
	// TODO: store the time series for SLO and smooth the SLO target.
	sloTarget := a.getSLOTarget(ctx, namespace, modelID, qConfig, input.ReplicaMetrics)
	if sloTarget == nil {
		logger.Info("No SLO targets", "modelID", modelID)
		return a.emptyResult(input), nil
	}

	// Compute capacities
	variantCapacities := a.computeAllVariantCapacities(
		ctx, namespace, modelID, input.ReplicaMetrics, input.VariantStates, sloTarget,
	)
	if len(variantCapacities) == 0 {
		return nil, fmt.Errorf("could not compute variant capacities for model %q", modelID)
	}

	// Aggregate and build result
	totalSupply, totalDemand := aggregateCapacities(variantCapacities)
	utilization := 0.0
	if totalSupply > 0 {
		utilization = totalDemand / totalSupply
	}

	return &interfaces.AnalyzerResult{
		AnalyzerName:      a.Name(),
		ModelID:           modelID,
		Namespace:         namespace,
		AnalyzedAt:        time.Now(),
		VariantCapacities: variantCapacities,
		TotalSupply:       totalSupply,
		TotalDemand:       totalDemand,
		Utilization:       utilization,
		RequiredCapacity:  math.Max(0, totalDemand-totalSupply),
		SpareCapacity:     math.Max(0, totalSupply-totalDemand),
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
	return a.guessSLOFromMetrics(ctx, namespace, modelID, config, metrics)
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
	modelID string,
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
		params := a.getParams(modelID, namespace, variantName)
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
	return fallbackSLOFromObservations(ctx, wm)
}

func (a *QueueingModelAnalyzer) updateVariantParameters(
	ctx context.Context,
	namespace string,
	modelID string,
	metrics []interfaces.ReplicaMetrics,
	config *QueueingModelConfig,
) {
	logger := ctrl.LoggerFrom(ctx)

	// Group metrics by variant
	variantMetrics := groupMetricsByVariant(metrics)

	// Run tuner for each variant
	for variantName, replicaMetrics := range variantMetrics {
		// Build environment from aggregated replica metrics
		env, err := buildEnvironmentFromMetrics(variantName, replicaMetrics)
		if err != nil {
			logger.V(1).Info("Failed to build environment for variant",
				"variant", variantName,
				"namespace", namespace,
				"error", err)
			continue
		}

		// Create a tuner for this variant
		tuner, err := a.createTunerForVariant(ctx, namespace, modelID, variantName, env, config.FilterConfig)
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
		a.storeParametersFromResults(namespace, modelID, variantName, results)

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
	modelID string,
	replicaMetrics []interfaces.ReplicaMetrics,
	variantStates []interfaces.VariantReplicaState,
	sloTarget *SLOTarget,
) []interfaces.VariantCapacity {
	logger := ctrl.LoggerFrom(ctx)

	// Build cost and accelerator lookup from input metrics
	variantCost := make(map[string]float64)
	variantAccel := make(map[string]string)
	for _, rm := range replicaMetrics {
		if _, ok := variantCost[rm.VariantName]; !ok {
			variantCost[rm.VariantName] = rm.Cost
			variantAccel[rm.VariantName] = rm.AcceleratorName
		}
	}

	variantCapacities := make([]interfaces.VariantCapacity, 0, len(variantStates))
	for _, variantState := range variantStates {
		variantName := variantState.VariantName
		readyCount := max(variantState.CurrentReplicas-variantState.PendingReplicas, 0)

		// variant capacity in case of error
		errorVariantCapacity := interfaces.VariantCapacity{
			VariantName:     variantName,
			AcceleratorName: variantAccel[variantName],
			Cost:            variantCost[variantName],
			ReplicaCount:    readyCount,
			PendingReplicas: variantState.PendingReplicas,

			PerReplicaCapacity: 1.0, // TODO: caller should handle variants withoout results, instead of relying on absolute values
			TotalCapacity:      1.0,
			TotalDemand:        0.0,
			Utilization:        0.0,
		}

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
			vr := errorVariantCapacity
			variantCapacities = append(variantCapacities, vr)
			continue
		}

		// Fall back to default if MaxBatchSize was not parsed from deployment args
		if maxBatchSize <= 0 {
			maxBatchSize = DefaultMaxBatchSize
		}

		// Prefill and decode parameters
		params := a.getParams(modelID, namespace, variantName)
		if params == nil {
			logger.Info("No parameters found for variant", "variant", variantName)
			vr := errorVariantCapacity
			variantCapacities = append(variantCapacities, vr)
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
			vr := errorVariantCapacity
			variantCapacities = append(variantCapacities, vr)
			continue
		}

		var maxRequestRate float32
		if _, metrics, _, err := queueAnalyzer.Size(targetPerf); err != nil {
			logger.Info("Failed to calculate max request rate for variant", "variant", variantName, "error", err)
			vr := errorVariantCapacity
			variantCapacities = append(variantCapacities, vr)
			continue
		} else {
			maxRequestRate = metrics.Throughput
		}

		if maxRequestRate == 0 {
			logger.Info("Failed to calculate max request rate for variant", "variant", variantName)
			vr := errorVariantCapacity
			variantCapacities = append(variantCapacities, vr)
			continue
		}

		desiredNumReplicas := math.Ceil(float64(totalArrivalRate) / float64(maxRequestRate))
		if desiredNumReplicas == 0 {
			desiredNumReplicas = 1
		}
		arrivalRatePerReplica := totalArrivalRate / float32(desiredNumReplicas)

		variantCapacity := interfaces.VariantCapacity{
			VariantName:     variantName,
			AcceleratorName: variantAccel[variantName],
			Cost:            variantCost[variantName], // TODO: multiply by numReplicas?
			ReplicaCount:    readyCount,
			PendingReplicas: variantState.PendingReplicas,

			PerReplicaCapacity: float64(maxRequestRate),
			TotalCapacity:      desiredNumReplicas * float64(maxRequestRate),
			TotalDemand:        float64(totalArrivalRate),
			Utilization:        float64(arrivalRatePerReplica / maxRequestRate),
		}
		variantCapacities = append(variantCapacities, variantCapacity)
	}

	return variantCapacities
}

// createTunerForVariant creates a new tuner instance for a variant.
// If parameters exist in the store, uses the stored state and covariance.
// Otherwise, attempts to guess initial state from environment metrics.
func (a *QueueingModelAnalyzer) createTunerForVariant(
	ctx context.Context,
	namespace string,
	modelID string,
	variantName string,
	env *tuner.Environment,
	filterConfig *tuner.FilterData,
) (*tuner.Tuner, error) {
	logger := ctrl.LoggerFrom(ctx)

	// Check if we have existing parameters
	existingParams := a.getParams(modelID, namespace, variantName)

	// Get base tuner config (uses user config or defaults)
	tunerConfig := tuner.CreateTunerConfigFromData(filterConfig, env)

	if existingParams != nil {
		// Restore state and covariance from previous tuning cycle
		logger.V(1).Info("Restoring tuner state from parameter store",
			"variant", variantName,
			"namespace", namespace,
			"alpha", existingParams.Alpha,
			"beta", existingParams.Beta,
			"gamma", existingParams.Gamma)

		tunerConfig.ModelData.InitState = ParamsToStateVector(
			float64(existingParams.Alpha),
			float64(existingParams.Beta),
			float64(existingParams.Gamma))

		flatCov := flattenCovariance(existingParams.Covariance)
		if flatCov != nil {
			tunerConfig.ModelData.InitCovarianceMatrix = flatCov
		}
	} else {
		// No existing parameters - attempt to guess initial state from metrics
		logger.V(1).Info("No existing parameters found, attempting to guess initial state",
			"variant", variantName,
			"namespace", namespace)

		state, err := guessInitState(env)
		if err != nil {
			logger.V(1).Info("Failed to guess initial state, using defaults",
				"variant", variantName,
				"namespace", namespace,
				"error", err)
			// tunerConfig already has default InitState, so we can proceed
		} else {
			alpha, beta, gamma := StateVectorToParams(state)
			logger.V(1).Info("Using guessed initial state",
				"variant", variantName,
				"namespace", namespace,
				"alpha", alpha,
				"beta", beta,
				"gamma", gamma)
			tunerConfig.ModelData.InitState = state
			// Update bounds based on guessed state
			tunerConfig.ModelData.MinState = tuner.GetFactoredSlice(state, tuner.DefaultMinStateFactor)
			tunerConfig.ModelData.MaxState = tuner.GetFactoredSlice(state, tuner.DefaultMaxStateFactor)
		}
	}

	// Create new tuner instance with environment
	t, err := tuner.NewTuner(tunerConfig, env)
	if err != nil {
		return nil, fmt.Errorf("failed to create tuner: %w", err)
	}

	return t, nil
}

// storeParametersFromResults saves tuned results to the parameter store.
func (a *QueueingModelAnalyzer) storeParametersFromResults(
	namespace, modelID, variantName string,
	results *tuner.TunedResults,
) {
	// Extract covariance matrix
	covariance := matrixToSlice2D(results.Covariance)

	params := &LearnedParameters{
		Alpha:       results.ServiceParms.Alpha,
		Beta:        results.ServiceParms.Beta,
		Gamma:       results.ServiceParms.Gamma,
		NIS:         results.NIS,
		Covariance:  covariance,
		LastUpdated: time.Now(),
	}

	a.setParams(modelID, namespace, variantName, params)
}

func (a *QueueingModelAnalyzer) emptyResult(input interfaces.AnalyzerInput) *interfaces.AnalyzerResult {
	return &interfaces.AnalyzerResult{
		AnalyzerName: a.Name(),
		ModelID:      input.ModelID,
		Namespace:    input.Namespace,
		AnalyzedAt:   time.Now(),
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

// fallbackSLOFromObservations creates SLO targets from observed TTFT/ITL
// with a headroom multiplier and reasonable caps. Used during cold start
// before the Kalman filter has learned hardware parameters.
func fallbackSLOFromObservations(
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

// buildEnvironmentFromMetrics creates a tuner Environment from aggregated replica metrics.
// Aggregates per-replica metrics (arrival rate, avg tokens, TTFT, ITL, max batch size)
// into a single Environment representing the variant's current operating state.
// Returns error if required metrics are unavailable.
func buildEnvironmentFromMetrics(
	variantName string,
	metrics []*interfaces.ReplicaMetrics,
) (*tuner.Environment, error) {
	if len(metrics) == 0 {
		return nil, fmt.Errorf("no replica metrics for variant %s", variantName)
	}

	// MaxBatchSize is per-deployment (same for all replicas of a variant),
	// so we extract it once from the first replica that has it.
	maxBatchSize := int64(DefaultMaxBatchSize)
	for _, rm := range metrics {
		if rm.MaxBatchSize > 0 {
			maxBatchSize = rm.MaxBatchSize
			break
		}
	}

	// Aggregate per-pod traffic metrics across replicas
	// TODO: option 1: TTFT and ITL must be weighted based on the arrival rate
	// TODO: option 2, just take any one pod (maybe the oldest?) that is a good representative of a variant load statistics.
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
func guessInitState(env *tuner.Environment) ([]float64, error) {
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

	return ParamsToStateVector(alpha, beta, gamma), nil
}

func aggregateCapacities(capacities []interfaces.VariantCapacity) (supply, demand float64) {
	for _, c := range capacities {
		supply += c.TotalCapacity
		demand += c.TotalDemand
	}
	return
}

// groupMetricsByVariant groups replica metrics by variant name.
func groupMetricsByVariant(metrics []interfaces.ReplicaMetrics) map[string][]*interfaces.ReplicaMetrics {
	grouped := make(map[string][]*interfaces.ReplicaMetrics)
	for _, m := range metrics {
		grouped[m.VariantName] = append(grouped[m.VariantName], &m)
	}
	return grouped
}
