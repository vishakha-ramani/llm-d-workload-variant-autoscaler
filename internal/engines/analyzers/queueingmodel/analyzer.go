package queueingmodel

import (
	"context"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/engines/analyzers/queueingmodel/tuner"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/interfaces"
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
	return "queueing_model"
}

// Analyze implements interfaces.Analyzer.
// Called for each model
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

	// Get SLO targets
	sloTarget := a.getSLOTarget(ctx, input.Namespace, input.ModelID, qConfig)
	if sloTarget == nil {
		logger.Info("No SLO targets", "modelID", input.ModelID)
		return a.emptyResult(input), nil
	}

	// Update parameters (tuner) for all variants associated with the model
	if qConfig.TuningEnabled {
		a.updateVariantParameters(ctx, input.Namespace, input.ReplicaMetrics, qConfig)
	}

	// Compute capacities
	variantCapacities := a.computeAllVariantCapacities(
		ctx, input.Namespace, input.ReplicaMetrics, input.VariantStates, sloTarget,
	)

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
) *SLOTarget {
	// TODO: Get from config or guess
	return guessSLOFromMetrics(ctx, nil)
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
		env, err := a.buildEnvironmentFromMetrics(ctx, namespace, variantName, replicaMetrics)
		if err != nil {
			logger.V(1).Info("Failed to build environment for variant",
				"variant", variantName,
				"namespace", namespace,
				"error", err)
			continue
		}

		// Create a tuner for this variant
		tuner, err := a.createTuner(ctx, namespace, variantName, env, config)
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

func (a *QueueingModelAnalyzer) computeAllVariantCapacities(
	ctx context.Context,
	namespace string,
	metrics []interfaces.ReplicaMetrics,
	states []interfaces.VariantReplicaState,
	slo *SLOTarget,
) []interfaces.VariantCapacity {
	// TODO: Compute lambdaStar for each variant
	return nil
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

// guessSLOFromMetrics estimates reasonable SLO targets from observed metrics.
// This allows the analyzer to work without explicit user-provided SLO configuration.
func guessSLOFromMetrics(ctx context.Context, metrics []interfaces.ReplicaMetrics) *SLOTarget {
	// TODO
	return nil
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
// Returns error if required metrics are unavailable.
func (a *QueueingModelAnalyzer) buildEnvironmentFromMetrics(
	ctx context.Context,
	namespace string,
	variantName string,
	metrics []interfaces.ReplicaMetrics,
) (*tuner.Environment, error) {
	// TODO: This is a stub. We need to:
	// 1. Aggregate/compute Lambda (request rate per minute) from metrics
	// 2. Aggregate AvgInputToks and AvgOutputToks (from where?)
	// 3. Get MaxBatchSize (from variant config or metrics)
	// 4. Compute/aggregate AvgTTFT and AvgITL from replica latency metrics
	//
	// Currently, ReplicaMetrics does not contain:
	// - Request rate information
	// - MaxBatchSize
	// - Latency metrics (TTFT, ITL)
	//
	// Options:
	// a) Extend ReplicaMetrics to include these fields
	// b) Query additional metrics sources here
	// c) Add these to AnalyzerInput as a separate structure

	return nil, fmt.Errorf("buildEnvironmentFromMetrics not yet implemented")
}

// createTuner gets an existing tuner for a variant or creates a new one.
// If parameters exist in the store, uses the stored state and covariance.
// Otherwise, attempts to guess initial state from environment metrics.
func (a *QueueingModelAnalyzer) createTuner(
	ctx context.Context,
	namespace string,
	variantName string,
	env *tuner.Environment,
	config *QueueingModelConfig,
) (*tuner.Tuner, error) {
	logger := ctrl.LoggerFrom(ctx)

	// Check if we have existing parameters
	existingParams := a.paramStore.Get(namespace, variantName)

	// Get base tuner config (uses user config or defaults)
	tunerConfig := a.getTunerConfig(config, env)

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

// getTunerConfig extracts or creates TunerConfigData from QueueingModelConfig.
// If user provided TunerConfig in config, uses that. Otherwise, creates default configuration.
func (a *QueueingModelAnalyzer) getTunerConfig(config *QueueingModelConfig, env *tuner.Environment) *tuner.TunerConfigData {
	// If user provided custom tuner config, use it
	if config.TunerConfig != nil {
		return config.TunerConfig
	}

	// Create default configuration
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
		FilterData: tuner.FilterData{
			GammaFactor: tuner.DefaultGammaFactor,
			ErrorLevel:  tuner.DefaultErrorLevel,
			TPercentile: tuner.DefaultTPercentile,
		},
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
	if cov == nil || len(cov) == 0 {
		return nil
	}
	n := len(cov)
	flat := make([]float64, 0, n*n)
	for i := 0; i < n; i++ {
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
	for i := 0; i < rows; i++ {
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
