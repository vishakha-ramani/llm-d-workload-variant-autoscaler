package queueingmodel

import (
	"context"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/interfaces"
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

func (a *QueueingModelAnalyzer) getSLOTarget(ctx context.Context, namespace, modelID string, config *QueueingModelConfig) *SLOTarget {
	// TODO: Get from config or guess
	return guessSLOFromMetrics(ctx, nil)
}

func (a *QueueingModelAnalyzer) updateVariantParameters(ctx context.Context, namespace string, metrics []interfaces.ReplicaMetrics, config *QueueingModelConfig) {
	// TODO: Run tuner for each variant
}

func (a *QueueingModelAnalyzer) computeAllVariantCapacities(
	ctx context.Context,
	namespace string,
	metrics []interfaces.ReplicaMetrics,
	states []interfaces.VariantReplicaState,
	slo *SLOTarget) []interfaces.VariantCapacity {
	// TODO: Compute lambda star for each variant
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
