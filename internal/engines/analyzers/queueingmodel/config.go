package queueingmodel

import "fmt"

// QueueingModelConfig implements interfaces.AnalyzerConfig
type QueueingModelConfig struct {
	// SLOTargets maps (modelID, namespace) to SLO targets
	// Key format: "namespace/modelID"
	SLOTargets map[string]*SLOTarget

	// Tuning configuration
	TuningEnabled bool
}

// SLOTarget defines TTFT/ITL targets for a model
type SLOTarget struct {
	TargetTTFT float32 // Target time-to-first-token (ms)
	TargetITL  float32 // Target inter-token latency (ms)
}

// GetAnalyzerName implements interfaces.AnalyzerConfig
func (c *QueueingModelConfig) GetAnalyzerName() string {
	return "queueing_model"
}

// GetSLOForModel retrieves SLO targets for a model in a namespace
func (c *QueueingModelConfig) GetSLOForModel(namespace, modelID string) *SLOTarget {
	if c.SLOTargets == nil {
		return nil
	}
	key := makeSLOKey(namespace, modelID)
	return c.SLOTargets[key]
}

// makeSLOKey creates a unique key for SLO targets
func makeSLOKey(namespace, modelID string) string {
	return fmt.Sprintf("%s/%s", namespace, modelID)
}
