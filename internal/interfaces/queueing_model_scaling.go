package interfaces

import "fmt"

// QueueingModelScalingConfig holds configuration for the queueing model analyzer.
// The queueing model analyzer uses SLO-driven capacity analysis based on
// queueing theory to determine scaling decisions.
type QueueingModelScalingConfig struct {
	// ModelID is the model identifier (only used in per-model override entries).
	ModelID string `yaml:"model_id,omitempty"`

	// Namespace is the namespace for this override (only used in per-model override entries).
	Namespace string `yaml:"namespace,omitempty"`

	// SLOMultiplier is the maximum tolerable ratio of iteration time under load
	// to the idle-state baseline alpha. In the queueing model, mean iteration
	// time at utilization rho is alpha/(1-rho). Setting the SLO at k*alpha
	// yields the target utilization rho = 1 - 1/k.
	// Only the stochastic queueing component (T_iter = k*alpha) is scaled;
	// deterministic work (prefill and decode token processing) remains at true cost.
	// Must be > 1.0 (k=1 means rho=0, no load tolerance; k<=1 is physically
	// meaningless in the queueing model).
	// Zero value means use default (3.0).
	SLOMultiplier float64 `yaml:"sloMultiplier,omitempty"`

	// TuningEnabled enables online parameter learning via Kalman filter.
	// When true, the tuner learns alpha/beta/gamma from observed metrics.
	// When false, relies on explicit SLO targets or fallback heuristics.
	// Pointer to distinguish unset (nil = default true) from explicitly false.
	TuningEnabled *bool `yaml:"tuningEnabled,omitempty"`

	// TargetTTFT is the target time-to-first-token in milliseconds.
	// Zero means infer from metrics using the queueing model.
	TargetTTFT float32 `yaml:"targetTTFT,omitempty"`

	// TargetITL is the target inter-token latency in milliseconds.
	// Zero means infer from metrics using the queueing model.
	TargetITL float32 `yaml:"targetITL,omitempty"`
}

// GetAnalyzerName implements the AnalyzerConfig interface.
func (c *QueueingModelScalingConfig) GetAnalyzerName() string {
	return "queueing-model"
}

// Validate checks for invalid configuration values.
func (c *QueueingModelScalingConfig) Validate() error {
	// SLOMultiplier: 0 = use default, >1 = valid, <=1 = invalid
	if c.SLOMultiplier != 0 && c.SLOMultiplier <= 1.0 {
		return fmt.Errorf("sloMultiplier must be > 1.0, got %.2f (k=1 means rho=0, no load tolerance; k<=1 is physically meaningless)", c.SLOMultiplier)
	}

	if c.TargetTTFT < 0 {
		return fmt.Errorf("targetTTFT must be >= 0, got %.2f", c.TargetTTFT)
	}
	if c.TargetITL < 0 {
		return fmt.Errorf("targetITL must be >= 0, got %.2f", c.TargetITL)
	}

	// Both or neither SLO target must be set
	if (c.TargetTTFT > 0) != (c.TargetITL > 0) {
		return fmt.Errorf("targetTTFT and targetITL must both be set or both be zero (got TTFT=%.2f, ITL=%.2f)", c.TargetTTFT, c.TargetITL)
	}

	// Per-model overrides must have both model_id and namespace
	if (c.ModelID != "") != (c.Namespace != "") {
		return fmt.Errorf("per-model overrides must have both model_id and namespace (got model_id=%q, namespace=%q)", c.ModelID, c.Namespace)
	}

	return nil
}
