package config

import (
	"os"
	"testing"
)

func TestQueueingModelConfigMapName_Default(t *testing.T) {
	os.Unsetenv("QUEUEING_MODEL_CONFIG_MAP_NAME")
	if got := QueueingModelConfigMapName(); got != "wva-queueing-model-config" {
		t.Errorf("QueueingModelConfigMapName() = %q, want %q", got, "wva-queueing-model-config")
	}
}

func TestQueueingModelConfigMapName_EnvOverride(t *testing.T) {
	os.Setenv("QUEUEING_MODEL_CONFIG_MAP_NAME", "custom-qm-config")
	defer os.Unsetenv("QUEUEING_MODEL_CONFIG_MAP_NAME")
	if got := QueueingModelConfigMapName(); got != "custom-qm-config" {
		t.Errorf("QueueingModelConfigMapName() = %q, want %q", got, "custom-qm-config")
	}
}
