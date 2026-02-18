package queueingmodel

import (
	"fmt"
	"sync"
	"time"
)

// ParameterStore holds learned parameters per variant
type ParameterStore struct {
	mu     sync.RWMutex
	params map[string]*LearnedParameters // key: namespace/variantName
}

// LearnedParameters holds tuned alpha, beta, gamma for one variant
type LearnedParameters struct {
	Alpha       float32
	Beta        float32
	Gamma       float32
	LastUpdated time.Time
	NIS         float64 // Normalized Innovation Squared

	// For continuity between tuning cycles
	State      []float64   // State vector [alpha, beta, gamma]
	Covariance [][]float64 // state covariance matrix
}

// NewParameterStore creates a new parameter store
func NewParameterStore() *ParameterStore {
	return &ParameterStore{
		params: make(map[string]*LearnedParameters),
	}
}

// Get retrieves parameters for a variant
func (s *ParameterStore) Get(namespace, variantName string) *LearnedParameters {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := makeParamKey(namespace, variantName)
	return s.params[key]
}

// Set stores parameters for a variant
func (s *ParameterStore) Set(namespace, variantName string, params *LearnedParameters) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := makeParamKey(namespace, variantName)
	s.params[key] = params
}

// makeParamKey creates a unique key for parameter storage
func makeParamKey(namespace, variantName string) string {
	return fmt.Sprintf("%s/%s", namespace, variantName)
}
