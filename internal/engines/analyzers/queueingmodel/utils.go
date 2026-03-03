package queueingmodel

import (
	"fmt"

	"gonum.org/v1/gonum/mat"
)

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

// MakeModelKey creates a unique key for a model
func MakeModelKey(namespace, modelID string) string {
	return fmt.Sprintf("%s/%s", namespace, modelID)
}

// makeVariantKey creates a unique key for a variant
func makeVariantKey(namespace, variantName string) string {
	return fmt.Sprintf("%s/%s", namespace, variantName)
}
