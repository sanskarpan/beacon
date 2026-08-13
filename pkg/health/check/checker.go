// Package check implements active health check runners (HTTP, TCP, gRPC, exec, TTL, alias).
package check

import (
	"context"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
)

// Checker runs one probe.
type Checker interface {
	Run(ctx context.Context) (catalog.HealthStatus, string, error)
}

// Result is a single observation.
type Result struct {
	Status   catalog.HealthStatus
	Output   string
	Duration time.Duration
	Err      error
}

// Truncate limits check output size.
func Truncate(s string, n int) string {
	if n <= 0 {
		n = 256
	}
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
