package check

import (
	"context"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
)

// TTLCheck is push-based: the service reports pass/warn/fail. Absence is failure.
type TTLCheck struct {
	mu      sync.Mutex
	clk     clock.Clock
	ttl     time.Duration
	status  catalog.HealthStatus
	output  string
	lastSet time.Time
}

// NewTTL creates a TTL check.
func NewTTL(clk clock.Clock, ttl time.Duration) *TTLCheck {
	if clk == nil {
		clk = clock.New()
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &TTLCheck{
		clk:    clk,
		ttl:    ttl,
		status: catalog.HealthCritical,
	}
}

// Set records a push from the service.
func (t *TTLCheck) Set(status catalog.HealthStatus, output string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status = status
	t.output = output
	t.lastSet = t.clk.Now()
}

// Run evaluates whether the last push is still fresh.
func (t *TTLCheck) Run(ctx context.Context) (catalog.HealthStatus, string, error) {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastSet.IsZero() || t.clk.Now().After(t.lastSet.Add(t.ttl)) {
		return catalog.HealthCritical, "ttl expired", nil
	}
	return t.status, t.output, nil
}
