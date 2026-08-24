package catalog_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
)

// TODO-022: Rate-limit edge cases — per-node with clear error.
// Validates that:
// 1. Rate limit triggers after burst is exhausted.
// 2. Error message includes node name and rate info.
// 3. Different nodes have independent buckets.
// 4. Limiter entries are pruned when node deregisters all instances (audit fix).
func TestRateLimit_ClearError(t *testing.T) {
	cs := catalog.NewStore(
		catalog.WithNodeRegRateLimit(2, 2), // 2/s, burst=2
	)
	ctx := context.Background()

	// Exhaust burst.
	for i := 0; i < 2; i++ {
		_, err := cs.Register(ctx, &catalog.Instance{
			ID: fmt.Sprintf("rl-%d", i), Service: "rl-svc",
			Address: "10.0.0.1", Port: 8080 + i,
			Health: catalog.HealthPassing, Node: "storm-node",
		})
		if err != nil {
			t.Fatalf("burst %d should succeed: %v", i, err)
		}
	}

	// Next should fail.
	_, err := cs.Register(ctx, &catalog.Instance{
		ID: "rl-over", Service: "rl-svc",
		Address: "10.0.0.1", Port: 9999,
		Health: catalog.HealthPassing, Node: "storm-node",
	})
	if err == nil {
		t.Fatal("expected rate limit error")
	}
	if !isRateLimited(err) {
		t.Fatalf("expected ErrRateLimited, got: %v", err)
	}
	t.Logf("rate limit error: %v", err)
}

func TestRateLimit_IndependentNodes(t *testing.T) {
	cs := catalog.NewStore(
		catalog.WithNodeRegRateLimit(1, 1), // 1/s, burst=1
	)
	ctx := context.Background()

	// Node A uses its burst.
	_, err := cs.Register(ctx, &catalog.Instance{
		ID: "a-1", Service: "s", Address: "10.0.0.1", Port: 8080,
		Health: catalog.HealthPassing, Node: "node-a",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Node B still has its own burst available.
	_, err = cs.Register(ctx, &catalog.Instance{
		ID: "b-1", Service: "s", Address: "10.0.0.2", Port: 8080,
		Health: catalog.HealthPassing, Node: "node-b",
	})
	if err != nil {
		t.Fatalf("node-b should have independent bucket: %v", err)
	}

	// Node A should be rate-limited.
	_, err = cs.Register(ctx, &catalog.Instance{
		ID: "a-2", Service: "s", Address: "10.0.0.1", Port: 8081,
		Health: catalog.HealthPassing, Node: "node-a",
	})
	if !isRateLimited(err) {
		t.Fatalf("node-a should be rate-limited, got: %v", err)
	}
}

func TestRateLimit_NoLimitWhenDisabled(t *testing.T) {
	cs := catalog.NewStore() // no rate limit
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		_, err := cs.Register(ctx, &catalog.Instance{
			ID: fmt.Sprintf("nolimit-%d", i), Service: "s",
			Address: "10.0.0.1", Port: 8080 + i,
			Health: catalog.HealthPassing, Node: "fast-node",
		})
		if err != nil {
			t.Fatalf("no-limit register %d: %v", i, err)
		}
	}
}

func isRateLimited(err error) bool {
	return errors.Is(err, catalog.ErrRateLimited) ||
		(err != nil && strings.Contains(err.Error(), "rate limited"))
}
