package catalog_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
)

func TestNodeRegistrationRateLimit(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	// 2/s sustained, burst 2 — third immediate register must fail.
	s := catalog.NewStore(catalog.WithClock(clk), catalog.WithNodeRegRateLimit(2, 2))

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		_, err := s.Register(ctx, &catalog.Instance{
			ID: fmt.Sprintf("a-%d", i), Service: "svc", Node: "node-1",
			Address: "10.0.0.1", Port: 8080 + i, Health: catalog.HealthPassing,
		})
		if err != nil {
			t.Fatalf("reg %d: %v", i, err)
		}
	}
	_, err := s.Register(ctx, &catalog.Instance{
		ID: "a-2", Service: "svc", Node: "node-1",
		Address: "10.0.0.1", Port: 8082, Health: catalog.HealthPassing,
	})
	if !errors.Is(err, catalog.ErrRateLimited) {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}

	// Different node has its own bucket.
	_, err = s.Register(ctx, &catalog.Instance{
		ID: "b-0", Service: "svc", Node: "node-2",
		Address: "10.0.0.2", Port: 8080, Health: catalog.HealthPassing,
	})
	if err != nil {
		t.Fatalf("other node should not be limited: %v", err)
	}

	// After time passes, node-1 can register again.
	clk.Advance(time.Second)
	_, err = s.Register(ctx, &catalog.Instance{
		ID: "a-3", Service: "svc", Node: "node-1",
		Address: "10.0.0.1", Port: 8083, Health: catalog.HealthPassing,
	})
	if err != nil {
		t.Fatalf("after refill: %v", err)
	}
}

func TestNodeRegistrationStormDoesNotSilentQueue(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	s := catalog.NewStore(catalog.WithClock(clk), catalog.WithNodeRegRateLimit(10, 10))
	ctx := context.Background()
	limited := 0
	ok := 0
	for i := 0; i < 100; i++ {
		_, err := s.Register(ctx, &catalog.Instance{
			ID:      fmt.Sprintf("storm-%d", i),
			Service: "storm", Node: "deploy-node",
			Address: "10.0.0.1", Port: 10000 + i, Health: catalog.HealthPassing,
		})
		if errors.Is(err, catalog.ErrRateLimited) {
			limited++
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		ok++
	}
	if limited == 0 {
		t.Fatal("expected rate limit under storm")
	}
	if ok > 15 {
		t.Fatalf("too many accepted under tight limit: ok=%d limited=%d", ok, limited)
	}
	// Catalog must not have queued the rejected ones — only accepted count present.
	n := 0
	for _, list := range s.ListServices() {
		n += len(list)
	}
	// ListServices returns tags not counts — count instances via GetNow
	res := s.GetNow("storm", catalog.QueryOptions{})
	if len(res.Instances) != ok {
		t.Fatalf("catalog has %d instances, accepted %d — rejected must not be queued", len(res.Instances), ok)
	}
}
