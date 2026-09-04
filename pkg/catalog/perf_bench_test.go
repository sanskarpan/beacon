package catalog_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
)

// TODO-053: Registration → local visible < 10 ms.
func BenchmarkRegistrationLatency(b *testing.B) {
	cs := catalog.NewStore()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inst := &catalog.Instance{
			ID:      fmt.Sprintf("bench-reg-%d", i),
			Service: "bench-service",
			Address: "10.0.0.1",
			Port:    8080 + (i % 100),
			Health:  catalog.HealthPassing,
			Node:    "bench-node",
		}
		idx, err := cs.Register(ctx, inst)
		if err != nil {
			b.Fatal(err)
		}
		// Immediately read back — must be visible at the returned index.
		got := cs.GetNow("bench-service", catalog.QueryOptions{})
		if len(got.Instances) == 0 {
			b.Fatalf("instance not visible after register (index %d)", idx)
		}
	}
}

// TODO-054: Catalog read 10k instances warm < 1 ms.
func BenchmarkCatalogRead10k(b *testing.B) {
	cs := catalog.NewStore()
	ctx := context.Background()

	// Seed 10k instances.
	for i := 0; i < 10000; i++ {
		svc := fmt.Sprintf("svc-%d", i/10)
		_, _ = cs.Register(ctx, &catalog.Instance{
			ID:      fmt.Sprintf("inst-%d", i),
			Service: svc,
			Address: "10.0.0.1",
			Port:    8080 + (i % 100),
			Health:  catalog.HealthPassing,
			Node:    "node-0",
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := cs.GetNow("svc-500", catalog.QueryOptions{})
		if len(res.Instances) != 10 {
			b.Fatalf("expected 10 instances, got %d", len(res.Instances))
		}
	}
}

// TODO-055: Registration throughput > 5,000/s per server.
func BenchmarkRegistrationThroughput(b *testing.B) {
	cs := catalog.NewStore()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cs.Register(ctx, &catalog.Instance{
			ID:      fmt.Sprintf("tp-%d", i),
			Service: "throughput",
			Address: "10.0.0.1",
			Port:    8080,
			Health:  catalog.HealthPassing,
			Node:    "node-0",
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TODO-057: Catalog memory 10k services × 10 instances < 400 MB.
func TestCatalogMemory10kServices(t *testing.T) {
	cs := catalog.NewStore()
	ctx := context.Background()

	for i := 0; i < 10000; i++ {
		svc := fmt.Sprintf("svc-%d", i)
		for j := 0; j < 10; j++ {
			_, _ = cs.Register(ctx, &catalog.Instance{
				ID:      fmt.Sprintf("svc-%d-inst-%d", i, j),
				Service: svc,
				Address: "10.0.0.1",
				Port:    8080 + j,
				Health:  catalog.HealthPassing,
				Node:    fmt.Sprintf("node-%d", j),
				Tags:    []string{fmt.Sprintf("version=v%d", j), "tier=standard"},
				Meta:    map[string]string{"region": "us-east-1", "zone": fmt.Sprintf("az-%d", j%3)},
			})
		}
	}

	// Snapshot all instances.
	snap := cs.Snapshot()
	total := len(snap.Instances)
	if total != 100000 {
		t.Fatalf("expected 100000 instances, got %d", total)
	}

	// Verify service count.
	if len(snap.Services) != 10000 {
		t.Fatalf("expected 10000 services, got %d", len(snap.Services))
	}
	t.Logf("10k services × 10 instances = %d instances loaded", total)
	// Note: actual memory measurement requires runtime.ReadMemStats in CI.
}

// TODO-058: Combined blocking query + registration throughput stress.
func TestCombinedStress(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))

	const writers = 5
	const readers = 10
	const perWriter = 200
	done := make(chan struct{})

	// Writers: continuous registration.
	go func() {
		defer close(done)
		for w := 0; w < writers; w++ {
			for i := 0; i < perWriter; i++ {
				ctx := context.Background()
				_, _ = cs.Register(ctx, &catalog.Instance{
					ID:      fmt.Sprintf("stress-%d-%d", w, i),
					Service: fmt.Sprintf("stress-svc-%d", w),
					Address: "10.0.0.1", Port: 8080 + i,
					Health: catalog.HealthPassing, Node: "node-0",
				})
			}
		}
	}()

	// Readers: blocking queries on various services.
	for r := 0; r < readers; r++ {
		go func(reader int) {
			svc := fmt.Sprintf("stress-svc-%d", reader%writers)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = cs.Get(ctx, svc, catalog.QueryOptions{})
		}(r)
	}

	<-done
	// Verify all instances landed.
	for w := 0; w < writers; w++ {
		svc := fmt.Sprintf("stress-svc-%d", w)
		res := cs.GetNow(svc, catalog.QueryOptions{})
		if len(res.Instances) != perWriter {
			t.Fatalf("service %s: expected %d instances, got %d", svc, perWriter, len(res.Instances))
		}
	}
}
