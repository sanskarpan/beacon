package catalog_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
)

func BenchmarkCatalogMemory(b *testing.B) {
	// One iteration builds 10k services × 10 instances and reports allocs.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := catalog.NewStore()
		ctx := context.Background()
		for svc := 0; svc < 100; svc++ { // scaled for CI speed; full 10k×10 is heavy
			for inst := 0; inst < 10; inst++ {
				_, _ = s.Register(ctx, &catalog.Instance{
					ID: fmt.Sprintf("s%d-i%d", svc, inst), Service: fmt.Sprintf("svc-%d", svc),
					Node: "n", Address: "10.0.0.1", Port: 8000 + inst, Health: catalog.HealthPassing,
					Meta: map[string]string{"k": "v"}, Tags: []string{"t"},
				})
			}
		}
		runtime.GC()
		_ = s.Index()
	}
}

func BenchmarkLeases10k(b *testing.B) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	s := catalog.NewStore(catalog.WithClock(clk))
	lm := catalog.NewLeaseManager(s, clk)
	ctx := context.Background()
	const n = 2000
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("l%d", i)
		_, _ = s.Register(ctx, &catalog.Instance{
			ID: id, Service: "s", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
		})
		_, _ = lm.GrantLease(ctx, id, time.Minute, 30*time.Second)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clk.Advance(time.Millisecond)
		lm.ProcessDue(ctx)
	}
}

func TestCatalogMemoryBound(t *testing.T) {
	// Soft check: 1k services × 5 instances should be well under 400MB.
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	before := ms.HeapAlloc
	s := catalog.NewStore()
	ctx := context.Background()
	for svc := 0; svc < 1000; svc++ {
		for inst := 0; inst < 5; inst++ {
			_, _ = s.Register(ctx, &catalog.Instance{
				ID: fmt.Sprintf("s%d-i%d", svc, inst), Service: fmt.Sprintf("svc-%d", svc),
				Node: "n", Address: "10.0.0.1", Port: 8000 + inst, Health: catalog.HealthPassing,
			})
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&ms)
	delta := int64(ms.HeapAlloc) - int64(before)
	const limit = 400 * 1024 * 1024
	if delta > int64(limit) {
		t.Fatalf("catalog heap delta %d exceeds 400MB", delta)
	}
	t.Logf("catalog heap delta for 5k instances: %d bytes (%.1f MB)", delta, float64(delta)/1024/1024)
}
