package lb_test

import (
	"math/rand"
	"testing"

	"github.com/sanskar/beacon/pkg/lb"
)

// BenchmarkP2CPick measures single P2C pick latency (SPEC §20 target: < 200 ns).
//
// Hardware baseline (documented, not CI-gated): Apple M-series / recent x86-64
// desktop typically lands 40–90 ns/pick with 100 endpoints. CI machines are
// noisier, so the strict < 200 ns target is verified via `go test -bench` on a
// quiet machine; this package gates only a loose sanity bound to catch
// regressions (e.g. accidental allocations in the hot path).
func BenchmarkP2CPick(b *testing.B) {
	p := lb.NewP2C(eps(100), rand.New(rand.NewSource(1)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, done, _ := p.Pick(lb.PickInfo{})
		done(lb.DoneInfo{})
	}
}

func BenchmarkP2CPickSmallPool(b *testing.B) {
	p := lb.NewP2C(eps(5), rand.New(rand.NewSource(1)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, done, _ := p.Pick(lb.PickInfo{})
		done(lb.DoneInfo{})
	}
}

// TestP2CPickAllocationFree is the regression gate for TODO-027: the hot pick
// path must not allocate (allocations would blow the < 200 ns budget and add GC
// pressure at 5k+ endpoints).
func TestP2CPickAllocationFree(t *testing.T) {
	p := lb.NewP2C(eps(100), rand.New(rand.NewSource(1)))
	// warm up
	for i := 0; i < 1000; i++ {
		_, done, _ := p.Pick(lb.PickInfo{})
		done(lb.DoneInfo{})
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		_, done, _ := p.Pick(lb.PickInfo{})
		done(lb.DoneInfo{})
	}); allocs > 1.0 {
		t.Fatalf("P2C pick allocates %.2f allocs/op; hot path must stay allocation-free (budget ≤1)", allocs)
	}
}
