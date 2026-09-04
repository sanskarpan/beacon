package lb_test

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/lb"
)

func eps(n int) []*lb.Endpoint {
	out := make([]*lb.Endpoint, n)
	for i := 0; i < n; i++ {
		out[i] = &lb.Endpoint{Addr: string(rune('a' + i)), Weight: 1, Healthy: true}
	}
	return out
}

func TestRoundRobinUniform(t *testing.T) {
	p := lb.NewRoundRobin(eps(5))
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		ep, done, err := p.Pick(lb.PickInfo{})
		if err != nil {
			t.Fatal(err)
		}
		counts[ep.Addr]++
		done(lb.DoneInfo{})
	}
	for _, c := range counts {
		if math.Abs(float64(c)-2000) > 50 {
			t.Fatalf("non-uniform: %v", counts)
		}
	}
}

func TestWeightedDistribution(t *testing.T) {
	e := []*lb.Endpoint{
		{Addr: "heavy", Weight: 9, Healthy: true},
		{Addr: "light", Weight: 1, Healthy: true},
	}
	p := lb.NewWeightedRR(e)
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		ep, done, _ := p.Pick(lb.PickInfo{})
		counts[ep.Addr]++
		done(lb.DoneInfo{})
	}
	// heavy should be ~90%
	ratio := float64(counts["heavy"]) / 10000
	if math.Abs(ratio-0.9) > 0.02 {
		t.Fatalf("weight ratio %.3f not near 0.9: %v", ratio, counts)
	}
}

func TestP2CAvoidsSlow(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	e := []*lb.Endpoint{
		{Addr: "fast", Weight: 1, Healthy: true},
		{Addr: "slow", Weight: 1, Healthy: true},
	}
	// make slow appear loaded
	e[1].Inflight.Store(100)
	p := lb.NewP2C(e, rng)
	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		ep, done, _ := p.Pick(lb.PickInfo{})
		counts[ep.Addr]++
		// only release fast; keep slow loaded
		if ep.Addr == "fast" {
			done(lb.DoneInfo{})
		}
	}
	if counts["fast"] < counts["slow"] {
		t.Fatalf("P2C should prefer fast: %v", counts)
	}
}

func TestP2CDistinct(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	p := lb.NewP2C(eps(10), rng)
	// internal: just ensure no panic over many picks
	for i := 0; i < 1000; i++ {
		ep, done, err := p.Pick(lb.PickInfo{})
		if err != nil || ep == nil {
			t.Fatal(err)
		}
		done(lb.DoneInfo{})
	}
}

func TestRingHashAffinity(t *testing.T) {
	p := lb.NewRingHash(eps(10), 50)
	ep1, _, _ := p.Pick(lb.PickInfo{HashKey: "session-42"})
	ep2, _, _ := p.Pick(lb.PickInfo{HashKey: "session-42"})
	if ep1.Addr != ep2.Addr {
		t.Fatalf("affinity broken: %s vs %s", ep1.Addr, ep2.Addr)
	}
}

func TestP2CBenchmarkSanity(t *testing.T) {
	p := lb.NewP2C(eps(100), rand.New(rand.NewSource(1)))
	start := time.Now()
	for i := 0; i < 100000; i++ {
		_, done, _ := p.Pick(lb.PickInfo{})
		done(lb.DoneInfo{})
	}
	// just ensure it finishes quickly
	if time.Since(start) > 2*time.Second {
		t.Fatal("too slow")
	}
}
