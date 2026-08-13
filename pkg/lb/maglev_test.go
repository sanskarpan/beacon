package lb_test

import (
	"fmt"
	"testing"

	"github.com/sanskar/beacon/pkg/lb"
)

func TestMaglevAffinity(t *testing.T) {
	eps := []*lb.Endpoint{
		{Addr: "a:1", Weight: 1, Healthy: true},
		{Addr: "b:1", Weight: 1, Healthy: true},
		{Addr: "c:1", Weight: 1, Healthy: true},
	}
	m := lb.NewMaglev(eps, 655)
	ep1, _, _ := m.Pick(lb.PickInfo{HashKey: "session-42"})
	ep2, _, _ := m.Pick(lb.PickInfo{HashKey: "session-42"})
	if ep1.Addr != ep2.Addr {
		t.Fatalf("affinity broken: %s vs %s", ep1.Addr, ep2.Addr)
	}
}

func TestMaglevLowDisruption(t *testing.T) {
	// Build table with N backends, remove one, measure disruption ~1/N.
	const n = 10
	const m = 10007 // prime
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = fmt.Sprintf("ep-%d", i)
	}
	tableA := buildViaPublic(names, m)
	// remove last
	tableB := buildViaPublic(names[:n-1], m)
	// Remap tableB indices are into smaller set — compare by name assignment.
	// Simpler: fraction of keys whose selected name changed.
	changed := 0
	for i := 0; i < m; i++ {
		aName := names[tableA[i]]
		bName := names[:n-1][tableB[i]]
		if aName != bName {
			changed++
		}
	}
	frac := float64(changed) / float64(m)
	// Maglev typically moves ~1/N to 2/N of keys; allow up to 0.35 for small N.
	if frac > 0.35 {
		t.Fatalf("disruption too high: %.2f (want closer to 1/%d)", frac, n)
	}
	t.Logf("maglev disruption on remove: %.3f", frac)
}

func buildViaPublic(names []string, tableSize int) []int {
	eps := make([]*lb.Endpoint, len(names))
	for i, n := range names {
		eps[i] = &lb.Endpoint{Addr: n, Weight: 1, Healthy: true}
	}
	m := lb.NewMaglev(eps, tableSize)
	// Probe table by hashing sequential keys and recording endpoint identity
	// (full table not exported). For disruption we rebuild via Update path.
	// Use internal-equivalent: create two Maglevs and sample.
	_ = m
	// Directly use package helper through picks isn't enough for table.
	// Export via disruption of pick distribution instead:
	return sampleTable(names, tableSize)
}

// sampleTable rebuilds maglev table using same algorithm as production via picks
// of keys "k-0".."k-tableSize-1" — approximate. Better: use MaglevDisruption on
// exported tables. We reimplement thin wrapper calling NewMaglev and reflecting
// through repeated picks is noisy. Use lb.NewMaglev and compare pick maps.
func sampleTable(names []string, tableSize int) []int {
	eps := make([]*lb.Endpoint, len(names))
	for i, n := range names {
		eps[i] = &lb.Endpoint{Addr: n, Weight: 1}
	}
	// Access table via package test in same package would work; here we
	// approximate by building in-package through NewMaglev and reading picks
	// of numeric keys that map uniformly.
	m := lb.NewMaglev(eps, tableSize)
	out := make([]int, tableSize)
	idxOf := map[string]int{}
	for i, n := range names {
		idxOf[n] = i
	}
	for i := 0; i < tableSize; i++ {
		ep, done, err := m.Pick(lb.PickInfo{HashKey: fmt.Sprintf("slot-%d", i)})
		if err != nil {
			out[i] = 0
			continue
		}
		out[i] = idxOf[ep.Addr]
		done(lb.DoneInfo{})
	}
	return out
}
