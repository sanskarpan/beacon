package lb

import (
	"fmt"
	"sync"
)

// Maglev implements Google Maglev consistent hashing.
// Better disruption properties than ring hash on membership change: when one
// endpoint is removed, only ~1/N of keys move (minimal disruption).
type Maglev struct {
	mu     sync.RWMutex
	eps    []*Endpoint
	table  []int // lookup table of endpoint indices
	tableN int
}

// NewMaglev builds a Maglev table. tableSize should be prime (default 65537).
func NewMaglev(eps []*Endpoint, tableSize int) *Maglev {
	if tableSize <= 0 {
		tableSize = 65537
	}
	m := &Maglev{tableN: tableSize}
	m.Update(eps)
	return m
}

func (m *Maglev) Name() string { return "maglev" }

func (m *Maglev) Update(eps []*Endpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eps = append([]*Endpoint(nil), eps...)
	m.table = buildMaglev(len(eps), m.tableN, func(i int) string {
		if i < 0 || i >= len(eps) {
			return ""
		}
		return eps[i].Addr
	})
}

func (m *Maglev) Pick(info PickInfo) (*Endpoint, func(DoneInfo), error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.eps) == 0 || len(m.table) == 0 {
		return nil, nil, ErrNoEndpoint
	}
	key := info.HashKey
	if key == "" {
		key = fmt.Sprintf("%p", m) // fallback; callers should set HashKey
	}
	h := fnv32(key)
	idx := m.table[int(h)%len(m.table)]
	if idx < 0 || idx >= len(m.eps) {
		idx = 0
	}
	ep := m.eps[idx]
	ep.Inflight.Add(1)
	return ep, func(DoneInfo) { ep.Inflight.Add(-1) }, nil
}

// buildMaglev constructs the Maglev lookup table of size M for N backends.
// Algorithm from the Maglev paper (permutation tables + greedy fill).
func buildMaglev(n, m int, name func(int) string) []int {
	if n == 0 || m == 0 {
		return nil
	}
	// preference lists: for each backend i, perm[i][j] = (offset + j*skip) % M
	type pref struct {
		offset, skip uint32
	}
	// m > 0 guarded above; next[i] >= 0 by construction — conversions cannot overflow.
	mu := uint32(m) //nolint:gosec // G115: bounded by constructor guard
	prefs := make([]pref, n)
	for i := 0; i < n; i++ {
		h1 := fnv32(name(i) + "#offset")
		h2 := fnv32(name(i) + "#skip")
		prefs[i] = pref{
			offset: h1 % mu,
			skip:   h2%(mu-1) + 1,
		}
	}
	entry := make([]int, m)
	for i := range entry {
		entry[i] = -1
	}
	next := make([]int, n) // next index into each backend's permutation
	filled := 0
	for filled < m {
		for i := 0; i < n && filled < m; i++ {
			c := int((prefs[i].offset + uint32(next[i])*prefs[i].skip) % mu) //nolint:gosec // G115: next[i] >= 0 counter
			next[i]++
			// find next empty slot along permutation
			for entry[c] >= 0 {
				c = int((prefs[i].offset + uint32(next[i])*prefs[i].skip) % mu) //nolint:gosec // G115: next[i] >= 0 counter
				next[i]++
			}
			entry[c] = i
			filled++
		}
	}
	return entry
}

// Disruption measures the fraction of table slots that change when going from
// a to b (same table size). Used in tests to show Maglev's better properties.
func MaglevDisruption(a, b []int) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 1
	}
	diff := 0
	for i := range a {
		if a[i] != b[i] {
			diff++
		}
	}
	return float64(diff) / float64(len(a))
}
