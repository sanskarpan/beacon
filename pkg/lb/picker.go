// Package lb implements load-balancing policies: RR, WRR, least-request, P2C, ring-hash, locality.
package lb

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Endpoint is a selectable backend.
type Endpoint struct {
	Addr     string
	Weight   int
	Inflight atomic.Int64
	Healthy  bool
	Zone     string
	Region   string
	// Meta for affinity
	Meta map[string]string
}

// DoneInfo is the outcome of a call.
type DoneInfo struct {
	Err error
}

// PickInfo carries optional affinity key.
type PickInfo struct {
	HashKey string
}

// Picker selects an endpoint.
type Picker interface {
	Pick(info PickInfo) (ep *Endpoint, done func(DoneInfo), err error)
	Update(endpoints []*Endpoint)
	Name() string
}

// ---------------------------------------------------------------------------
// Round-robin
// ---------------------------------------------------------------------------

// RoundRobin cycles with an atomic counter.
type RoundRobin struct {
	mu  sync.RWMutex
	eps []*Endpoint
	idx atomic.Uint64
}

func NewRoundRobin(eps []*Endpoint) *RoundRobin {
	r := &RoundRobin{}
	r.Update(eps)
	return r
}
func (r *RoundRobin) Name() string { return "round_robin" }
func (r *RoundRobin) Update(eps []*Endpoint) {
	r.mu.Lock()
	r.eps = append([]*Endpoint(nil), eps...)
	r.mu.Unlock()
}
func (r *RoundRobin) Pick(info PickInfo) (*Endpoint, func(DoneInfo), error) {
	_ = info
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.eps) == 0 {
		return nil, nil, ErrNoEndpoint
	}
	i := r.idx.Add(1) - 1
	ep := r.eps[i%uint64(len(r.eps))]
	ep.Inflight.Add(1)
	return ep, func(DoneInfo) { ep.Inflight.Add(-1) }, nil
}

// ---------------------------------------------------------------------------
// Smooth weighted round-robin (Nginx algorithm)
// ---------------------------------------------------------------------------

// WeightedRR implements smooth WRR so a 100:1 ratio doesn't allocate a 101-slice.
type WeightedRR struct {
	mu      sync.Mutex
	eps     []*Endpoint
	current []int
	total   int
}

func NewWeightedRR(eps []*Endpoint) *WeightedRR {
	w := &WeightedRR{}
	w.Update(eps)
	return w
}
func (w *WeightedRR) Name() string { return "weighted_round_robin" }
func (w *WeightedRR) Update(eps []*Endpoint) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.eps = append([]*Endpoint(nil), eps...)
	w.current = make([]int, len(eps))
	w.total = 0
	for _, e := range eps {
		wt := e.Weight
		if wt <= 0 {
			wt = 1
		}
		w.total += wt
	}
}
func (w *WeightedRR) Pick(info PickInfo) (*Endpoint, func(DoneInfo), error) {
	_ = info
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.eps) == 0 || w.total == 0 {
		return nil, nil, ErrNoEndpoint
	}
	best := -1
	for i, e := range w.eps {
		wt := e.Weight
		if wt <= 0 {
			wt = 1
		}
		w.current[i] += wt
		if best < 0 || w.current[i] > w.current[best] {
			best = i
		}
	}
	w.current[best] -= w.total
	ep := w.eps[best]
	ep.Inflight.Add(1)
	return ep, func(DoneInfo) { ep.Inflight.Add(-1) }, nil
}

// ---------------------------------------------------------------------------
// Least request
// ---------------------------------------------------------------------------

// LeastRequest always picks the lowest inflight (can herd — prefer P2C).
type LeastRequest struct {
	mu  sync.RWMutex
	eps []*Endpoint
}

func NewLeastRequest(eps []*Endpoint) *LeastRequest {
	l := &LeastRequest{}
	l.Update(eps)
	return l
}
func (l *LeastRequest) Name() string { return "least_request" }
func (l *LeastRequest) Update(eps []*Endpoint) {
	l.mu.Lock()
	l.eps = append([]*Endpoint(nil), eps...)
	l.mu.Unlock()
}
func (l *LeastRequest) Pick(info PickInfo) (*Endpoint, func(DoneInfo), error) {
	_ = info
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.eps) == 0 {
		return nil, nil, ErrNoEndpoint
	}
	best := l.eps[0]
	for _, e := range l.eps[1:] {
		if e.Inflight.Load() < best.Inflight.Load() {
			best = e
		}
	}
	best.Inflight.Add(1)
	return best, func(DoneInfo) { best.Inflight.Add(-1) }, nil
}

// ---------------------------------------------------------------------------
// Power of two choices
// ---------------------------------------------------------------------------

// P2C picks two at random and chooses the lower inflight/weight.
//
// "Pick the least loaded" is WORSE than random in a distributed setting: every
// client independently identifies the same least-loaded endpoint and stampedes
// it. P2C's randomness breaks that synchronisation.
type P2C struct {
	mu    sync.RWMutex
	eps   []*Endpoint
	rng   *rand.Rand
	rngMu sync.Mutex
	// optional outcome callback for outlier detection
	OnDone func(addr string, err error)
}

func NewP2C(eps []*Endpoint, rng *rand.Rand) *P2C {
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	p := &P2C{rng: rng}
	p.Update(eps)
	return p
}
func (p *P2C) Name() string { return "p2c" }
func (p *P2C) Update(eps []*Endpoint) {
	p.mu.Lock()
	p.eps = append([]*Endpoint(nil), eps...)
	p.mu.Unlock()
}
func (p *P2C) Pick(info PickInfo) (*Endpoint, func(DoneInfo), error) {
	_ = info
	p.mu.RLock()
	n := len(p.eps)
	if n == 0 {
		p.mu.RUnlock()
		return nil, nil, ErrNoEndpoint
	}
	if n == 1 {
		ep := p.eps[0]
		p.mu.RUnlock()
		ep.Inflight.Add(1)
		return ep, p.done(ep), nil
	}
	p.rngMu.Lock()
	a := p.rng.Intn(n)
	b := p.rng.Intn(n - 1)
	p.rngMu.Unlock()
	if b >= a {
		b++
	}
	sa, sb := p.eps[a], p.eps[b]
	p.mu.RUnlock()
	wa, wb := float64(sa.Weight), float64(sb.Weight)
	if wa <= 0 {
		wa = 1
	}
	if wb <= 0 {
		wb = 1
	}
	chosen := sa
	if float64(sb.Inflight.Load())/wb < float64(sa.Inflight.Load())/wa {
		chosen = sb
	}
	chosen.Inflight.Add(1)
	return chosen, p.done(chosen), nil
}

func (p *P2C) done(ep *Endpoint) func(DoneInfo) {
	return func(di DoneInfo) {
		ep.Inflight.Add(-1)
		if p.OnDone != nil {
			p.OnDone(ep.Addr, di.Err)
		}
	}
}

// ---------------------------------------------------------------------------
// Ring hash
// ---------------------------------------------------------------------------

// RingHash provides session affinity via consistent hashing with virtual nodes.
type RingHash struct {
	mu     sync.RWMutex
	eps    []*Endpoint
	ring   []ringNode
	vnodes int
}

type ringNode struct {
	hash uint32
	ep   *Endpoint
}

func NewRingHash(eps []*Endpoint, vnodes int) *RingHash {
	if vnodes <= 0 {
		vnodes = 100
	}
	r := &RingHash{vnodes: vnodes}
	r.Update(eps)
	return r
}
func (r *RingHash) Name() string { return "ring_hash" }
func (r *RingHash) Update(eps []*Endpoint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.eps = append([]*Endpoint(nil), eps...)
	r.ring = r.ring[:0]
	for _, ep := range eps {
		for i := 0; i < r.vnodes; i++ {
			h := fnv32(fmt.Sprintf("%s#%d", ep.Addr, i))
			r.ring = append(r.ring, ringNode{hash: h, ep: ep})
		}
	}
	// sort ring by hash for consistent lookup
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i].hash < r.ring[j].hash })
}
func (r *RingHash) Pick(info PickInfo) (*Endpoint, func(DoneInfo), error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return nil, nil, ErrNoEndpoint
	}
	key := info.HashKey
	if key == "" {
		key = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	h := fnv32(key)
	// binary search
	lo, hi := 0, len(r.ring)
	for lo < hi {
		mid := (lo + hi) / 2
		if r.ring[mid].hash < h {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= len(r.ring) {
		lo = 0
	}
	ep := r.ring[lo].ep
	ep.Inflight.Add(1)
	return ep, func(DoneInfo) { ep.Inflight.Add(-1) }, nil
}

func fnv32(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// ---------------------------------------------------------------------------
// Locality-aware priority with gradual overflow
// ---------------------------------------------------------------------------

// LocalityPicker prefers same zone, then region, then any — with gradual overflow
// (Envoy overprovisioning), not a cliff.
type LocalityPicker struct {
	mu             sync.RWMutex
	eps            []*Endpoint
	localZone      string
	localRegion    string
	overprovision  float64 // default 1.4
	panicThreshold float64 // default 0.5 — below this, route to all
	inner          Picker  // usually P2C
	innerMu        sync.Mutex
}

func NewLocalityPicker(eps []*Endpoint, zone, region string, inner Picker) *LocalityPicker {
	l := &LocalityPicker{
		localZone:      zone,
		localRegion:    region,
		overprovision:  1.4,
		panicThreshold: 0.5,
		inner:          inner,
	}
	if l.inner == nil {
		l.inner = NewP2C(nil, nil)
	}
	l.Update(eps)
	return l
}
func (l *LocalityPicker) Name() string { return "locality" }
func (l *LocalityPicker) Update(eps []*Endpoint) {
	l.mu.Lock()
	l.eps = append([]*Endpoint(nil), eps...)
	l.mu.Unlock()
}

func (l *LocalityPicker) healthyPercent(subset []*Endpoint) float64 {
	if len(subset) == 0 {
		return 0
	}
	ok := 0
	for _, e := range subset {
		if e.Healthy {
			ok++
		}
	}
	return float64(ok) / float64(len(subset))
}

// overflowWeight is Envoy's gradual formula: min(100, healthy% * 100 / overprovision).
func (l *LocalityPicker) overflowWeight(healthyPct float64) uint32 {
	return uint32(math.Min(100, healthyPct*100/l.overprovision))
}

func (l *LocalityPicker) Pick(info PickInfo) (*Endpoint, func(DoneInfo), error) {
	l.mu.RLock()
	eps := append([]*Endpoint(nil), l.eps...)
	l.mu.RUnlock()
	if len(eps) == 0 {
		return nil, nil, ErrNoEndpoint
	}

	// Panic mode: below threshold healthy, route to ALL endpoints.
	allHealthy := 0
	for _, e := range eps {
		if e.Healthy {
			allHealthy++
		}
	}
	if float64(allHealthy)/float64(len(eps)) < l.panicThreshold {
		l.innerMu.Lock()
		l.inner.Update(eps)
		ep, done, err := l.inner.Pick(info)
		l.innerMu.Unlock()
		return ep, done, err
	}

	var p0, p1, p2 []*Endpoint
	for _, e := range eps {
		if !e.Healthy {
			continue
		}
		switch {
		case e.Zone == l.localZone && l.localZone != "":
			p0 = append(p0, e)
		case e.Region == l.localRegion && l.localRegion != "":
			p1 = append(p1, e)
		default:
			p2 = append(p2, e)
		}
	}

	// Prefer p0 while overflow weight is high enough; otherwise spill.
	candidates := p0
	if len(candidates) == 0 || l.overflowWeight(l.healthyPercent(p0)) < 50 {
		candidates = append(candidates, p1...)
	}
	if len(candidates) == 0 {
		candidates = append(candidates, p2...)
	}
	if len(candidates) == 0 {
		// last resort: all
		candidates = eps
	}
	l.innerMu.Lock()
	l.inner.Update(candidates)
	ep, done, err := l.inner.Pick(info)
	l.innerMu.Unlock()
	return ep, done, err
}

// ErrNoEndpoint means the pool is empty.
var ErrNoEndpoint = fmt.Errorf("lb: no endpoint available")

// NewPicker constructs a named policy.
func NewPicker(policy string, eps []*Endpoint) Picker {
	switch policy {
	case "weighted_round_robin", "wrr":
		return NewWeightedRR(eps)
	case "least_request":
		return NewLeastRequest(eps)
	case "p2c":
		return NewP2C(eps, nil)
	case "ring_hash":
		return NewRingHash(eps, 100)
	case "maglev":
		return NewMaglev(eps, 65537)
	default:
		return NewRoundRobin(eps)
	}
}
