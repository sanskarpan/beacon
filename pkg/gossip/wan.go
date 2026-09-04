package gossip

import (
	"sync"
)

// WANPool models multi-datacenter federation via WAN gossip.
// Each DC has a LAN membership; WAN links DC gateways and floods catalog
// digests across DCs at a lower rate than LAN.
type WANPool struct {
	mu               sync.RWMutex
	dcs              map[string]*DCLink // dc name → link
	self             string
	wildcardHandlers []func(fromDC string, index uint64, payload []byte)
}

// DCLink is one datacenter's WAN presence.
type DCLink struct {
	Name     string
	Gateways []Member // WAN gossip members (servers)
	// CatalogDigest is a coarse hash/index of the DC's catalog for anti-entropy.
	CatalogIndex uint64
	// OnReceive is invoked when a remote DC floods a catalog snapshot notice.
	Handlers []func(fromDC string, index uint64, payload []byte)
}

// NewWAN creates a WAN pool for the local DC.
func NewWAN(localDC string) *WANPool {
	return &WANPool{
		dcs:  make(map[string]*DCLink),
		self: localDC,
	}
}

// JoinDC registers a remote DC gateway.
func (w *WANPool) JoinDC(dc string, gateways []Member) {
	w.mu.Lock()
	defer w.mu.Unlock()
	link := w.dcs[dc]
	if link == nil {
		link = &DCLink{Name: dc}
		w.dcs[dc] = link
		// apply wildcard handlers to new DC
		link.Handlers = append(append([]func(string, uint64, []byte){}, w.wildcardHandlers...), link.Handlers...)
	}
	link.Gateways = append([]Member(nil), gateways...)
}

// LeaveDC removes a remote DC.
func (w *WANPool) LeaveDC(dc string) {
	w.mu.Lock()
	delete(w.dcs, dc)
	w.mu.Unlock()
}

// Flood advertises a catalog index/payload to all remote DCs.
func (w *WANPool) Flood(index uint64, payload []byte) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for name, link := range w.dcs {
		if name == w.self {
			continue
		}
		for _, h := range link.Handlers {
			h(w.self, index, payload)
		}
		// update remote view of our index on their side is their handler's job
		_ = link
	}
}

// OnFlood registers a handler for floods from a specific DC (or "*" for all).
func (w *WANPool) OnFlood(dc string, fn func(fromDC string, index uint64, payload []byte)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if dc == "*" {
		w.wildcardHandlers = append(w.wildcardHandlers, fn)
		for _, link := range w.dcs {
			link.Handlers = append(link.Handlers, fn)
		}
		if w.dcs[w.self] == nil {
			w.dcs[w.self] = &DCLink{Name: w.self, Handlers: append([]func(string, uint64, []byte){}, w.wildcardHandlers...)}
		}
		return
	}
	link := w.dcs[dc]
	if link == nil {
		link = &DCLink{Name: dc}
		w.dcs[dc] = link
	}
	link.Handlers = append(link.Handlers, fn)
}

// Datacenters lists known DCs.
func (w *WANPool) Datacenters() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]string, 0, len(w.dcs))
	for k := range w.dcs {
		out = append(out, k)
	}
	return out
}

// Deliver simulates receiving a flood from a remote DC (test/sim helper).
func (w *WANPool) Deliver(fromDC string, index uint64, payload []byte) {
	w.mu.RLock()
	var handlers []func(string, uint64, []byte)
	if link := w.dcs[fromDC]; link != nil {
		handlers = append(handlers, link.Handlers...)
		link.CatalogIndex = index
	}
	// also fire wildcard handlers stored on self if no specific handler
	if len(handlers) == 0 && w.dcs[w.self] != nil {
		handlers = append(handlers, w.dcs[w.self].Handlers...)
	}
	w.mu.RUnlock()
	for _, h := range handlers {
		h(fromDC, index, payload)
	}
}
