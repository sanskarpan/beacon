package catalog

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/trace"
)

// LeaseManager grants, renews, and expires leases using a single timer heap.
// 10,000 leases must not be 10,000 goroutines.
type LeaseManager struct {
	mu      sync.Mutex
	clk     clock.Clock
	store   *Store
	bus     *events.Bus
	leases  map[string]*leaseEntry // by lease ID
	byInst  map[string]string      // instance ID → lease ID
	pq      leaseHeap
	timer   clock.Timer
	stopped bool
	// Grace period after expiry during which a renewal restores the instance.
	grace time.Duration
	// Default deregister delay after critical-on-expiry.
	defaultDeregisterAfter time.Duration
}

type leaseEntry struct {
	lease     Lease
	critical  bool // true once expired but not yet removed
	removeAt  time.Time
	heapIdx   int
	// nextAction is when we next need to wake (expiry or removal)
	nextAction time.Time
}

// LeaseManagerOption configures the manager.
type LeaseManagerOption func(*LeaseManager)

// WithLeaseBus attaches events.
func WithLeaseBus(b *events.Bus) LeaseManagerOption {
	return func(m *LeaseManager) { m.bus = b }
}

// WithGrace sets the post-expiry renewal grace window.
func WithGrace(d time.Duration) LeaseManagerOption {
	return func(m *LeaseManager) { m.grace = d }
}

// NewLeaseManager creates a manager bound to a catalog store.
func NewLeaseManager(store *Store, clk clock.Clock, opts ...LeaseManagerOption) *LeaseManager {
	if clk == nil {
		clk = clock.New()
	}
	m := &LeaseManager{
		clk:                    clk,
		store:                  store,
		leases:                 make(map[string]*leaseEntry),
		byInst:                 make(map[string]string),
		grace:                  2 * time.Second,
		defaultDeregisterAfter: 30 * time.Second,
	}
	for _, o := range opts {
		o(m)
	}
	heap.Init(&m.pq)
	return m
}

// GrantLease attaches a new lease to an instance.
func (m *LeaseManager) GrantLease(ctx context.Context, instanceID string, ttl, deregisterAfter time.Duration) (*Lease, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("lease ttl must be positive")
	}
	if deregisterAfter <= 0 {
		deregisterAfter = m.defaultDeregisterAfter
	}
	now := m.clk.Now()
	id := trace.NewIDAt(now)
	l := &Lease{
		ID:              id,
		TTL:             ttl,
		GrantedAt:       now,
		ExpiresAt:       now.Add(ttl),
		DeregisterAfter: deregisterAfter,
		InstanceID:      instanceID,
	}

	m.mu.Lock()
	// replace existing
	if oldID, ok := m.byInst[instanceID]; ok {
		if old, ok2 := m.leases[oldID]; ok2 {
			heap.Remove(&m.pq, old.heapIdx)
			delete(m.leases, oldID)
		}
	}
	e := &leaseEntry{
		lease:      *l,
		nextAction: l.ExpiresAt,
	}
	m.leases[id] = e
	m.byInst[instanceID] = id
	heap.Push(&m.pq, e)
	m.rescheduleLocked()
	m.mu.Unlock()

	// attach to instance in catalog without health bump if already set
	if inst, ok := m.store.GetInstance(instanceID); ok {
		inst.Lease = l
		_, _ = m.store.Register(ctx, inst)
	}

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Kind:     events.EvLeaseGranted,
			TraceID:  events.TraceFrom(ctx),
			Instance: instanceID,
			Detail:   fmt.Sprintf("ttl=%s", ttl),
		})
	}
	return l, nil
}

// RenewLease extends a lease. Idempotent and does NOT bump the catalog index
// if nothing else changed — only the lease expiry moves.
func (m *LeaseManager) RenewLease(ctx context.Context, leaseID string) (*Lease, error) {
	m.mu.Lock()
	e, ok := m.leases[leaseID]
	if !ok {
		m.mu.Unlock()
		return nil, ErrNotFound
	}
	now := m.clk.Now()
	// Grace: accept slightly after expiry.
	if now.After(e.lease.ExpiresAt.Add(m.grace)) && e.critical {
		// past grace and already critical — still allow restore within deregister window
		if !now.Before(e.removeAt) {
			m.mu.Unlock()
			return nil, fmt.Errorf("lease expired past grace: %w", ErrNotFound)
		}
	}
	e.lease.ExpiresAt = now.Add(e.lease.TTL)
	e.critical = false
	e.removeAt = time.Time{}
	e.nextAction = e.lease.ExpiresAt
	heap.Fix(&m.pq, e.heapIdx)
	m.rescheduleLocked()
	l := e.lease
	instID := l.InstanceID
	m.mu.Unlock()

	// Restore health to passing if we had marked critical for expiry.
	// Renewal itself must not bump index if health already passing.
	_, _ = m.store.UpdateHealth(ctx, instID, HealthPassing)

	// Update lease pointer on instance without forcing unnecessary churn:
	// re-register only lease field via Get+Register would bump — skip.
	// Health update handles restore; lease times live in the manager.

	if m.bus != nil {
		m.bus.Publish(events.Event{
			Kind:     events.EvLeaseRenewed,
			TraceID:  events.TraceFrom(ctx),
			Instance: instID,
		})
	}
	return &l, nil
}

// RevokeLease drops a lease without removing the instance.
func (m *LeaseManager) RevokeLease(leaseID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.leases[leaseID]
	if !ok {
		return nil
	}
	heap.Remove(&m.pq, e.heapIdx)
	delete(m.leases, leaseID)
	delete(m.byInst, e.lease.InstanceID)
	m.rescheduleLocked()
	return nil
}

// Start begins the single expiry loop. Call once.
func (m *LeaseManager) Start(ctx context.Context) {
	go m.loop(ctx)
}

func (m *LeaseManager) loop(ctx context.Context) {
	for {
		m.mu.Lock()
		var wait time.Duration
		if m.pq.Len() == 0 {
			wait = time.Hour
		} else {
			next := m.pq[0].nextAction
			wait = next.Sub(m.clk.Now())
			if wait < 0 {
				wait = 0
			}
		}
		if m.timer != nil {
			m.timer.Stop()
		}
		m.timer = m.clk.NewTimer(wait)
		t := m.timer
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case <-t.C():
			m.fireDue(ctx)
		}
	}
}

func (m *LeaseManager) fireDue(ctx context.Context) {
	now := m.clk.Now()
	for {
		m.mu.Lock()
		if m.pq.Len() == 0 {
			m.mu.Unlock()
			return
		}
		e := m.pq[0]
		if e.nextAction.After(now) {
			m.rescheduleLocked()
			m.mu.Unlock()
			return
		}
		heap.Pop(&m.pq)
		if !e.critical {
			// Expiry: mark critical, schedule removal.
			e.critical = true
			e.removeAt = e.lease.ExpiresAt.Add(e.lease.DeregisterAfter)
			e.nextAction = e.removeAt
			heap.Push(&m.pq, e)
			instID := e.lease.InstanceID
			m.mu.Unlock()

			_, _ = m.store.UpdateHealth(ctx, instID, HealthCritical)
			if m.bus != nil {
				m.bus.Publish(events.Event{
					Kind:     events.EvLeaseExpired,
					Instance: instID,
					Detail:   "marked critical; removal delayed",
				})
			}
			continue
		}
		// Removal
		instID := e.lease.InstanceID
		leaseID := e.lease.ID
		delete(m.leases, leaseID)
		delete(m.byInst, instID)
		m.mu.Unlock()
		_, _ = m.store.Deregister(ctx, instID)
	}
}

func (m *LeaseManager) rescheduleLocked() {
	// timer reset happens in loop; this is a hook for tests
}

// ActiveCount returns number of tracked leases.
func (m *LeaseManager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.leases)
}

// ProcessDue fires all leases that are due as of the current clock.
// Useful with a virtual clock from tests (avoids racing the background loop).
func (m *LeaseManager) ProcessDue(ctx context.Context) {
	m.fireDue(ctx)
}

// GetLease returns a copy of the lease if present.
func (m *LeaseManager) GetLease(leaseID string) (Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.leases[leaseID]
	if !ok {
		return Lease{}, false
	}
	return e.lease, true
}

// --- heap ---

type leaseHeap []*leaseEntry

func (h leaseHeap) Len() int { return len(h) }
func (h leaseHeap) Less(i, j int) bool {
	return h[i].nextAction.Before(h[j].nextAction)
}
func (h leaseHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}
func (h *leaseHeap) Push(x any) {
	e := x.(*leaseEntry)
	e.heapIdx = len(*h)
	*h = append(*h, e)
}
func (h *leaseHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.heapIdx = -1
	*h = old[:n-1]
	return e
}
