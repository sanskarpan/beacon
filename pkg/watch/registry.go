// Package watch implements the watch registry, blocking queries, cache, and staggered fan-out.
package watch

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"golang.org/x/sync/singleflight"
)

// ErrIndexCompacted is returned when a client's index has aged out of the ring
// buffer. The client must re-list — never silently skip events.
var ErrIndexCompacted = errors.New("index too old; re-list required")

// Event is a watch notification.
type Event struct {
	Kind      string              `json:"kind"` // snapshot | add | update | remove
	Service   string              `json:"service"`
	Instances []*catalog.Instance `json:"instances,omitempty"`
	Index     uint64              `json:"index"`
	TraceID   string              `json:"trace_id,omitempty"`
}

// Registry manages per-service subscriptions with staggered fan-out.
type Registry struct {
	mu       sync.RWMutex
	store    *catalog.Store
	clk      clock.Clock
	bus      *events.Bus
	watchers map[string][]*watcher
	cache    *Cache
	sf       singleflight.Group
	rng      *rand.Rand
	// fan-out: 200µs per watcher, cap 500ms
	perWatcherSpread time.Duration
	maxSpread        time.Duration
	nextID           atomic.Uint64
}

type watcher struct {
	id      uint64
	service string
	ch      chan Event
	// lastIdx is written by the serve goroutine and read by Stats()
	// under RLock — plain uint64 was a data race (#86).
	lastIdx atomic.Uint64
	cancel  context.CancelFunc

	sendMu   sync.Mutex
	closed   bool
	needSync atomic.Bool
}

// Option configures the registry.
type Option func(*Registry)

// WithWatchBus attaches events.
func WithWatchBus(b *events.Bus) Option { return func(r *Registry) { r.bus = b } }

// WithWatchClock injects clock.
func WithWatchClock(c clock.Clock) Option { return func(r *Registry) { r.clk = c } }

// NewRegistry creates a watch registry.
func NewRegistry(store *catalog.Store, opts ...Option) *Registry {
	r := &Registry{
		store:            store,
		clk:              clock.New(),
		watchers:         make(map[string][]*watcher),
		cache:            NewCache(1000),
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
		perWatcherSpread: 200 * time.Microsecond,
		maxSpread:        500 * time.Millisecond,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Cache returns the event ring buffer.
func (r *Registry) Cache() *Cache { return r.cache }

// Watch opens a subscription. First delivery is a snapshot; subsequent are deltas.
func (r *Registry) Watch(ctx context.Context, service string, fromIndex uint64) (<-chan Event, error) {
	//nolint:gosec // G118: cancel is stored on the watcher and invoked by remove/unsubscribe; ownership is transferred, not lost.
	ctx, cancel := context.WithCancel(ctx)
	w := &watcher{
		service: service,
		ch:      make(chan Event, 16),
		cancel:  cancel,
	}
	w.lastIdx.Store(fromIndex)

	r.mu.Lock()
	w.id = r.nextID.Add(1)
	r.watchers[service] = append(r.watchers[service], w)
	r.mu.Unlock()

	if r.bus != nil {
		r.bus.Publish(events.Event{
			Kind:    events.EvWatchOpened,
			Service: service,
			Index:   fromIndex,
		})
	}

	// Resume from cache or send snapshot.
	go r.serve(ctx, w)

	go func() {
		<-ctx.Done()
		r.remove(w)
		w.closeCh()
	}()

	return w.ch, nil
}

// trySend delivers an event without blocking; returns false if closed or full.
func (w *watcher) trySend(ev Event) (sent, closed bool) {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if w.closed {
		return false, true
	}
	select {
	case w.ch <- ev:
		return true, false
	default:
		w.needSync.Store(true)
		return false, false
	}
}

func (w *watcher) closeCh() {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	close(w.ch)
}

func (r *Registry) serve(ctx context.Context, w *watcher) {
	// Try cache resumption
	if w.lastIdx.Load() > 0 {
		evs, err := r.cache.Since(w.lastIdx.Load())
		if err == ErrIndexCompacted {
			if r.bus != nil {
				r.bus.Publish(events.Event{Kind: events.EvWatchCompacted, Service: w.service, Index: w.lastIdx.Load()})
			}
			// fall through to snapshot
		} else if err == nil && len(evs) > 0 {
			for _, ev := range evs {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if sent, closed := w.trySend(ev); closed {
					return
				} else if sent {
					w.lastIdx.Store(ev.Index)
				}
			}
			return // subsequent updates via Notify
		}
	}

	// Snapshot via singleflight
	v, err, _ := r.sf.Do("snap:"+w.service, func() (any, error) {
		return r.store.GetNow(w.service, catalog.QueryOptions{}), nil
	})
	if err != nil {
		return
	}
	res := v.(*catalog.Result)
	ev := Event{
		Kind:      "snapshot",
		Service:   w.service,
		Instances: res.Instances,
		Index:     res.Index,
	}
	select {
	case <-ctx.Done():
		return
	default:
	}
	if sent, closed := w.trySend(ev); !closed && sent {
		w.lastIdx.Store(res.Index)
	}
}

// Notify fans out a catalog change with staggered delivery.
func (r *Registry) Notify(service string, ev Event) {
	r.cache.Append(ev)

	r.mu.RLock()
	ws := append([]*watcher(nil), r.watchers[service]...)
	r.mu.RUnlock()

	n := len(ws)
	if n == 0 {
		return
	}

	spread := time.Duration(n) * r.perWatcherSpread
	if spread > r.maxSpread {
		spread = r.maxSpread
	}

	// Herd detection: collect planned fire times
	timestamps := make([]time.Time, 0, n)
	base := r.clk.Now()

	for i, w := range ws {
		delay := time.Duration(0)
		if n > 1 {
			delay = time.Duration(i) * spread / time.Duration(n)
		}
		timestamps = append(timestamps, base.Add(delay))
		r.clk.AfterFunc(delay, func() {
			sent, closed := w.trySend(ev)
			if closed || !sent {
				return
			}
			if r.bus != nil {
				r.bus.Publish(events.Event{
					Kind:    events.EvWatchNotified,
					Service: service,
					Index:   ev.Index,
					TraceID: ev.TraceID,
				})
			}
		})
	}

	r.detectHerd(service, timestamps)
}

func (r *Registry) detectHerd(service string, ts []time.Time) {
	if len(ts) < 10 {
		return
	}
	// bucket by 10ms
	buckets := map[int64]int{}
	for _, t := range ts {
		b := t.UnixMilli() / 10
		buckets[b]++
	}
	for _, c := range buckets {
		if c >= len(ts)/2 && c >= 10 {
			if r.bus != nil {
				r.bus.Publish(events.Event{
					Kind:    events.EvHerdDetected,
					Service: service,
					Detail:  "notification spike in 10ms bucket",
				})
			}
			return
		}
	}
}

func (r *Registry) remove(target *watcher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ws := r.watchers[target.service]
	keep := ws[:0]
	for _, w := range ws {
		if w != target {
			keep = append(keep, w)
		}
	}
	if len(keep) == 0 {
		delete(r.watchers, target.service)
	} else {
		r.watchers[target.service] = keep
	}
}

// WatcherCount returns open watchers for a service.
func (r *Registry) WatcherCount(service string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.watchers[service])
}

// Stats returns watcher table for console.
func (r *Registry) Stats() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := 0
	list := []map[string]any{}
	for svc, ws := range r.watchers {
		total += len(ws)
		for _, w := range ws {
			list = append(list, map[string]any{
				"service": svc,
				"id":      w.id,
				"index":   w.lastIdx.Load(),
			})
		}
	}
	return map[string]any{
		"total_watchers": total,
		"watchers":       list,
		"cache": map[string]any{
			"oldest": r.cache.Oldest(),
			"newest": r.cache.Newest(),
			"size":   len(r.cache.Events()),
		},
	}
}

// ---------------------------------------------------------------------------
// Blocking query helpers
// ---------------------------------------------------------------------------

var globalJitterMu sync.Mutex
var globalJitterRng = rand.New(rand.NewSource(1))

// JitterDuration applies ±fraction jitter (default 16%) to break phase-lock herds.
func JitterDuration(d time.Duration, fraction float64, rng *rand.Rand) time.Duration {
	if d <= 0 {
		return d
	}
	if fraction <= 0 {
		fraction = 0.16
	}
	if rng == nil {
		globalJitterMu.Lock()
		rng = globalJitterRng
		// use Float64 under lock to avoid per-request NewSource
		f := 1 + (rng.Float64()*2-1)*fraction
		globalJitterMu.Unlock()
		return time.Duration(float64(d) * f)
	}
	// uniform in [1-f, 1+f]
	f := 1 + (rng.Float64()*2-1)*fraction
	return time.Duration(float64(d) * f)
}

// BlockingQuery runs a Consul-style blocking read against the catalog.
//
//  1. Jitter the wait timeout by ±16%
//  2. Guard against a future index (reset to 0)
//  3. On timeout, return current state — not an error
func BlockingQuery(ctx context.Context, store *catalog.Store, service string, opts catalog.QueryOptions, clk clock.Clock, rng *rand.Rand) (*catalog.Result, error) {
	// NOTE: wakeups are driven by the catalog's own (clock-injected) index;
	// only the timeout edge below is wall-clock, because Go context deadlines
	// cannot run on a virtual clock. clk is kept for API stability and future
	// virtual-deadline support.
	_ = clk
	wait := opts.Wait
	if wait <= 0 {
		wait = 5 * time.Minute
	}
	wait = JitterDuration(wait, 0.16, rng)

	// Future index guard
	if opts.MinIndex > store.Index() {
		opts.MinIndex = 0
	}

	if opts.MinIndex == 0 {
		return store.GetNow(service, opts), nil
	}

	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	res, err := store.Get(ctx, service, opts)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// On timeout return current state, not an error.
		return store.GetNow(service, opts), nil
	}
	return res, err
}
