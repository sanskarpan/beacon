// Package events is the observability bus for beacon.
//
// Every significant state change emits an Event carrying a TraceID so a single
// registration can be followed: SDK → agent → catalog → gossip → watch → client.
// Without TraceID from day zero, the Phase 16 propagation timeline is unbuildable.
package events

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
)

// Kind identifies an event type.
type Kind string

const (
	// Registration
	EvInstanceRegistered   Kind = "instance.registered"
	EvInstanceDeregistered Kind = "instance.deregistered"
	EvLeaseRenewed         Kind = "lease.renewed"
	EvLeaseExpired         Kind = "lease.expired"
	EvLeaseGranted         Kind = "lease.granted"

	// Health
	EvCheckExecuted    Kind = "check.executed"
	EvHealthChanged    Kind = "health.changed"
	EvFlappingDetected Kind = "health.flapping"
	EvOutlierEjected   Kind = "outlier.ejected"
	EvOutlierReturned  Kind = "outlier.returned"
	EvEjectionCapReached Kind = "outlier.ejection_cap"
	EvPanicModeEntered Kind = "lb.panic_mode"

	// Propagation
	EvGossipDelta     Kind = "gossip.delta"
	EvAntiEntropySync Kind = "antientropy.sync"
	EvConverged       Kind = "propagation.converged"
	EvNodeFailed      Kind = "node.failed"
	EvNodeJoined      Kind = "node.joined"

	// Watch
	EvWatchOpened    Kind = "watch.opened"
	EvWatchNotified  Kind = "watch.notified"
	EvWatchCompacted Kind = "watch.compacted"
	EvHerdDetected   Kind = "watch.herd"

	// xDS
	EvXDSPush Kind = "xds.push"
	EvXDSAck  Kind = "xds.ack"
	EvXDSNack Kind = "xds.nack"

	// Resolution
	EvResolveRequest    Kind = "resolve.request"
	EvStaleEndpointUsed Kind = "resolve.stale"

	// Index
	EvIndexBumped Kind = "catalog.index_bumped"
)

// Event is a single observability record.
type Event struct {
	Kind      Kind           `json:"kind"`
	Timestamp time.Time      `json:"timestamp"`
	TraceID   string         `json:"trace_id,omitempty"`
	Node      string         `json:"node,omitempty"`
	Service   string         `json:"service,omitempty"`
	Instance  string         `json:"instance,omitempty"`
	Index     uint64         `json:"index,omitempty"`
	From      string         `json:"from,omitempty"`
	To        string         `json:"to,omitempty"`
	Detail    string         `json:"detail,omitempty"`
	Elapsed   time.Duration  `json:"elapsed,omitempty"`
	Adds      int            `json:"adds,omitempty"`
	Removes   int            `json:"removes,omitempty"`
	Updates   int            `json:"updates,omitempty"`
	Meta      map[string]any `json:"meta,omitempty"`
}

// Bus is a non-blocking pub/sub fan-out.
//
// Publish never blocks: slow subscribers drop events (and increment a counter)
// rather than stalling the control plane.
type Bus struct {
	clk     clock.Clock
	mu      sync.RWMutex
	subs    map[uint64]chan Event
	nextID  uint64
	dropped atomic.Uint64
	jsonl   io.Writer
	filter  func(Event) bool
}

// NewBus creates an event bus. clk may be nil (wall clock used).
func NewBus(clk clock.Clock) *Bus {
	if clk == nil {
		clk = clock.New()
	}
	return &Bus{
		clk:  clk,
		subs: make(map[uint64]chan Event),
	}
}

// SetJSONLWriter writes every event as a JSON line (for sim traces / console replay).
func (b *Bus) SetJSONLWriter(w io.Writer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.jsonl = w
}

// Subscribe returns a buffered channel of events and an unsubscribe function.
func (b *Bus) Subscribe(buf int) (<-chan Event, func()) {
	if buf <= 0 {
		buf = 256
	}
	ch := make(chan Event, buf)
	b.mu.Lock()
	id := b.nextID
	b.nextID++
	b.subs[id] = ch
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs, id)
		b.mu.Unlock()
		// drain without closing from publisher side races — close here
		close(ch)
	}
}

// Publish emits an event to all subscribers. Never blocks.
func (b *Bus) Publish(ev Event) {
	if ev.Timestamp.IsZero() {
		ev.Timestamp = b.clk.Now()
	}
	b.mu.RLock()
	jsonl := b.jsonl
	subs := make([]chan Event, 0, len(b.subs))
	for _, ch := range b.subs {
		subs = append(subs, ch)
	}
	b.mu.RUnlock()

	if jsonl != nil {
		_ = json.NewEncoder(jsonl).Encode(ev)
	}

	for _, ch := range subs {
		// Protect against concurrent unsubscribe closing the channel.
		func(ch chan Event) {
			defer func() { _ = recover() }()
			select {
			case ch <- ev:
			default:
				b.dropped.Add(1)
			}
		}(ch)
	}
}

// Dropped returns how many events were dropped due to slow consumers.
func (b *Bus) Dropped() uint64 { return b.dropped.Load() }

// Emit is a convenience for constructing and publishing.
func (b *Bus) Emit(kind Kind, opts ...func(*Event)) {
	ev := Event{Kind: kind}
	for _, o := range opts {
		o(&ev)
	}
	b.Publish(ev)
}

// WithTrace sets TraceID.
func WithTrace(id string) func(*Event) {
	return func(e *Event) { e.TraceID = id }
}

// WithInstance sets instance + service fields.
func WithInstance(service, instance string) func(*Event) {
	return func(e *Event) { e.Service = service; e.Instance = instance }
}

// WithIndex sets catalog index.
func WithIndex(idx uint64) func(*Event) {
	return func(e *Event) { e.Index = idx }
}

// WithDetail sets free-form detail.
func WithDetail(d string) func(*Event) {
	return func(e *Event) { e.Detail = d }
}

// WithNode sets node name.
func WithNode(n string) func(*Event) {
	return func(e *Event) { e.Node = n }
}

// WithElapsed sets elapsed duration.
func WithElapsed(d time.Duration) func(*Event) {
	return func(e *Event) { e.Elapsed = d }
}

// TraceIDKey is the context key for TraceID.
type ctxKey int

const traceKey ctxKey = 1

// ContextWithTrace returns a child context carrying a TraceID.
func ContextWithTrace(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceKey, id)
}

// TraceFrom extracts a TraceID from context, or empty string.
func TraceFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(traceKey).(string); ok {
		return v
	}
	return ""
}
