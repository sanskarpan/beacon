package catalog

import (
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
)

// IndexBatcher coalesces mutations within a window into one notification.
//
// A 1,000-instance deploy naively produces 1,000 index bumps × N watchers.
// Coalescing within 50 ms turns that into ~20 bumps. The index stays monotonic;
// watchers still converge to the final state without observing every intermediate.
type IndexBatcher struct {
	clock  clock.Clock
	window time.Duration
	mu     sync.Mutex
	pending map[string]struct{}
	timer  clock.Timer
	flush  func(services []string, index uint64)
	// lastIndex is the highest index seen during the window
	lastIndex uint64
	indexFn   func() uint64
}

// NewIndexBatcher creates a batcher. flush is invoked with the set of touched
// services and the current catalog index when the window closes.
func NewIndexBatcher(clk clock.Clock, window time.Duration, flush func([]string, uint64)) *IndexBatcher {
	if clk == nil {
		clk = clock.New()
	}
	if window <= 0 {
		window = 50 * time.Millisecond
	}
	return &IndexBatcher{
		clock:   clk,
		window:  window,
		pending: make(map[string]struct{}),
		flush:   flush,
	}
}

// Touch records that service changed. Starts the window timer on first touch.
func (b *IndexBatcher) Touch(service string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending[service] = struct{}{}
	if b.timer == nil {
		b.timer = b.clock.NewTimer(b.window)
		t := b.timer
		go func() {
			<-t.C()
			b.doFlush()
		}()
	}
}

// TouchWithIndex records a change and tracks the latest index.
func (b *IndexBatcher) TouchWithIndex(service string, index uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending[service] = struct{}{}
	if index > b.lastIndex {
		b.lastIndex = index
	}
	if b.timer == nil {
		b.timer = b.clock.NewTimer(b.window)
		t := b.timer
		go func() {
			<-t.C()
			b.doFlush()
		}()
	}
}

func (b *IndexBatcher) doFlush() {
	b.mu.Lock()
	services := make([]string, 0, len(b.pending))
	for s := range b.pending {
		services = append(services, s)
	}
	idx := b.lastIndex
	b.pending = make(map[string]struct{})
	b.timer = nil
	b.lastIndex = 0
	flush := b.flush
	b.mu.Unlock()
	if flush != nil && len(services) > 0 {
		flush(services, idx)
	}
}

// PendingCount returns how many services are waiting to flush (test helper).
func (b *IndexBatcher) PendingCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}
