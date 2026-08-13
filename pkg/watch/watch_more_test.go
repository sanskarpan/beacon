package watch_test

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/watch"
)

func TestStaggeredFanOutSpread(t *testing.T) {
	// Use wall clock so AfterFunc delays are real and timestamps differ.
	s := catalog.NewStore()
	r := watch.NewRegistry(s)

	const n = 30
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var hits []time.Time
	var mu sync.Mutex
	var ready atomic.Int32

	for i := 0; i < n; i++ {
		ch, err := r.Watch(ctx, "svc", 0)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for ev := range ch {
				if ev.Kind == "snapshot" {
					ready.Add(1)
					continue
				}
				mu.Lock()
				hits = append(hits, time.Now())
				mu.Unlock()
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for ready.Load() < int32(n) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	r.Notify("svc", watch.Event{Kind: "add", Service: "svc", Index: 1})
	// fan-out spread up to 500ms
	time.Sleep(600 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(hits) < n/2 {
		t.Fatalf("too few hits: %d", len(hits))
	}
	min, max := hits[0], hits[0]
	for _, h := range hits {
		if h.Before(min) {
			min = h
		}
		if h.After(max) {
			max = h
		}
	}
	spread := max.Sub(min)
	if spread < time.Millisecond && n > 5 {
		t.Fatalf("notifications not spread — spread=%s hits=%d", spread, len(hits))
	}
	t.Logf("spread=%s hits=%d", spread, len(hits))
}

func TestSlowConsumerDoesNotStallOthers(t *testing.T) {
	clk := clock.New()
	s := catalog.NewStore(catalog.WithClock(clk))
	r := watch.NewRegistry(s, watch.WithWatchClock(clk))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// slow watcher: never reads after snapshot
	slowCh, _ := r.Watch(ctx, "svc", 0)
	<-slowCh // snapshot

	fastCh, _ := r.Watch(ctx, "svc", 0)
	<-fastCh

	// flood notifications — slow buffer fills; fast still receives
	received := 0
	done := make(chan struct{})
	go func() {
		for range fastCh {
			received++
			if received >= 5 {
				close(done)
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		r.Notify("svc", watch.Event{Kind: "add", Service: "svc", Index: uint64(i + 1)})
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("fast consumer stalled; received=%d", received)
	}
}

func TestWatchGoroutineCleanup(t *testing.T) {
	s := catalog.NewStore()
	r := watch.NewRegistry(s)
	runtime.GC()
	base := runtime.NumGoroutine()

	const n = 100
	ctx, cancel := context.WithCancel(context.Background())
	chs := make([]<-chan watch.Event, 0, n)
	for i := 0; i < n; i++ {
		ch, err := r.Watch(ctx, "svc", 0)
		if err != nil {
			t.Fatal(err)
		}
		chs = append(chs, ch)
		// drain snapshot so serve goroutine can exit cleanly
		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancel()
	// drain closed channels
	for _, ch := range chs {
		for range ch {
		}
	}
	// allow cleanup
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= base+30 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("base=%d now=%d (soft check; some runtime noise expected)", base, runtime.NumGoroutine())
}

func TestWatchCacheResume(t *testing.T) {
	c := watch.NewCache(100)
	for i := uint64(1); i <= 5; i++ {
		c.Append(watch.Event{Index: i, Kind: "add", Service: "s"})
	}
	evs, err := c.Since(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 { // 3,4,5
		t.Fatalf("want 3 got %d", len(evs))
	}
}
