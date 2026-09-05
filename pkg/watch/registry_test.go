package watch_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/watch"
)

func TestBlockingQueryImmediateWhenAdvanced(t *testing.T) {
	s := catalog.NewStore()
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	idx := s.Index()
	res, err := watch.BlockingQuery(context.Background(), s, "s", catalog.QueryOptions{MinIndex: 0}, clock.New(), nil)
	if err != nil || res.Index != idx {
		t.Fatalf("got %+v err=%v", res, err)
	}
}

func TestBlockingQueryTimeoutReturnsState(t *testing.T) {
	s := catalog.NewStore()
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	res, err := watch.BlockingQuery(ctx, s, "s", catalog.QueryOptions{
		MinIndex: s.Index(), Wait: 50 * time.Millisecond,
	}, clock.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Instances) != 1 {
		t.Fatal("should return current state")
	}
}

func TestFutureIndexReset(t *testing.T) {
	s := catalog.NewStore()
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	res, err := watch.BlockingQuery(context.Background(), s, "s", catalog.QueryOptions{
		MinIndex: 999999, Wait: time.Second,
	}, clock.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("nil")
	}
}

func TestWatchCacheCompaction(t *testing.T) {
	c := watch.NewCache(3)
	for i := uint64(1); i <= 5; i++ {
		c.Append(watch.Event{Index: i, Kind: "add"})
	}
	_, err := c.Since(1)
	if err != watch.ErrIndexCompacted {
		t.Fatalf("want compacted, got %v", err)
	}
	evs, err := c.Since(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) == 0 {
		t.Fatal("expected events")
	}
}

func TestWatchSnapshot(t *testing.T) {
	s := catalog.NewStore()
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	r := watch.NewRegistry(s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := r.Watch(ctx, "s", 0)
	if err != nil {
		t.Fatal(err)
	}
	snapWait := time.NewTimer(time.Second)
	defer snapWait.Stop()
	select {
	case ev := <-ch:
		if ev.Kind != "snapshot" {
			t.Fatalf("want snapshot got %s", ev.Kind)
		}
	case <-snapWait.C:
		t.Fatal("timeout")
	}
}

// TestStatsConcurrentWithServe guards #86: Stats() reads watcher.lastIdx
// under RLock while serve goroutines write it. Must be race-free.
func TestStatsConcurrentWithServe(t *testing.T) {
	s := catalog.NewStore()
	r := watch.NewRegistry(s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := r.Watch(ctx, "svc", 0)
	if err != nil {
		t.Fatal(err)
	}
	// drain initial snapshot so serve proceeds to updates
	snapTimer := time.NewTimer(2 * time.Second)
	defer snapTimer.Stop()
	select {
	case <-ch:
	case <-snapTimer.C:
		t.Fatal("no snapshot")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			r.Notify("svc", watch.Event{Kind: "add", Service: "svc", Index: uint64(i + 1)})
			_ = r.Stats()
		}
	}()
	// concurrent reader drains + stats
	stall := time.NewTimer(5 * time.Second)
	defer stall.Stop()
	for {
		select {
		case <-done:
			_ = r.Stats()
			return
		case <-ch:
			_ = r.Stats()
		case <-stall.C:
			t.Fatal("stall")
		}
	}
}

func TestWatchPassingFilterAppliesToSnapshot(t *testing.T) {
	s := catalog.NewStore()
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "passing", Service: "svc", Health: catalog.HealthPassing,
	})
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "critical", Service: "svc", Health: catalog.HealthCritical,
	})
	r := watch.NewRegistry(s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := r.WatchWithOptions(ctx, "svc", watch.WatchOptions{Passing: true})
	if err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case ev := <-ch:
		if len(ev.Instances) != 1 || ev.Instances[0].ID != "passing" {
			t.Fatalf("passing snapshot: %+v", ev.Instances)
		}
	case <-timer.C:
		t.Fatal("snapshot timeout")
	}
}
