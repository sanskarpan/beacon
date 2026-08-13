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
	select {
	case ev := <-ch:
		if ev.Kind != "snapshot" {
			t.Fatalf("want snapshot got %s", ev.Kind)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}
