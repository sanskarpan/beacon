package catalog_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
)

func TestRegisterGetDeregister(t *testing.T) {
	s := catalog.NewStore()
	idx, err := s.Register(context.Background(), &catalog.Instance{
		ID: "a1", Service: "api", Node: "n1", Address: "10.0.0.1", Port: 8080,
		Health: catalog.HealthPassing, Weight: 1,
	})
	if err != nil || idx == 0 {
		t.Fatalf("register: idx=%d err=%v", idx, err)
	}
	res := s.GetNow("api", catalog.QueryOptions{})
	if len(res.Instances) != 1 {
		t.Fatalf("want 1 instance, got %d", len(res.Instances))
	}
	_, err = s.Deregister(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	res = s.GetNow("api", catalog.QueryOptions{})
	if len(res.Instances) != 0 {
		t.Fatalf("want 0 after deregister")
	}
}

func TestIndexMonotonic(t *testing.T) {
	s := catalog.NewStore()
	var prev uint64
	for i := 0; i < 1000; i++ {
		idx, err := s.Register(context.Background(), &catalog.Instance{
			ID: fmt.Sprintf("i%d", i), Service: "s", Node: "n",
			Address: "1.1.1.1", Port: i + 1, Health: catalog.HealthPassing,
		})
		if err != nil {
			t.Fatal(err)
		}
		if idx < prev {
			t.Fatalf("index decreased: %d < %d", idx, prev)
		}
		prev = idx
	}
}

func TestUpdateHealthNoBumpWhenUnchanged(t *testing.T) {
	s := catalog.NewStore()
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "x", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1,
		Health: catalog.HealthPassing,
	})
	before := s.Index()
	idx, err := s.UpdateHealth(context.Background(), "x", catalog.HealthPassing)
	if err != nil {
		t.Fatal(err)
	}
	if idx != before {
		t.Fatalf("index bumped on unchanged health: %d → %d", before, idx)
	}
	idx, err = s.UpdateHealth(context.Background(), "x", catalog.HealthCritical)
	if err != nil {
		t.Fatal(err)
	}
	if idx <= before {
		t.Fatalf("index should bump on change")
	}
}

func TestHealthAggregation(t *testing.T) {
	if catalog.Aggregate([]catalog.HealthStatus{
		catalog.HealthPassing, catalog.HealthCritical, catalog.HealthWarning,
	}) != catalog.HealthCritical {
		t.Fatal("worst should be critical")
	}
}

func TestFilters(t *testing.T) {
	s := catalog.NewStore()
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "a", Service: "pay", Node: "n", Address: "1.1.1.1", Port: 1,
		Health: catalog.HealthPassing, Tags: []string{"v2"}, Meta: map[string]string{"version": "v2"},
		Locality: catalog.Locality{Zone: "z1"},
	})
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "b", Service: "pay", Node: "n", Address: "1.1.1.2", Port: 1,
		Health: catalog.HealthCritical, Tags: []string{"v1"}, Meta: map[string]string{"version": "v1"},
	})
	res := s.GetNow("pay", catalog.QueryOptions{Passing: true})
	if len(res.Instances) != 1 {
		t.Fatalf("passing: want 1 got %d", len(res.Instances))
	}
	res = s.GetNow("pay", catalog.QueryOptions{Tags: []string{"v2"}})
	if len(res.Instances) != 1 || res.Instances[0].ID != "a" {
		t.Fatal("tag filter")
	}
	res = s.GetNow("pay", catalog.QueryOptions{Filter: `Meta.version == "v2"`})
	if len(res.Instances) != 1 {
		t.Fatal("meta filter")
	}
}

func TestBatchedBumps(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	bus := events.NewBus(clk)
	s := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus), catalog.WithBatchWindow(50*time.Millisecond))

	// open a waiter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wakes := 0
	go func() {
		idx := uint64(0)
		for {
			res, err := s.Get(ctx, "batch", catalog.QueryOptions{MinIndex: idx})
			if err != nil {
				return
			}
			if res.Index > idx {
				wakes++
				idx = res.Index
			}
		}
	}()

	for i := 0; i < 100; i++ {
		_, _ = s.Register(context.Background(), &catalog.Instance{
			ID: fmt.Sprintf("b%d", i), Service: "batch", Node: "n",
			Address: "1.1.1.1", Port: i + 1, Health: catalog.HealthPassing,
		})
	}
	// before flush, individual indexes still bump for CreateIndex
	// flush window
	clk.Advance(100 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	// With batching, watcher should not wake 100 times for intermediate states
	// (may still see some). Key property: index is monotonic and final state complete.
	res := s.GetNow("batch", catalog.QueryOptions{})
	if len(res.Instances) != 100 {
		t.Fatalf("want 100 instances, got %d", len(res.Instances))
	}
	t.Logf("wakes=%d index=%d", wakes, s.Index())
}

func TestConcurrentRegister(t *testing.T) {
	s := catalog.NewStore()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := fmt.Sprintf("g%d-i%d", g, i)
				_, _ = s.Register(context.Background(), &catalog.Instance{
					ID: id, Service: "c", Node: "n", Address: "1.1.1.1", Port: 1,
					Health: catalog.HealthPassing,
				})
				if i%2 == 0 {
					_, _ = s.Deregister(context.Background(), id)
				}
				_ = s.GetNow("c", catalog.QueryOptions{})
			}
		}(g)
	}
	wg.Wait()
}

func TestBlockingQuery(t *testing.T) {
	s := catalog.NewStore()
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "blk", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	res := s.GetNow("blk", catalog.QueryOptions{})
	start := time.Now()
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = s.Register(context.Background(), &catalog.Instance{
			ID: "2", Service: "blk", Node: "n", Address: "1.1.1.2", Port: 1, Health: catalog.HealthPassing,
		})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := s.Get(ctx, "blk", catalog.QueryOptions{MinIndex: res.Index})
	if err != nil {
		t.Fatal(err)
	}
	if out.Index <= res.Index {
		t.Fatal("expected advanced index")
	}
	if time.Since(start) > time.Second {
		t.Fatal("blocked too long")
	}
}

func TestSnapshotRestore(t *testing.T) {
	s := catalog.NewStore()
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "s1", Service: "snap", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	snap := s.Snapshot()
	s2 := catalog.NewStore()
	if err := s2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if len(s2.GetNow("snap", catalog.QueryOptions{}).Instances) != 1 {
		t.Fatal("restore failed")
	}
}
