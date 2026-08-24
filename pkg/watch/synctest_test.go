package watch

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
)

// TestSynctest_WatcherFanOut verifies that all watchers receive a notification
// within a bounded time when using the clock-driven staggered fan-out.
func TestSynctest_WatcherFanOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := clock.New()
		store := catalog.NewStore()
		reg := NewRegistry(store, WithWatchClock(clk))

		// Register an instance so there's data.
		_, _ = store.Register(context.Background(), &catalog.Instance{
			ID: "a-1", Service: "web", Node: "n1",
		})

		// Create a cancellable context so we can clean up watchers.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Open 5 watchers.
		var channels []<-chan Event
		for i := 0; i < 5; i++ {
			ch, err := reg.Watch(ctx, "web", 0)
			if err != nil {
				t.Fatal(err)
			}
			channels = append(channels, ch)
		}

		// All should receive the initial snapshot.
		synctest.Wait()

		// Give a bit of time for the staggered fan-out to complete.
		clk.Sleep(5 * time.Millisecond)
		synctest.Wait()

		// Each watcher channel should have received at least one event (snapshot).
		for i, ch := range channels {
			select {
			case ev := <-ch:
				if ev.Kind != "snapshot" {
					t.Errorf("watcher %d: expected snapshot, got %s", i, ev.Kind)
				}
			default:
				t.Errorf("watcher %d: no event received", i)
			}
		}

		// Cancel all watchers so cleanup goroutines exit.
		cancel()
		synctest.Wait()
	})
}

// TestSynctest_CacheExpiry verifies that events age out of the ring buffer.
func TestSynctest_CacheExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := NewCache(5) // tiny buffer

		// Fill the cache.
		for i := uint64(1); i <= 10; i++ {
			cache.Append(Event{Index: i, Service: "web", Kind: "update"})
		}

		// Oldest should be 6 (first 5 evicted).
		if cache.Oldest() != 6 {
			t.Fatalf("expected oldest=6, got %d", cache.Oldest())
		}

		// Querying for index 5 should return ErrIndexCompacted.
		_, err := cache.Since(5)
		if err != ErrIndexCompacted {
			t.Fatalf("expected ErrIndexCompacted, got %v", err)
		}

		// Querying for index 7 should return events 8, 9, 10.
		evs, err := cache.Since(7)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 3 {
			t.Fatalf("expected 3 events, got %d", len(evs))
		}
	})
}
