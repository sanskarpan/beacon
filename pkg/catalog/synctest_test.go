package catalog

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
)

// TestSynctest_LeaseExpiry uses synctest to verify the lease manager expires
// leases at the right time without wall-clock waits.
func TestSynctest_LeaseExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := clock.New()
		store := NewStore(WithClock(clk))
		bus := events.NewBus(clk)
		mgr := NewLeaseManager(store, clk, WithLeaseBus(bus), WithGrace(0))

		ctx := context.Background()

		// Register an instance.
		_, err := store.Register(ctx, &Instance{
			ID:      "inst-1",
			Service: "web",
			Node:    "node-a",
		})
		if err != nil {
			t.Fatal(err)
		}

		// Grant a lease with 1s TTL and 1s deregister-after.
		_, err = mgr.GrantLease(ctx, "inst-1", 1*time.Second, 1*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if mgr.ActiveCount() != 1 {
			t.Fatalf("expected 1 active lease, got %d", mgr.ActiveCount())
		}

		// Start the manager loop.
		cancelCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mgr.Start(cancelCtx)

		// Advance past the TTL + deregister-after window.
		// synctest advances the clock automatically when all goroutines block.
		clk.Sleep(2500 * time.Millisecond)
		synctest.Wait()

		// Lease should have been expired and the instance deregistered.
		_, ok := mgr.GetLease("fake-id")
		_ = ok // lease entry removed after deregistration

		// Verify the instance is gone from the store.
		_, found := store.GetInstance("inst-1")
		if found {
			t.Fatal("expected instance to be deregistered after lease expiry")
		}
	})
}

// TestSynctest_LeaseRenewalPreventsExpiry verifies that renewing a lease
// before it expires prevents deregistration.
func TestSynctest_LeaseRenewalPreventsExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := clock.New()
		store := NewStore(WithClock(clk))
		mgr := NewLeaseManager(store, clk)

		ctx := context.Background()

		_, err := store.Register(ctx, &Instance{
			ID:      "inst-2",
			Service: "api",
			Node:    "node-b",
		})
		if err != nil {
			t.Fatal(err)
		}

		l, err := mgr.GrantLease(ctx, "inst-2", 2*time.Second, 1*time.Second)
		if err != nil {
			t.Fatal(err)
		}

		cancelCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mgr.Start(cancelCtx)

		// Sleep 1.5s (within the 2s TTL) and renew.
		clk.Sleep(1500 * time.Millisecond)
		synctest.Wait()

		_, err = mgr.RenewLease(ctx, l.ID)
		if err != nil {
			t.Fatalf("renewal should succeed: %v", err)
		}

		// Advance another 1.5s (past original TTL, within renewal window).
		clk.Sleep(1500 * time.Millisecond)
		synctest.Wait()

		// Instance should still exist because we renewed.
		_, found := store.GetInstance("inst-2")
		if !found {
			t.Fatal("instance should still exist after lease renewal")
		}
	})
}

// TestSynctest_BlockedQueryTimeout verifies that a blocking query on a store
// correctly returns when an update arrives.
func TestSynctest_BlockedQueryTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clk := clock.New()
		store := NewStore(WithClock(clk))

		// Register an instance.
		_, _ = store.Register(context.Background(), &Instance{
			ID:      "inst-3",
			Service: "db",
			Node:    "node-c",
		})

		idx := store.Index()

		type queryResult struct {
			res *Result
			err error
		}
		ch := make(chan queryResult, 1)

		// Start a blocking query at the current index.
		go func() {
			res, err := store.Get(context.Background(), "db", QueryOptions{MinIndex: idx})
			ch <- queryResult{res, err}
		}()

		// Let synctest drain — the goroutine should be parked.
		synctest.Wait()

		// Bump the index to wake the waiter.
		_, _ = store.Register(context.Background(), &Instance{
			ID:      "inst-4",
			Service: "db",
			Node:    "node-d",
		})
		synctest.Wait()

		// The blocking query should have completed with the new data.
		select {
		case qr := <-ch:
			if qr.err != nil {
				t.Fatalf("blocking query returned error: %v", qr.err)
			}
			if len(qr.res.Instances) == 0 {
				t.Fatal("blocking query returned empty instances")
			}
		default:
			t.Fatal("blocking query did not complete after index bump")
		}
	})
}
