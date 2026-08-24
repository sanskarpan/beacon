package consensus_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store/raft/consensus"
)

// TODO-060: Property 12 — CP no conflicting linearizable results under concurrency.
//
// Concurrent writers on the leader must never produce two instances with the
// same ID. Linearizable reads (consistent=true) must return the same index
// immediately after each write, never a stale subset.
func TestProperty_ConcurrentCPWritesNoConflicts(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	cluster, err := consensus.NewCluster([]string{"c1", "c2", "c3"}, clk, bus)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	leader := cluster.Leader(8 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	st := consensus.NewStore(leader)

	// Concurrent writes: each goroutine registers a unique instance.
	const writers = 20
	const perWriter = 5
	var wg sync.WaitGroup
	var writeCount atomic.Int64
	var writeErr atomic.Int64

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				id := fmt.Sprintf("svc-%d-%d", writer, i)
				_, err := st.Register(ctx, &catalog.Instance{
					ID:      id,
					Service: "concurrent",
					Address: "10.0.0.1",
					Port:    8000 + writer*100 + i,
					Health:  catalog.HealthPassing,
					Node:    "node-0",
				})
				cancel()
				if err != nil {
					writeErr.Add(1)
				} else {
					writeCount.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	t.Logf("writes: %d ok, %d errors", writeCount.Load(), writeErr.Load())

	if writeCount.Load() != writers*perWriter {
		t.Fatalf("expected %d successful writes, got %d", writers*perWriter, writeCount.Load())
	}

	// Verify every ID is unique in the catalog (no duplicates).
	snap := leader.FSM.Store().Snapshot()
	ids := make(map[string]bool, len(snap.Instances))
	for id := range snap.Instances {
		if ids[id] {
			t.Fatalf("duplicate instance ID in CP catalog: %s", id)
		}
		ids[id] = true
	}
	if len(ids) != writers*perWriter {
		t.Fatalf("expected %d unique instances, got %d", writers*perWriter, len(ids))
	}

	// Linearizable read (consistent=true on leader) must see all writes.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := st.Get(ctx, "concurrent", catalog.QueryOptions{Consistent: true})
	if err != nil {
		t.Fatalf("consistent read: %v", err)
	}
	if len(res.Instances) != writers*perWriter {
		t.Fatalf("consistent read returned %d instances, expected %d", len(res.Instances), writers*perWriter)
	}
}

// TODO-061: Property 2 — deregistration completeness after multi-node convergence.
//
// After registering an instance and confirming it has replicated to all nodes,
// deregistering it must result in zero nodes returning that instance.
func TestProperty_DeregistrationCompleteness(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	cluster, err := consensus.NewCluster([]string{"d1", "d2", "d3"}, clk, bus)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	leader := cluster.Leader(8 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	st := consensus.NewStore(leader)
	ctx := context.Background()

	// Register and wait for replication.
	inst := &catalog.Instance{
		ID: "ephemeral-1", Service: "delete-me", Address: "10.0.0.99", Port: 9999,
		Health: catalog.HealthPassing, Node: "test-node",
	}
	if _, err := st.Register(ctx, inst); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Wait for all nodes to have the instance.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range []string{"d1", "d2", "d3"} {
			n := cluster.Node(id)
			if _, found := n.FSM.Store().GetInstance("ephemeral-1"); !found {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Deregister.
	if _, err := st.Deregister(ctx, "ephemeral-1"); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	// Wait for deregistration to propagate to all nodes.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		none := true
		for _, id := range []string{"d1", "d2", "d3"} {
			n := cluster.Node(id)
			if _, found := n.FSM.Store().GetInstance("ephemeral-1"); found {
				none = false
				break
			}
		}
		if none {
			return // success
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("deregistered instance still visible on at least one node after convergence")
}

// TODO-062: Property 1 — registration durability across servers within bound.
//
// After registering on the leader and confirming replication, all nodes must
// retain the instance even after a short time passes and no further writes
// occur (i.e., Raft log entries are durable, not ephemeral).
func TestProperty_RegistrationDurability(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	cluster, err := consensus.NewCluster([]string{"r1", "r2", "r3"}, clk, bus)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	leader := cluster.Leader(8 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	st := consensus.NewStore(leader)
	ctx := context.Background()

	// Register several instances.
	for i := 0; i < 10; i++ {
		_, err := st.Register(ctx, &catalog.Instance{
			ID: fmt.Sprintf("dur-%d", i), Service: "durable",
			Address: "10.0.0.1", Port: 10000 + i,
			Health: catalog.HealthPassing, Node: "node-0",
		})
		if err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
	}

	// Wait for replication.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range []string{"r1", "r2", "r3"} {
			n := cluster.Node(id)
			res := n.FSM.Store().GetNow("durable", catalog.QueryOptions{})
			if len(res.Instances) != 10 {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Wait a bounded time — instances must survive.
	time.Sleep(500 * time.Millisecond)

	// Verify durability on all nodes.
	for _, id := range []string{"r1", "r2", "r3"} {
		n := cluster.Node(id)
		res := n.FSM.Store().GetNow("durable", catalog.QueryOptions{})
		if len(res.Instances) != 10 {
			t.Fatalf("node %s: expected 10 durable instances, got %d", id, len(res.Instances))
		}
	}
}
