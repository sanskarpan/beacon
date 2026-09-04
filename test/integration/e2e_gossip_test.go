package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	"github.com/sanskar/beacon/pkg/trace"
	"github.com/sanskar/beacon/test/integration"
)

// E2E: register on node 0 → visible on all 10 nodes via gossip.
func TestE2E_GossipRegisterAllNodes(t *testing.T) {
	clk := clock.New()
	stacks := integration.MultiNodeAP(10, clk)
	defer integration.CloseAll(stacks)

	tid := trace.NewID()
	ctx := events.ContextWithTrace(context.Background(), tid)
	_, err := stacks[0].Store.Register(ctx, &catalog.Instance{
		ID: "i1", Service: "payments", Node: "n0",
		Address: "10.0.0.1", Port: 8080, Health: catalog.HealthPassing, TraceID: tid,
	})
	if err != nil {
		t.Fatal(err)
	}

	ok := integration.WaitFor(2*time.Second, func() bool {
		for _, s := range stacks {
			if _, found := s.Store.GetInstance("i1"); !found {
				return false
			}
		}
		return true
	})
	if !ok {
		for i, s := range stacks {
			_, found := s.Store.GetInstance("i1")
			t.Logf("node %d has instance: %v", i, found)
		}
		t.Fatal("not converged to all 10 nodes")
	}
}

// E2E: kill node → instances critical on peers within 3s.
func TestE2E_GossipNodeFailCritical(t *testing.T) {
	clk := clock.New()
	stacks := integration.MultiNodeAP(5, clk)
	defer integration.CloseAll(stacks)

	_, _ = stacks[0].Store.Register(context.Background(), &catalog.Instance{
		ID: "victim", Service: "svc", Node: "n0",
		Address: "10.0.0.1", Port: 1, Health: catalog.HealthPassing,
	})
	if !integration.WaitFor(time.Second, func() bool {
		_, ok := stacks[1].Store.GetInstance("victim")
		return ok
	}) {
		t.Fatal("not propagated")
	}

	// Fail membership node n0
	mem, ok := stacks[0].Membership.(*gossip.MemoryMembership)
	if !ok {
		t.Fatal("expected MemoryMembership")
	}
	mem.Fail()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for i := 1; i < len(stacks); i++ {
			inst, found := stacks[i].Store.GetInstance("victim")
			if !found || inst.Health != catalog.HealthCritical {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("instances not critical within 3s after node fail")
}

// E2E: deregistration is monotone — stale register at same incarnation cannot undo it.
func TestE2E_DeregisterMonotone(t *testing.T) {
	clk := clock.New()
	cs := catalog.NewStore(catalog.WithClock(clk))
	gc := gossip.NewCluster(clk)
	m := gossip.NewMemory(gc, "n", "127.0.0.1", 1)
	s := gstore.New(gstore.Config{Local: cs, Membership: m})

	_ = s.ApplyDelta(gstore.Delta{
		Type: gossip.DeltaRegister,
		Instance: &catalog.Instance{
			ID: "x", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1,
			Health: catalog.HealthPassing, Incarnation: 5,
		},
		Incarnation: 5,
	})
	_ = s.ApplyDelta(gstore.Delta{
		Type:        gossip.DeltaDeregister,
		InstanceID:  "x",
		Incarnation: 5,
	})
	if _, ok := s.GetInstance("x"); ok {
		t.Fatal("should be deregistered")
	}
	// stale register at same incarnation must not revive
	applied := s.ApplyDelta(gstore.Delta{
		Type: gossip.DeltaRegister,
		Instance: &catalog.Instance{
			ID: "x", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1,
			Health: catalog.HealthPassing, Incarnation: 5,
		},
		Incarnation: 5,
	})
	if applied {
		t.Fatal("equal-incarnation register must not undo deregister")
	}
	if _, ok := s.GetInstance("x"); ok {
		t.Fatal("tombstone violated: instance revived at same incarnation")
	}
	// Higher incarnation may re-register (node recreated) — that's correct.
	_ = s.ApplyDelta(gstore.Delta{
		Type: gossip.DeltaRegister,
		Instance: &catalog.Instance{
			ID: "x", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1,
			Health: catalog.HealthPassing, Incarnation: 6,
		},
		Incarnation: 6,
	})
	if _, ok := s.GetInstance("x"); !ok {
		t.Fatal("higher incarnation should re-register")
	}
}

// E2E: partition → diverge → heal → converge.
func TestE2E_GossipPartitionHeal(t *testing.T) {
	clk := clock.New()
	stacks := integration.MultiNodeAP(4, clk)
	defer integration.CloseAll(stacks)

	// baseline registration on n0
	_, _ = stacks[0].Store.Register(context.Background(), &catalog.Instance{
		ID: "shared", Service: "s", Node: "n0", Address: "1.1.1.1", Port: 1,
		Health: catalog.HealthPassing,
	})
	integration.WaitFor(time.Second, func() bool {
		_, ok := stacks[3].Store.GetInstance("shared")
		return ok
	})

	// partition n0,n1 | n2,n3
	stacks[0].GossipCluster.Partition([]string{"n0", "n1"}, []string{"n2", "n3"})

	// writes on each side
	_, _ = stacks[0].Store.Register(context.Background(), &catalog.Instance{
		ID: "side-a", Service: "s", Node: "n0", Address: "2.2.2.2", Port: 1,
		Health: catalog.HealthPassing,
	})
	_, _ = stacks[2].Store.Register(context.Background(), &catalog.Instance{
		ID: "side-b", Service: "s", Node: "n2", Address: "3.3.3.3", Port: 1,
		Health: catalog.HealthPassing,
	})

	// side-a should not be on side-b while partitioned
	time.Sleep(50 * time.Millisecond)
	if _, ok := stacks[2].Store.GetInstance("side-a"); ok {
		t.Fatal("partition leak: side-a visible on n2")
	}
	if _, ok := stacks[0].Store.GetInstance("side-b"); ok {
		t.Fatal("partition leak: side-b visible on n0")
	}

	// heal
	stacks[0].GossipCluster.Heal()
	// re-broadcast via full sync of catalogs
	for i := 0; i < len(stacks); i++ {
		for j := 0; j < len(stacks); j++ {
			if i == j {
				continue
			}
			if gs, ok := stacks[i].Store.(*gstore.Store); ok {
				_ = gs.FullSync(stacks[j].Store.Snapshot())
			}
		}
	}

	ok := integration.WaitFor(2*time.Second, func() bool {
		_, a := stacks[2].Store.GetInstance("side-a")
		_, b := stacks[0].Store.GetInstance("side-b")
		return a && b
	})
	if !ok {
		t.Fatal("did not converge after heal")
	}
}

// E2E: missed deltas catch up via FullSync.
func TestE2E_FullStateSync(t *testing.T) {
	clk := clock.New()
	stacks := integration.MultiNodeAP(2, clk)
	defer integration.CloseAll(stacks)

	// Isolate n1 so it misses deltas
	stacks[0].GossipCluster.Partition([]string{"n0"}, []string{"n1"})
	for i := 0; i < 50; i++ {
		_, _ = stacks[0].Store.Register(context.Background(), &catalog.Instance{
			ID:      string(rune('A'+(i%26))) + string(rune('0'+i/26)),
			Service: "bulk", Node: "n0", Address: "10.0.0.1", Port: 8000 + i,
			Health: catalog.HealthPassing,
		})
	}
	if n := len(stacks[1].Store.Snapshot().Instances); n != 0 {
		// might have nothing
		t.Logf("n1 instances during partition: %d", n)
	}

	stacks[0].GossipCluster.Heal()
	gs1 := stacks[1].Store.(*gstore.Store)
	_ = gs1.FullSync(stacks[0].Store.Snapshot())

	snap0 := stacks[0].Store.Snapshot()
	snap1 := stacks[1].Store.Snapshot()
	if len(snap1.Instances) < len(snap0.Instances) {
		t.Fatalf("full sync incomplete: n0=%d n1=%d", len(snap0.Instances), len(snap1.Instances))
	}
}
