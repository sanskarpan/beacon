package gossip_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
)

func TestGossipRegisterVisibleOnAllNodes(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	bus := events.NewBus(clk)
	cluster := gossip.NewCluster(clk)
	const n = 10
	stores := make([]*gstore.Store, n)
	for i := 0; i < n; i++ {
		name := "node" + string(rune('A'+i))
		m := gossip.NewMemory(cluster, name, "127.0.0.1", 1000+i)
		cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
		stores[i] = gstore.New(gstore.Config{Local: cs, Membership: m, Bus: bus})
		if i > 0 {
			_, _ = m.Join([]string{"nodeA"})
		}
	}
	_, err := stores[0].Register(context.Background(), &catalog.Instance{
		ID: "i1", Service: "pay", Node: "nodeA", Address: "10.0.0.1", Port: 8080,
		Health: catalog.HealthPassing,
	})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	for i, st := range stores {
		if _, ok := st.GetInstance("i1"); !ok {
			t.Fatalf("node %d missing instance", i)
		}
	}
}

func TestNodeFailureMarksCritical(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	bus := events.NewBus(clk)
	cluster := gossip.NewCluster(clk)
	m0 := gossip.NewMemory(cluster, "n0", "127.0.0.1", 1)
	m1 := gossip.NewMemory(cluster, "n1", "127.0.0.1", 2)
	_, _ = m1.Join([]string{"n0"})
	cs0 := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	cs1 := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	s0 := gstore.New(gstore.Config{Local: cs0, Membership: m0, Bus: bus})
	s1 := gstore.New(gstore.Config{Local: cs1, Membership: m1, Bus: bus})

	_, _ = s0.Register(context.Background(), &catalog.Instance{
		ID: "x", Service: "s", Node: "n0", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	// ensure s1 has it
	if _, ok := s1.GetInstance("x"); !ok {
		t.Fatal("not propagated")
	}
	// Ensure watchMembership goroutines are subscribed before failing.
	time.Sleep(50 * time.Millisecond)
	m0.Fail()
	// allow membership event processing
	deadline := time.Now().Add(2 * time.Second)
	var inst *catalog.Instance
	var ok bool
	for time.Now().Before(deadline) {
		inst, ok = s1.GetInstance("x")
		if ok && inst.Health == catalog.HealthCritical {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok {
		t.Fatal("missing")
	}
	if inst.Health != catalog.HealthCritical {
		t.Fatalf("want critical after node fail, got %s", inst.Health)
	}
}

func TestIncarnationConflict(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	cs := catalog.NewStore(catalog.WithClock(clk))
	cluster := gossip.NewCluster(clk)
	m := gossip.NewMemory(cluster, "n", "127.0.0.1", 1)
	s := gstore.New(gstore.Config{Local: cs, Membership: m})

	ok := s.ApplyDelta(gstore.Delta{
		Type: gossip.DeltaRegister,
		Instance: &catalog.Instance{
			ID: "i", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1,
			Health: catalog.HealthPassing, Incarnation: 2,
		},
		Incarnation: 2,
	})
	if !ok {
		t.Fatal("should apply")
	}
	// stale
	ok = s.ApplyDelta(gstore.Delta{
		Type: gossip.DeltaRegister,
		Instance: &catalog.Instance{
			ID: "i", Service: "s", Node: "n", Address: "2.2.2.2", Port: 1,
			Health: catalog.HealthPassing, Incarnation: 1,
		},
		Incarnation: 1,
	})
	if ok {
		t.Fatal("stale should be rejected")
	}
	inst, _ := s.GetInstance("i")
	if inst.Address != "1.1.1.1" {
		t.Fatal("stale overwrote")
	}
}

func TestAntiEntropyRecoversMissedDeltaOverMembershipTransport(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	cluster := gossip.NewCluster(clk)
	cluster.SetNetwork(gossip.NetworkConfig{Fanout: 0})
	m0 := gossip.NewMemory(cluster, "n0", "127.0.0.1", 1)
	m1 := gossip.NewMemory(cluster, "n1", "127.0.0.1", 2)
	s0 := gstore.New(gstore.Config{Local: catalog.NewStore(catalog.WithClock(clk)), Membership: m0})
	s1 := gstore.New(gstore.Config{Local: catalog.NewStore(catalog.WithClock(clk)), Membership: m1})

	cluster.Partition([]string{"n0"}, []string{"n1"})
	for i := 0; i < 2; i++ {
		if _, err := s0.Register(context.Background(), &catalog.Instance{
			ID: "missed-" + string(rune('0'+i)), Service: "svc", Node: "n0",
			Address: "10.0.0.1", Port: 8000 + i, Health: catalog.HealthPassing,
		}); err != nil {
			t.Fatal(err)
		}
	}
	cluster.Heal()
	if _, err := s0.Register(context.Background(), &catalog.Instance{
		ID: "observed", Service: "svc", Node: "n0", Address: "10.0.0.1",
		Port: 8002, Health: catalog.HealthPassing,
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"missed-0", "missed-1", "observed"} {
		if _, ok := s1.GetInstance(id); !ok {
			t.Fatalf("anti-entropy did not recover %s", id)
		}
	}
	if s1.NeedsFullSync() {
		t.Fatal("successful anti-entropy left a pending sync")
	}
}

func TestAntiEntropyChunksOversizedDelta(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	cluster := gossip.NewCluster(clk)
	m0 := gossip.NewMemory(cluster, "n0", "127.0.0.1", 1)
	m1 := gossip.NewMemory(cluster, "n1", "127.0.0.1", 2)
	s0 := gstore.New(gstore.Config{Local: catalog.NewStore(catalog.WithClock(clk)), Membership: m0})
	s1 := gstore.New(gstore.Config{Local: catalog.NewStore(catalog.WithClock(clk)), Membership: m1})
	meta := map[string]string{"large": string(make([]byte, 1200))}
	if _, err := s0.Register(context.Background(), &catalog.Instance{
		ID: "oversized", Service: "svc", Node: "n0", Address: "10.0.0.1",
		Port: 9000, Health: catalog.HealthPassing, Meta: meta,
	}); err != nil {
		t.Fatal(err)
	}
	inst, ok := s1.GetInstance("oversized")
	if !ok || inst.Meta["large"] != meta["large"] {
		t.Fatal("oversized delta was not recovered through chunked anti-entropy")
	}
	if s0.NeedsFullSync() || s1.NeedsFullSync() {
		t.Fatal("successful oversized-delta sync left a pending flag")
	}
}
