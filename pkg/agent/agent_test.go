package agent_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/agent"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
)

func TestAgentRegisterSync(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	bus := events.NewBus(clk)
	store := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	client := &agent.LocalClient{Store: store, Node: "n1"}
	a := agent.New(agent.Config{
		NodeName: "n1", Client: client, Store: store, Bus: bus, Clock: clk,
		ClusterSize: func() int { return 1 },
	})
	err := a.Register(context.Background(), &catalog.Instance{
		ID: "s1", Service: "web", Address: "127.0.0.1", Port: 8080, Health: catalog.HealthPassing,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.GetInstance("s1"); !ok {
		t.Fatal("catalog missing instance")
	}
}

func TestAgentPutsBackDeleted(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	store := catalog.NewStore(catalog.WithClock(clk))
	client := &agent.LocalClient{Store: store, Node: "n1"}
	a := agent.New(agent.Config{
		NodeName: "n1", Client: client, Store: store, Clock: clk,
		ClusterSize: func() int { return 1 },
	})
	_ = a.Register(context.Background(), &catalog.Instance{
		ID: "s1", Service: "web", Address: "127.0.0.1", Port: 8080, Health: catalog.HealthPassing,
	})
	// operator deletes from catalog
	_, _ = store.Deregister(context.Background(), "s1")
	if _, ok := store.GetInstance("s1"); ok {
		t.Fatal("should be gone")
	}
	// agent sync puts it back
	if err := a.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.GetInstance("s1"); !ok {
		t.Fatal("agent should repopulate")
	}
}

func TestCatalogRepopulationAfterWipe(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	store := catalog.NewStore(catalog.WithClock(clk))
	client := &agent.LocalClient{Store: store, Node: "n1"}
	a := agent.New(agent.Config{
		NodeName: "n1", Client: client, Store: store, Clock: clk,
		ClusterSize: func() int { return 1 },
	})
	for i := 0; i < 5; i++ {
		_ = a.Register(context.Background(), &catalog.Instance{
			ID: string(rune('a'+i)), Service: "svc", Address: "127.0.0.1", Port: 8000 + i,
			Health: catalog.HealthPassing,
		})
	}
	// wipe server state
	_ = store.Restore(&catalog.Snapshot{
		Index: 0, Services: map[string]*catalog.Service{}, Instances: map[string]*catalog.Instance{},
	})
	if err := a.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.InstancesOnNode("n1")) != 5 {
		t.Fatalf("want 5 after repopulate, got %d", len(store.InstancesOnNode("n1")))
	}
}

func TestSyncIntervalScaling(t *testing.T) {
	if agent.SyncInterval(10) != time.Minute {
		t.Fatal()
	}
	if agent.SyncInterval(10000) != 30*time.Minute {
		t.Fatal()
	}
}
