package gossip_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/agent"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
)

// When gossip is disabled, anti-entropy alone still converges (agent push).
func TestGossipDisabledAntiEntropyFallback(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	// server catalog has no gossip layer — only agent anti-entropy
	server := catalog.NewStore(catalog.WithClock(clk))
	client := &agent.LocalClient{Store: server, Node: "agent-1"}
	a := agent.New(agent.Config{
		NodeName: "agent-1", Client: client, Store: catalog.NewStore(catalog.WithClock(clk)),
		Clock: clk, ClusterSize: func() int { return 1 },
	})
	defer a.Stop()

	_ = a.Register(context.Background(), &catalog.Instance{
		ID: "only-ae", Service: "s", Address: "127.0.0.1", Port: 9, Health: catalog.HealthPassing,
	})
	// wipe server (simulates missed gossip)
	_ = server.Restore(&catalog.Snapshot{
		Services:  map[string]*catalog.Service{},
		Instances: map[string]*catalog.Instance{},
	})
	if err := a.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := server.GetInstance("only-ae"); !ok {
		t.Fatal("anti-entropy alone must repopulate without gossip")
	}
}
