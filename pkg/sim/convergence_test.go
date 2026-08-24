package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	"github.com/sanskar/beacon/pkg/trace"
)

// TestConvergence100NodesUnder2s is the TODO-041 headline: a registration on
// one node reaches all 100 nodes in under 2s of virtual time — with a real
// transport model (50ms one-way latency, bounded fanout 3) on the fabric, not
// the instant full-mesh fast path.
func TestConvergence100NodesUnder2s(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	bus := events.NewBus(clk)
	cluster := gossip.NewCluster(clk)
	cluster.SetNetwork(gossip.NetworkConfig{Latency: 50 * time.Millisecond, Fanout: 3})

	const nodes = 100
	members := make([]*gossip.MemoryMembership, nodes)
	stores := make([]*gstore.Store, nodes)
	for i := 0; i < nodes; i++ {
		name := fmt.Sprintf("n%d", i)
		members[i] = gossip.NewMemory(cluster, name, "127.0.0.1", 8000+i)
		cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
		stores[i] = gstore.New(gstore.Config{Local: cs, Membership: members[i], Bus: bus})
	}
	for i := 1; i < nodes; i++ {
		if _, err := members[i].Join([]string{"n0"}); err != nil {
			t.Fatalf("join: %v", err)
		}
	}

	tid := trace.NewIDAt(clk.Now())
	inst := &catalog.Instance{
		ID: "pay-1", Service: "payments", Node: "n0",
		Address: "10.0.0.1", Port: 8080, Health: catalog.HealthPassing, TraceID: tid,
	}
	start := clk.Now()
	if _, err := stores[0].Register(events.ContextWithTrace(context.Background(), tid), inst); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Advance in small steps until every node has the instance, bounded by 2s.
	converged := 0
	for converged < nodes && clk.Now().Sub(start) < 2*time.Second {
		clk.Advance(10 * time.Millisecond)
		converged = 0
		for _, st := range stores {
			if _, ok := st.GetInstance("pay-1"); ok {
				converged++
			}
		}
	}
	elapsed := clk.Now().Sub(start)
	if converged != nodes {
		t.Fatalf("100-node gossip converged only %d/%d after %s", converged, nodes, elapsed)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("100-node convergence took %s, want < 2s", elapsed)
	}
	t.Logf("100 nodes converged in %s (50ms latency, fanout 3, 0 loss)", elapsed)
}
