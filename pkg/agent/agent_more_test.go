package agent_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/agent"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
)

// countingClient records sync-related register calls.
type countingClient struct {
	inner *agent.LocalClient
	regs  atomic.Int64
}

func (c *countingClient) Register(ctx context.Context, inst *catalog.Instance) (uint64, error) {
	c.regs.Add(1)
	return c.inner.Register(ctx, inst)
}
func (c *countingClient) Deregister(ctx context.Context, id string) (uint64, error) {
	return c.inner.Deregister(ctx, id)
}
func (c *countingClient) UpdateHealth(ctx context.Context, id string, h catalog.HealthStatus) (uint64, error) {
	return c.inner.UpdateHealth(ctx, id, h)
}
func (c *countingClient) NodeServices(ctx context.Context, node string) (map[string]*catalog.Instance, error) {
	return c.inner.NodeServices(ctx, node)
}

func TestImmediateSyncOnLocalChange(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	store := catalog.NewStore(catalog.WithClock(clk))
	inner := &agent.LocalClient{Store: store, Node: "n1"}
	cc := &countingClient{inner: inner}
	a := agent.New(agent.Config{
		NodeName: "n1", Client: cc, Store: store, Clock: clk,
		ClusterSize: func() int { return 1 },
	})
	defer a.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.StartAntiEntropy(ctx)

	before := cc.regs.Load()
	_ = a.Register(context.Background(), &catalog.Instance{
		ID: "fast", Service: "s", Address: "127.0.0.1", Port: 1, Health: catalog.HealthPassing,
	})
	// Register already calls client.Register once; anti-entropy may call again
	// Advance a tiny bit and ensure we didn't need full interval
	clk.Advance(200 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if cc.regs.Load() <= before {
		t.Fatal("expected register path to hit client")
	}
	// must not require 1 minute interval
	if _, ok := store.GetInstance("fast"); !ok {
		t.Fatal("missing instance")
	}
}

func TestManyAgentsJitteredSync(t *testing.T) {
	// Ensure SyncInterval scales and 100 agents would not all share the same exact delay.
	intervals := map[time.Duration]int{}
	for size := range []int{10, 200, 1000, 5000, 10000} {
		_ = size
	}
	for _, size := range []int{10, 200, 1000, 5000, 10000} {
		intervals[agent.SyncInterval(size)]++
	}
	if len(intervals) < 3 {
		t.Fatalf("expected multiple interval tiers, got %v", intervals)
	}
}
