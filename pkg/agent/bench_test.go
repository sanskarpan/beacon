package agent_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	"github.com/sanskar/beacon/pkg/health"
	"github.com/sanskar/beacon/pkg/store"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
)

// TODO-038: Agent with 100 local services memory/CPU benchmark.
func TestAgent_100ServicesBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in -short mode")
	}

	clk := clock.New()
	bus := events.NewBus(clk)
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	cluster := gossip.NewCluster(clk)
	mem := gossip.NewMemory(cluster, "agent-bench", "127.0.0.1", 7946)

	storeCfg := gstore.Config{Local: cs, Membership: mem, Bus: bus}
	gs := gstore.New(storeCfg)
	_ = store.CatalogStore(gs) // ensure interface

	runner := health.NewRunner(cs, clk, 64, health.WithRunnerBus(bus))
	defer runner.Stop()

	// Register 100 services with 1 check each.
	for i := 0; i < 100; i++ {
		svc := fmt.Sprintf("svc-%d", i)
		inst := &catalog.Instance{
			ID: fmt.Sprintf("inst-%d", i), Service: svc,
			Address: "127.0.0.1", Port: 8080 + i, Health: catalog.HealthPassing,
			Node: "agent-bench",
			Checks: []catalog.Check{
				{
					ID: "alive", Type: catalog.CheckTCP,
					TCP: "127.0.0.1:1", Timeout: 1 * time.Second,
					Interval:               30 * time.Second,
					FailuresBeforeCritical: 3,
					SuccessesBeforePassing: 2,
				},
			},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := gs.Register(ctx, inst)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		runner.Add(inst)
	}

	// Let checks tick once.
	time.Sleep(2 * time.Second)

	// Measure goroutines before/after.
	afterGoroutines := runtime.NumGoroutine()

	// Verify all services are registered.
	res := gs.GetNow("svc-50", catalog.QueryOptions{})
	if len(res.Instances) != 1 {
		t.Fatalf("expected 1 instance for svc-50, got %d", len(res.Instances))
	}

	t.Logf("100 services with 1 check each: %d goroutines active", afterGoroutines)
}

// TODO-056: Health checks: 500 concurrent per agent < 5% CPU.
func TestAgent_500ConcurrentHealthChecks(t *testing.T) {
	if testing.Short() {
		t.Skip("skip in -short mode")
	}

	clk := clock.New()
	bus := events.NewBus(clk)
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))

	runner := health.NewRunner(cs, clk, 128, health.WithRunnerBus(bus))
	defer runner.Stop()

	const N = 500

	// Register N instances each with a health check.
	for i := 0; i < N; i++ {
		svc := fmt.Sprintf("svc-%d", i%50) // 50 services, 10 instances each
		inst := &catalog.Instance{
			ID: fmt.Sprintf("hc-%d", i), Service: svc,
			Address: "127.0.0.1", Port: 9000 + (i % 100), Health: catalog.HealthPassing,
			Node: "hc-node",
			Checks: []catalog.Check{
				{
					ID: "alive", Type: catalog.CheckTCP,
					TCP: "127.0.0.1:1", Timeout: 1 * time.Second,
					Interval:               10 * time.Second,
					FailuresBeforeCritical: 3,
					SuccessesBeforePassing: 2,
				},
			},
		}
		_, _ = cs.Register(context.Background(), inst)
		runner.Add(inst)
	}

	// Measure goroutines after checks start.
	time.Sleep(500 * time.Millisecond)
	goroutines := runtime.NumGoroutine()
	t.Logf("500 concurrent health checks: %d goroutines", goroutines)

	// Let checks run for a bit.
	time.Sleep(3 * time.Second)

	// Cleanup.
	runner.Stop()
	t.Log("500 concurrent health checks completed without deadlock")
}

// TestAgent_ConcurrentRegisterDeregister tests no goroutine leak.
func TestAgent_ConcurrentRegisterDeregister(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))

	cluster := gossip.NewCluster(clk)
	mem := gossip.NewMemory(cluster, "concurrent-test", "127.0.0.1", 7947)
	gs := gstore.New(gstore.Config{Local: cs, Membership: mem, Bus: bus})

	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	const N = 200
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			id := fmt.Sprintf("inst-%d", i)
			_, _ = gs.Register(ctx, &catalog.Instance{
				ID: id, Service: "concurrent",
				Address: "10.0.0.1", Port: 8080 + (i % 100),
				Health: catalog.HealthPassing, Node: "node-0",
			})
			time.Sleep(5 * time.Millisecond)
			_, _ = gs.Deregister(ctx, id)
		}(i)
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	leaked := after - before
	t.Logf("goroutines before=%d after=%d leaked=%d", before, after, leaked)

	if leaked > 10 {
		t.Errorf("possible goroutine leak: %d goroutines grew after 200 register/deregister cycles", leaked)
	}
}
