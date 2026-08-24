package integration_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	"github.com/sanskar/beacon/pkg/sdk"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	"github.com/sanskar/beacon/pkg/watch"
)

// TestE2E_InstanceDeathClientP99 measures instance death → client address-list
// update over 100 repetitions for the gossip+stream path.
// SPEC §20 target: p99 < 3s.
func TestE2E_InstanceDeathClientP99(t *testing.T) {
	const reps = 100
	samples := make([]time.Duration, 0, reps)

	for i := 0; i < reps; i++ {
		d := measureDeathToClientOnce(t)
		samples = append(samples, d)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p50 := percentile(samples, 50)
	p99 := percentile(samples, 99)
	max := samples[len(samples)-1]
	t.Logf("instance death → client apply: p50=%s p99=%s max=%s (n=%d)", p50, p99, max, reps)
	if p99 > 3*time.Second {
		t.Fatalf("p99 %s exceeds 3s bound", p99)
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	// Nearest-rank: index = ceil(p/100 * n) - 1
	idx := (p*len(sorted) + 99) / 100
	if idx < 1 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

func measureDeathToClientOnce(t *testing.T) time.Duration {
	t.Helper()
	clk := clock.New()
	bus := events.NewBus(clk)
	gc := gossip.NewCluster(clk)

	type node struct {
		mem *gossip.MemoryMembership
		st  *gstore.Store
	}
	nodes := make([]*node, 3)
	for i := 0; i < 3; i++ {
		name := []string{"n0", "n1", "n2"}[i]
		mem := gossip.NewMemory(gc, name, "127.0.0.1", 9000+i)
		cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
		wr := watch.NewRegistry(cs, watch.WithWatchClock(clk), watch.WithWatchBus(bus))
		st := gstore.New(gstore.Config{Local: cs, Membership: mem, Bus: bus, Watch: wr})
		nodes[i] = &node{mem: mem, st: st}
	}
	_, _ = nodes[1].mem.Join([]string{"n0"})
	_, _ = nodes[2].mem.Join([]string{"n0"})

	client := sdk.New(sdk.Config{
		Registry: sdk.StoreAdapter{S: nodes[1].st},
		Clock:    clk,
		Bus:      bus,
	})

	// Second healthy instance so never-empty panic mode does not mask victim removal.
	mustReg(t, nodes[1].st, &catalog.Instance{
		ID: "healthy", Service: "payments", Node: "n1",
		Address: "10.0.0.2", Port: 8080, Health: catalog.HealthPassing,
	})
	mustReg(t, nodes[0].st, &catalog.Instance{
		ID: "victim", Service: "payments", Node: "n0",
		Address: "10.0.0.1", Port: 8080, Health: catalog.HealthPassing,
	})

	// Wait until client store has both instances and Resolve sees them.
	waitUntil(t, 2*time.Second, func() bool {
		_, okV := nodes[1].st.GetInstance("victim")
		_, okH := nodes[1].st.GetInstance("healthy")
		if !okV || !okH {
			return false
		}
		insts, err := client.Resolve(context.Background(), "payments", catalog.QueryOptions{Passing: true})
		if err != nil {
			return false
		}
		var hasV, hasH bool
		for _, in := range insts {
			if in.ID == "victim" {
				hasV = true
			}
			if in.ID == "healthy" {
				hasH = true
			}
		}
		return hasV && hasH
	})

	start := time.Now()
	// Gossip failure detection path: node fail → membership → critical on peers.
	nodes[0].mem.Fail()
	// Also push an explicit health delta (agent path after failure detection) so
	// catalog peers converge even if a single membership event is dropped.
	_, _ = nodes[0].st.UpdateHealth(context.Background(), "victim", catalog.HealthCritical)

	// Bound: client must observe catalog critical + address list without victim.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		in, found := nodes[1].st.GetInstance("victim")
		catalogCritical := found && in.Health == catalog.HealthCritical
		if !catalogCritical {
			time.Sleep(200 * time.Microsecond)
			continue
		}
		// Prefer live catalog view for the address-list check (same data the
		// resolver/watch path consumes), then confirm SDK Resolve agrees.
		live := nodes[1].st.GetNow("payments", catalog.QueryOptions{Passing: true})
		liveHasVictim := false
		liveHasHealthy := false
		for _, x := range live.Instances {
			if x.ID == "victim" {
				liveHasVictim = true
			}
			if x.ID == "healthy" {
				liveHasHealthy = true
			}
		}
		if liveHasVictim || !liveHasHealthy {
			time.Sleep(200 * time.Microsecond)
			continue
		}
		insts, err := client.Resolve(context.Background(), "payments", catalog.QueryOptions{Passing: true})
		if err != nil {
			time.Sleep(200 * time.Microsecond)
			continue
		}
		hasVictimPassing := false
		hasHealthy := false
		for _, x := range insts {
			if x.ID == "victim" && x.Health == catalog.HealthPassing {
				hasVictimPassing = true
			}
			if x.ID == "healthy" {
				hasHealthy = true
			}
		}
		if !hasVictimPassing && hasHealthy {
			return time.Since(start)
		}
		time.Sleep(200 * time.Microsecond)
	}
	return time.Since(start)
}

func mustReg(t *testing.T, st *gstore.Store, inst *catalog.Instance) {
	t.Helper()
	if _, err := st.Register(context.Background(), inst); err != nil {
		t.Fatal(err)
	}
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(500 * time.Microsecond)
	}
	t.Fatal("condition not met in time")
}
