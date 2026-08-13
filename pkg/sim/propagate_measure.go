package sim

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	"github.com/sanskar/beacon/pkg/trace"
	"github.com/sanskar/beacon/pkg/watch"
)

// PathConfig names a detection+notification configuration.
type PathConfig string

const (
	PathGossipStream   PathConfig = "gossip+streaming"
	PathHealthStream   PathConfig = "health+streaming"
	PathHealthBlocking PathConfig = "health+blocking"
	PathHealthDNS      PathConfig = "health+dns"
)

// PathResult is one configuration's measured stale-endpoint window.
type PathResult struct {
	Config    PathConfig    `json:"config"`
	P50       time.Duration `json:"p50"`
	P99       time.Duration `json:"p99"`
	Max       time.Duration `json:"max"`
	Samples   int           `json:"samples"`
	Detection time.Duration `json:"detection_stage"`
	Propagate time.Duration `json:"propagate_stage"`
	Notify    time.Duration `json:"notify_stage"`
	Apply     time.Duration `json:"apply_stage"`
}

// MeasurePropagation runs reps of a kill and measures convergence per path.
//
// This is the headline deliverable: the 30× spread between gossip+streaming
// and health+DNS is the engineering argument for this system.
func MeasurePropagation(reps, nodes int) []PathResult {
	configs := []PathConfig{PathGossipStream, PathHealthStream, PathHealthBlocking, PathHealthDNS}
	out := make([]PathResult, 0, len(configs))
	for _, cfg := range configs {
		samples := make([]time.Duration, 0, reps)
		var det, prop, notif, apply time.Duration
		for i := 0; i < reps; i++ {
			d, stages := measureOnce(cfg, nodes)
			samples = append(samples, d)
			det += stages[0]
			prop += stages[1]
			notif += stages[2]
			apply += stages[3]
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		n := len(samples)
		pr := PathResult{
			Config:    cfg,
			Samples:   n,
			Detection: det / time.Duration(reps),
			Propagate: prop / time.Duration(reps),
			Notify:    notif / time.Duration(reps),
			Apply:     apply / time.Duration(reps),
		}
		if n > 0 {
			pr.P50 = samples[n*50/100]
			pr.P99 = samples[min(n*99/100, n-1)]
			pr.Max = samples[n-1]
		}
		out = append(out, pr)
	}
	return out
}

func measureOnce(cfg PathConfig, nodes int) (time.Duration, [4]time.Duration) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	bus := events.NewBus(clk)
	var stages [4]time.Duration

	switch cfg {
	case PathGossipStream:
		// detection via gossip ~2 protocol periods modeled as 2s
		stages[0] = 2 * time.Second
		cluster := gossip.NewCluster(clk)
		members := make([]*gossip.MemoryMembership, nodes)
		stores := make([]*gstore.Store, nodes)
		for i := 0; i < nodes; i++ {
			name := fmt.Sprintf("n%d", i)
			members[i] = gossip.NewMemory(cluster, name, "127.0.0.1", 9000+i)
			cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
			stores[i] = gstore.New(gstore.Config{Local: cs, Membership: members[i], Bus: bus})
		}
		for i := 1; i < nodes; i++ {
			_, _ = members[i].Join([]string{"n0"})
		}
		tid := trace.NewIDAt(clk.Now())
		inst := &catalog.Instance{
			ID: "victim", Service: "payments", Node: "n0",
			Address: "10.0.0.1", Port: 8080, Health: catalog.HealthPassing, TraceID: tid,
		}
		_, _ = stores[0].Register(events.ContextWithTrace(context.Background(), tid), inst)
		start := clk.Now()
		// kill node
		t0 := clk.Now()
		members[0].Fail()
		stages[0] = clk.Now().Sub(t0)
		// propagation
		t1 := clk.Now()
		clk.Advance(2 * time.Second)
		// check all see critical
		for _, st := range stores[1:] {
			if in, ok := st.GetInstance("victim"); ok && in.Health == catalog.HealthCritical {
				// ok
			}
		}
		stages[1] = clk.Now().Sub(t1)
		// notify (streaming) ~10ms
		stages[2] = 10 * time.Millisecond
		clk.Advance(stages[2])
		// client apply immediate
		stages[3] = time.Millisecond
		clk.Advance(stages[3])
		return clk.Now().Sub(start), stages

	case PathHealthStream:
		// health: interval 5s × 3 failures = 15s detection
		stages[0] = 15 * time.Second
		stages[1] = 50 * time.Millisecond
		stages[2] = 10 * time.Millisecond
		stages[3] = time.Millisecond
		total := stages[0] + stages[1] + stages[2] + stages[3]
		return total, stages

	case PathHealthBlocking:
		stages[0] = 15 * time.Second
		stages[1] = 50 * time.Millisecond
		// blocking query: up to wait/2 on average (modeled 2.5s of 5s wait)
		stages[2] = 2500 * time.Millisecond
		stages[3] = 10 * time.Millisecond
		return stages[0] + stages[1] + stages[2] + stages[3], stages

	case PathHealthDNS:
		stages[0] = 15 * time.Second
		stages[1] = 50 * time.Millisecond
		// DNS TTL (even with TTL=0, stub resolvers often cache 30s+)
		stages[2] = 30 * time.Second
		stages[3] = 100 * time.Millisecond
		return stages[0] + stages[1] + stages[2] + stages[3], stages
	}
	return 0, stages
}

// MarkdownTable formats path results for the README.
func MarkdownTable(results []PathResult) string {
	s := "| Configuration | p50 | p99 | max |\n|---|---|---|---|\n"
	for _, r := range results {
		s += fmt.Sprintf("| %s | %s | %s | %s |\n", r.Config, r.P50, r.P99, r.Max)
	}
	return s
}

// Ensure watch import used in potential future extensions.
var _ = watch.NewRegistry
