package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	"github.com/sanskar/beacon/pkg/trace"
)

// ContrastResult compares the same registration workload with gossip on vs off
// (anti-entropy / Merkle catch-up only). Exported for the console overlay (TODO-012).
type ContrastResult struct {
	GossipOnP50  time.Duration `json:"gossip_on_p50"`
	GossipOnP99  time.Duration `json:"gossip_on_p99"`
	GossipOffP50 time.Duration `json:"gossip_off_p50"`
	GossipOffP99 time.Duration `json:"gossip_off_p99"`
	SlowdownP50  float64       `json:"slowdown_p50"`
	SlowdownP99  float64       `json:"slowdown_p99"`
	Samples      int           `json:"samples"`
	Nodes        int           `json:"nodes"`
	AEInterval   time.Duration `json:"ae_interval"`
	Note         string        `json:"note"`
}

const (
	PathGossipOn  PathConfig = "gossip-on"
	PathGossipOff PathConfig = "gossip-off"
)

// MeasureGossipContrast runs the same registration→peer-visible workload with
// gossip piggyback enabled vs disabled (separate fabrics + periodic MerkleSync
// at aeInterval modeling anti-entropy-only catch-up).
func MeasureGossipContrast(reps, nodes int, aeInterval time.Duration) ContrastResult {
	if reps < 1 {
		reps = 1
	}
	if nodes < 2 {
		nodes = 3
	}
	if aeInterval <= 0 {
		aeInterval = 30 * time.Second // typical agent sync tier for small clusters is faster;
		// for catalog peer catch-up without gossip we model a slower AE interval.
	}

	on := measureSamples(reps, nodes, true, aeInterval)
	off := measureSamples(reps, nodes, false, aeInterval)
	sort.Slice(on, func(i, j int) bool { return on[i] < on[j] })
	sort.Slice(off, func(i, j int) bool { return off[i] < off[j] })

	p50 := func(s []time.Duration) time.Duration {
		if len(s) == 0 {
			return 0
		}
		return s[len(s)*50/100]
	}
	p99 := func(s []time.Duration) time.Duration {
		if len(s) == 0 {
			return 0
		}
		return s[min(len(s)*99/100, len(s)-1)]
	}

	on50, on99 := p50(on), p99(on)
	off50, off99 := p50(off), p99(off)
	slow50, slow99 := 0.0, 0.0
	if on50 > 0 {
		slow50 = float64(off50) / float64(on50)
	}
	if on99 > 0 {
		slow99 = float64(off99) / float64(on99)
	}

	return ContrastResult{
		GossipOnP50:  on50,
		GossipOnP99:  on99,
		GossipOffP50: off50,
		GossipOffP99: off99,
		SlowdownP50:  slow50,
		SlowdownP99:  slow99,
		Samples:      reps,
		Nodes:        nodes,
		AEInterval:   aeInterval,
		Note: "gossip-on uses SWIM piggyback Broadcast; gossip-off uses isolated fabrics + " +
			"MerkleSync once per AE interval (no live delta stream)",
	}
}

func measureSamples(reps, nodes int, gossipOn bool, aeInterval time.Duration) []time.Duration {
	out := make([]time.Duration, 0, reps)
	for i := 0; i < reps; i++ {
		out = append(out, measureContrastOnce(nodes, gossipOn, aeInterval))
	}
	return out
}

func measureContrastOnce(nodes int, gossipOn bool, aeInterval time.Duration) time.Duration {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	bus := events.NewBus(clk)

	var stores []*gstore.Store
	if gossipOn {
		cluster := gossip.NewCluster(clk)
		cluster.SetNetwork(gossip.NetworkConfig{Fanout: 0}) // full mesh for deterministic timing
		members := make([]*gossip.MemoryMembership, nodes)
		stores = make([]*gstore.Store, nodes)
		for i := 0; i < nodes; i++ {
			name := fmt.Sprintf("n%d", i)
			members[i] = gossip.NewMemory(cluster, name, "127.0.0.1", 9000+i)
			cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
			stores[i] = gstore.New(gstore.Config{Local: cs, Membership: members[i], Bus: bus})
		}
		for i := 1; i < nodes; i++ {
			_, _ = members[i].Join([]string{"n0"})
		}
	} else {
		// Isolated fabrics — Register on n0 cannot piggyback to peers.
		stores = make([]*gstore.Store, nodes)
		for i := 0; i < nodes; i++ {
			c := gossip.NewCluster(clk)
			c.SetNetwork(gossip.NetworkConfig{Fanout: 0})
			name := fmt.Sprintf("n%d", i)
			m := gossip.NewMemory(c, name, "127.0.0.1", 9000+i)
			cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
			stores[i] = gstore.New(gstore.Config{Local: cs, Membership: m, Bus: bus})
		}
	}

	tid := trace.NewIDAt(clk.Now())
	inst := &catalog.Instance{
		ID: "pay-1", Service: "payments", Node: "n0",
		Address: "10.0.0.1", Port: 8080, Health: catalog.HealthPassing, TraceID: tid,
	}
	start := clk.Now()
	_, _ = stores[0].Register(events.ContextWithTrace(context.Background(), tid), inst)

	if gossipOn {
		// Full-mesh Broadcast is synchronous; peers should already have it.
		// Advance a tiny amount for stage accounting.
		clk.Advance(time.Millisecond)
	} else {
		// Anti-entropy only: wait one AE interval then MerkleSync from n0 → others.
		clk.Advance(aeInterval)
		dig := stores[0].BuildDigest(true)
		all := stores[0].AllInstancesMap()
		for i := 1; i < nodes; i++ {
			_ = stores[i].MerkleSync(dig, all)
		}
	}

	// Convergence: every peer sees pay-1.
	for i := 1; i < nodes; i++ {
		if _, ok := stores[i].GetInstance("pay-1"); !ok {
			// Force one more AE cycle if needed.
			clk.Advance(aeInterval)
			dig := stores[0].BuildDigest(true)
			_ = stores[i].MerkleSync(dig, stores[0].AllInstancesMap())
		}
	}
	return clk.Now().Sub(start)
}

// ContrastAsPathResults adapts ContrastResult into PathResult rows for charts.
func ContrastAsPathResults(c ContrastResult) []PathResult {
	return []PathResult{
		{Config: PathGossipOn, P50: c.GossipOnP50, P99: c.GossipOnP99, Samples: c.Samples},
		{Config: PathGossipOff, P50: c.GossipOffP50, P99: c.GossipOffP99, Samples: c.Samples},
	}
}

// WriteContrastJSON writes contrast numbers for the console overlay.
func WriteContrastJSON(dir string, c ContrastResult) error {
	if dir == "" {
		dir = "tmp/sim"
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "gossip_contrast.json"), b, 0o600)
}

// ContrastMarkdown documents the slowdown factor.
func ContrastMarkdown(c ContrastResult) string {
	return fmt.Sprintf(
		"| Mode | p50 | p99 |\n|---|---|---|\n| gossip-on | %s | %s |\n| gossip-off (AE @ %s) | %s | %s |\n\n"+
			"**Slowdown factor:** p50 ≈ **%.1f×**, p99 ≈ **%.1f×** (gossip-off / gossip-on).\n\n%s\n",
		c.GossipOnP50, c.GossipOnP99, c.AEInterval, c.GossipOffP50, c.GossipOffP99,
		c.SlowdownP50, c.SlowdownP99, c.Note,
	)
}
