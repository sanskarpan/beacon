package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	"github.com/sanskar/beacon/pkg/trace"
)

// transportCfg is the default transport model for convergence measurement:
// 50ms one-way latency with SWIM bounded fanout 3 — a real network, not the
// instant full-mesh fast path.
var transportCfg = gossip.NetworkConfig{Latency: 50 * time.Millisecond, Fanout: 3}

// measureConvergenceTime registers one instance on node 0 and returns the
// virtual time until every node in the cluster has it, under the given
// transport model. The second return is how many nodes converged.
func measureConvergenceTime(clk *clock.Virtual, bus *events.Bus, nodes int, cfg gossip.NetworkConfig) (time.Duration, int) {
	cluster := gossip.NewCluster(clk)
	cluster.SetNetwork(cfg)
	members := make([]*gossip.MemoryMembership, nodes)
	stores := make([]*gstore.Store, nodes)
	for i := 0; i < nodes; i++ {
		name := fmt.Sprintf("n%d", i)
		members[i] = gossip.NewMemory(cluster, name, "127.0.0.1", 8000+i)
		cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
		stores[i] = gstore.New(gstore.Config{Local: cs, Membership: members[i], Bus: bus})
	}
	for i := 1; i < nodes; i++ {
		_, _ = members[i].Join([]string{"n0"})
	}

	tid := trace.NewIDAt(clk.Now())
	inst := &catalog.Instance{
		ID: "pay-1", Service: "payments", Node: "n0",
		Address: "10.0.0.1", Port: 8080, Health: catalog.HealthPassing, TraceID: tid,
	}
	start := clk.Now()
	if _, err := stores[0].Register(events.ContextWithTrace(context.Background(), tid), inst); err != nil {
		return clk.Now().Sub(start), 0
	}

	// Advance in small steps until every node has the instance, bounded by a
	// generous cap (5s virtual) so a broken transport fails the sweep loudly.
	converged := 0
	for converged < nodes && clk.Now().Sub(start) < 5*time.Second {
		clk.Advance(10 * time.Millisecond)
		converged = 0
		for _, st := range stores {
			if _, ok := st.GetInstance("pay-1"); ok {
				converged++
			}
		}
	}
	return clk.Now().Sub(start), converged
}

// Convergence runs the 100-node transport-model convergence scenario
// (TODO-041) as a Runner scenario, so it is available to YAML and the CLI.
func (r *Runner) Convergence(nodes int) Result {
	res := Result{Name: "convergence", Metrics: map[string]any{}}
	if nodes <= 0 {
		nodes = 100
	}
	elapsed, converged := measureConvergenceTime(r.clk, r.bus, nodes, transportCfg)
	res.Metrics["nodes"] = nodes
	res.Metrics["converged"] = converged
	res.Metrics["elapsed_ms"] = elapsed.Milliseconds()
	res.Metrics["transport"] = "50ms latency, fanout 3, 0 loss"
	res.Assertions = append(res.Assertions,
		AssertResult{Name: "all_nodes_see_instance", OK: converged == nodes, Detail: fmt.Sprintf("%d/%d", converged, nodes)},
		AssertResult{Name: "convergence_under_2s", OK: elapsed <= 2*time.Second, Detail: elapsed.String()},
	)
	res.OK = allOK(res.Assertions)
	return res
}

// SweepSize is one row of the cluster-size sweep (TODO-043).
type SweepSize struct {
	Nodes     int           `json:"nodes"`
	Elapsed   time.Duration `json:"elapsed"`
	Converged int           `json:"converged"`
}

// Sweep measures convergence at 3, 10, 50, 100 and 500 nodes under the
// transport model and writes JSON + markdown (with a chart) into outDir.
func (r *Runner) Sweep() ([]SweepSize, error) {
	sizes := []int{3, 10, 50, 100, 500}
	rows := make([]SweepSize, 0, len(sizes))
	md := "| nodes | convergence | converged |\n|---|---|---|\n"
	for _, n := range sizes {
		clk := clock.NewVirtual(time.Unix(0, 0).UTC())
		bus := events.NewBus(clk)
		elapsed, converged := measureConvergenceTime(clk, bus, n, transportCfg)
		rows = append(rows, SweepSize{Nodes: n, Elapsed: elapsed, Converged: converged})
		md += fmt.Sprintf("| %d | %s | %d/%d |\n", n, elapsed, converged, n)
	}
	md += "\n```\n" + chartConvergence(rows) + "```\n"
	if r.outDir != "" {
		if err := writeJSONFile(r.outDir+"/sweep.json", rows); err != nil {
			return rows, err
		}
		if err := os.WriteFile(r.outDir+"/sweep.md", []byte(md), 0o644); err != nil {
			return rows, err
		}
	}
	return rows, nil
}

// chartConvergence renders a small ASCII bar chart of convergence time vs N.
func chartConvergence(rows []SweepSize) string {
	max := time.Duration(0)
	for _, r := range rows {
		if r.Elapsed > max {
			max = r.Elapsed
		}
	}
	width := 40
	s := "convergence (ms) vs nodes\n"
	for _, r := range rows {
		bars := 0
		if max > 0 {
			bars = int(r.Elapsed * time.Duration(width) / max)
		}
		line := ""
		for i := 0; i < bars; i++ {
			line += "#"
		}
		s += fmt.Sprintf("%4d | %-40s %dms\n", r.Nodes, line, r.Elapsed.Milliseconds())
	}
	return s
}

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
