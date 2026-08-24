// Package telemetry tracks observed call-graph edges (RPS / error rate) for the console.
package telemetry

import (
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
)

// EdgeKey identifies a directed call relationship.
type EdgeKey struct {
	Source string `json:"source"` // calling service
	Target string `json:"target"` // called service
}

// EdgeStats is a live summary of one call edge.
type EdgeStats struct {
	Source    string  `json:"source"`
	Target    string  `json:"target"`
	RPS       float64 `json:"rps"`
	ErrorRate float64 `json:"error_rate"` // 0..1
	Successes int64   `json:"successes"`
	Failures  int64   `json:"failures"`
	// Window is the sample window length in seconds.
	WindowSec float64 `json:"window_sec"`
}

type sample struct {
	at  time.Time
	err bool
}

// CallGraph accumulates SDK outcome reports into edges.
type CallGraph struct {
	mu      sync.Mutex
	clk     clock.Clock
	bus     *events.Bus
	window  time.Duration
	samples map[EdgeKey][]sample
}

// NewCallGraph creates a graph with a rolling window (default 10s).
func NewCallGraph(clk clock.Clock, bus *events.Bus, window time.Duration) *CallGraph {
	if clk == nil {
		clk = clock.New()
	}
	if window <= 0 {
		window = 10 * time.Second
	}
	return &CallGraph{
		clk:     clk,
		bus:     bus,
		window:  window,
		samples: make(map[EdgeKey][]sample),
	}
}

// Record adds one RPC outcome from source → target.
func (g *CallGraph) Record(source, target string, err error) {
	if source == "" {
		source = "unknown"
	}
	if target == "" {
		target = "unknown"
	}
	k := EdgeKey{Source: source, Target: target}
	now := g.clk.Now()
	g.mu.Lock()
	defer g.mu.Unlock()
	ss := append(g.samples[k], sample{at: now, err: err != nil})
	// prune
	cut := now.Add(-g.window)
	i := 0
	for i < len(ss) && ss[i].at.Before(cut) {
		i++
	}
	g.samples[k] = ss[i:]
	if g.bus != nil {
		detail := "ok"
		if err != nil {
			detail = "error"
		}
		g.bus.Publish(events.Event{
			Kind:    "telemetry.call",
			Service: target,
			From:    source,
			To:      target,
			Detail:  detail,
		})
	}
}

// Edges returns current RPS / error-rate for all observed edges.
func (g *CallGraph) Edges() []EdgeStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clk.Now()
	cut := now.Add(-g.window)
	out := make([]EdgeStats, 0, len(g.samples))
	winSec := g.window.Seconds()
	if winSec <= 0 {
		winSec = 1
	}
	for k, ss := range g.samples {
		var ok, fail int64
		kept := ss[:0]
		for _, s := range ss {
			if s.at.Before(cut) {
				continue
			}
			kept = append(kept, s)
			if s.err {
				fail++
			} else {
				ok++
			}
		}
		g.samples[k] = kept
		total := ok + fail
		er := 0.0
		if total > 0 {
			er = float64(fail) / float64(total)
		}
		out = append(out, EdgeStats{
			Source: k.Source, Target: k.Target,
			RPS: float64(total) / winSec, ErrorRate: er,
			Successes: ok, Failures: fail, WindowSec: winSec,
		})
	}
	return out
}
