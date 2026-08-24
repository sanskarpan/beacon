package telemetry_test

import (
	"errors"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/telemetry"
)

func TestCallGraph_RPSAndErrorRate(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	g := telemetry.NewCallGraph(clk, nil, 10*time.Second)

	for i := 0; i < 8; i++ {
		g.Record("web", "payments", nil)
	}
	for i := 0; i < 2; i++ {
		g.Record("web", "payments", errors.New("boom"))
	}

	edges := g.Edges()
	if len(edges) != 1 {
		t.Fatalf("edges=%d want 1", len(edges))
	}
	e := edges[0]
	if e.Source != "web" || e.Target != "payments" {
		t.Fatalf("edge %+v", e)
	}
	if e.Successes != 8 || e.Failures != 2 {
		t.Fatalf("counts ok=%d fail=%d", e.Successes, e.Failures)
	}
	if e.ErrorRate < 0.19 || e.ErrorRate > 0.21 {
		t.Fatalf("error_rate=%v want ~0.2", e.ErrorRate)
	}
	// 10 samples / 10s window = 1 RPS
	if e.RPS < 0.9 || e.RPS > 1.1 {
		t.Fatalf("rps=%v want ~1", e.RPS)
	}
}
