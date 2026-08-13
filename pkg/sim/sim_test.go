package sim_test

import (
	"testing"

	"github.com/sanskar/beacon/pkg/sim"
)

func TestScenarios(t *testing.T) {
	r := sim.NewRunner(t.TempDir())
	defer r.Close()
	for _, res := range r.RunAll() {
		t.Logf("%s ok=%v metrics=%v", res.Name, res.OK, res.Metrics)
		if !res.OK {
			for _, a := range res.Assertions {
				if !a.OK {
					t.Errorf("%s: %s — %s", res.Name, a.Name, a.Detail)
				}
			}
		}
	}
}

func TestPropagationMeasure(t *testing.T) {
	results := sim.MeasurePropagation(5, 5)
	if len(results) != 4 {
		t.Fatal(len(results))
	}
	// gossip path should be faster than dns path
	var gossip, dns sim.PathResult
	for _, r := range results {
		if r.Config == sim.PathGossipStream {
			gossip = r
		}
		if r.Config == sim.PathHealthDNS {
			dns = r
		}
	}
	if gossip.P99 >= dns.P50 {
		t.Fatalf("gossip p99 %s should be << dns p50 %s", gossip.P99, dns.P50)
	}
	t.Log("\n" + sim.MarkdownTable(results))
}
