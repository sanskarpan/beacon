package sim_test

import (
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/sim"
)

func TestMeasureGossipContrast(t *testing.T) {
	// AE interval large enough that off ≫ on for virtual clock measurement.
	c := sim.MeasureGossipContrast(10, 5, 30*time.Second)
	if c.Samples != 10 {
		t.Fatalf("samples=%d", c.Samples)
	}
	if c.GossipOnP50 <= 0 {
		t.Fatal("gossip-on p50 should be > 0")
	}
	if c.GossipOffP50 < c.GossipOnP50 {
		t.Fatalf("gossip-off p50 (%s) should be >= gossip-on (%s)", c.GossipOffP50, c.GossipOnP50)
	}
	if c.SlowdownP50 < 10 {
		t.Fatalf("expected substantial slowdown, got %.1f×", c.SlowdownP50)
	}
	t.Logf("contrast: on p50=%s off p50=%s slowdown=%.1f×", c.GossipOnP50, c.GossipOffP50, c.SlowdownP50)

	if err := sim.WriteContrastJSON(t.TempDir(), c); err != nil {
		t.Fatal(err)
	}
	md := sim.ContrastMarkdown(c)
	if md == "" {
		t.Fatal("empty markdown")
	}
}
