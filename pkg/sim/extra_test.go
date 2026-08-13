package sim_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sanskar/beacon/pkg/sim"
)

func TestRolloutAndZoneFailure(t *testing.T) {
	r := sim.NewRunner(t.TempDir())
	defer r.Close()
	roll := r.Rollout(10)
	if !roll.OK {
		t.Fatalf("rollout: %+v", roll)
	}
	zf := r.ZoneFailure()
	if !zf.OK {
		t.Fatalf("zone-failure: %+v", zf)
	}
}

func TestYAMLScenario(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flap.yaml")
	content := `
name: flap-yaml
steps:
  - action: flap
  - action: assert
    name: hysteresis_zero_transitions_on_flap
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	r := sim.NewRunner(dir)
	defer r.Close()
	res, err := r.RunYAMLFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK {
		t.Fatalf("yaml scenario failed: %+v", res)
	}
}

func TestClusterSizeSweep(t *testing.T) {
	// light sweep for CI
	sizes := []int{3, 10}
	for _, n := range sizes {
		r := sim.NewRunner(t.TempDir())
		res := r.Propagate(n)
		r.Close()
		if !res.OK {
			t.Fatalf("propagate n=%d: %+v", n, res)
		}
	}
}
