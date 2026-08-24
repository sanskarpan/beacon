package sim

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
)

// scenarioDir returns the test/scenario directory, working from the package
// dir (pkg/sim) or the repo root.
func scenarioDir(t *testing.T) string {
	t.Helper()
	for _, p := range []string{"../../test/scenario", "test/scenario"} {
		if fi, err := os.Stat(p); err == nil && fi.IsDir() {
			return p
		}
	}
	t.Fatal("test/scenario not found")
	return ""
}

// TestYAMLScenariosAll (TODO-040) runs every declarative scenario file in
// test/scenario and fails if any assertion in any file is false.
func TestYAMLScenariosAll(t *testing.T) {
	dir := scenarioDir(t)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".yaml") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	if len(files) < 8 {
		t.Fatalf("expected >= 8 scenario files, found %d", len(files))
	}
	r := NewRunner(t.TempDir())
	defer r.Close()
	ran := 0
	for _, f := range files {
		res, err := r.RunYAMLFile(f)
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		ran++
		for _, a := range res.Assertions {
			if !a.OK {
				t.Errorf("%s: assertion %q failed: %s", f, a.Name, a.Detail)
			}
		}
		if !res.OK {
			t.Errorf("%s: scenario overall failed", f)
		}
	}
	t.Logf("ran %d YAML scenarios from %s", ran, dir)
}

// TestCIConvergenceGate (TODO-044) is the CI regression gate: 100-node
// convergence under the transport model must stay under a bound. The bound is
// configurable via BEACON_CONV_GATE_MS (default 2000ms) so CI can tighten it
// on fast hardware or loosen it under load.
func TestCIConvergenceGate(t *testing.T) {
	bound := 2000 * time.Millisecond
	if v := os.Getenv("BEACON_CONV_GATE_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			bound = time.Duration(ms) * time.Millisecond
		}
	}
	reps := 3
	if v := os.Getenv("BEACON_CONV_GATE_REPS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			reps = n
		}
	}
	var max time.Duration
	for i := 0; i < reps; i++ {
		clk := clock.NewVirtual(time.Unix(0, 0).UTC())
		bus := events.NewBus(clk)
		elapsed, converged := measureConvergenceTime(clk, bus, 100, transportCfg)
		if converged != 100 {
			t.Fatalf("rep %d: convergence failed (%d/100)", i, converged)
		}
		if elapsed > max {
			max = elapsed
		}
	}
	if max >= bound {
		t.Fatalf("convergence gate exceeded: max %s over %d reps (bound %s, set BEACON_CONV_GATE_MS to adjust)", max, reps, bound)
	}
	t.Logf("convergence gate OK: max %s over %d reps (bound %s)", max, reps, bound)
}

// TestSweepArtifacts (TODO-043) verifies the cluster-size sweep produces
// JSON + markdown with a chart, and that every size converges fully.
func TestSweepArtifacts(t *testing.T) {
	out := t.TempDir()
	r := NewRunner(out)
	defer r.Close()
	rows, err := r.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("expected 5 sweep sizes, got %d", len(rows))
	}
	want := []int{3, 10, 50, 100, 500}
	for i, row := range rows {
		if row.Nodes != want[i] {
			t.Errorf("sweep row %d: nodes=%d want %d", i, row.Nodes, want[i])
		}
		if row.Converged != row.Nodes {
			t.Errorf("nodes=%d: converged %d/%d", row.Nodes, row.Converged, row.Nodes)
		}
		if row.Elapsed <= 0 {
			t.Errorf("nodes=%d: zero elapsed", row.Nodes)
		}
	}
	for _, f := range []string{"sweep.json", "sweep.md"} {
		b, err := os.ReadFile(filepath.Join(out, f))
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if len(b) == 0 {
			t.Fatalf("%s empty", f)
		}
	}
	md, _ := os.ReadFile(filepath.Join(out, "sweep.md"))
	if !strings.Contains(string(md), "500") || !strings.Contains(string(md), "#") {
		t.Errorf("sweep.md missing chart or 500-node row:\n%s", md)
	}
	t.Logf("sweep: %dms (3) → %dms (500)", rows[0].Elapsed.Milliseconds(), rows[4].Elapsed.Milliseconds())
}
