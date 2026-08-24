package lab_test

import (
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/lab"
)

func TestConsistencyLab_PartitionDivergenceAndHeal(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	l := lab.NewConsistencyLab(clk, bus)

	// Baseline write on A should reach B (full mesh).
	if _, err := l.WriteAP("a"); err != nil {
		t.Fatal(err)
	}
	// Allow any async delivery (full mesh is sync, but be safe).
	time.Sleep(20 * time.Millisecond)
	st := l.Snapshot()
	if st.Divergence != 0 {
		t.Fatalf("healthy mesh divergence=%d want 0 (a=%d b=%d)", st.Divergence, st.APAInstances, st.APBInstances)
	}

	l.Partition()
	// Independent writes on each side → divergence.
	if _, err := l.WriteAP("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.WriteAP("b"); err != nil {
		t.Fatal(err)
	}
	st = l.Snapshot()
	if !st.Partitioned {
		t.Fatal("expected partitioned")
	}
	if st.Divergence < 2 {
		t.Fatalf("divergence=%d want >=2 under partition", st.Divergence)
	}
	if st.CPMinorityOK {
		t.Fatalf("minority should reject: %s", st.CPMinorityMsg)
	}

	// Majority CP write still works.
	if _, err := l.WriteCP(false); err != nil {
		t.Fatalf("majority write: %v", err)
	}
	// Minority write fails.
	if _, err := l.WriteCP(true); err == nil {
		t.Fatal("expected minority write failure")
	}

	l.Heal()
	// After heal AP still has divergence until re-sync — we only assert partition flag.
	st = l.Snapshot()
	if st.Partitioned {
		t.Fatal("heal should clear partition flag")
	}
}
