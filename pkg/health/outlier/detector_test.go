package outlier_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/health/outlier"
)

func TestMaxEjectionPercent(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	d := outlier.New(outlier.DefaultConfig(), clk, nil)
	const n = 100
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("10.0.0.%d:80", i)
		for j := 0; j < 10; j++ {
			d.Record(addr, fmt.Errorf("err"), 0)
		}
	}
	d.Sweep()
	if d.EjectedFraction() > 0.11 {
		t.Fatalf("ejected fraction %.2f exceeds cap", d.EjectedFraction())
	}
	// at least 90% stay in pool
	if d.EjectedCount() > 10 {
		t.Fatalf("ejected %d > 10", d.EjectedCount())
	}
}

func TestEjectOnConsecutiveErrors(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	cfg := outlier.DefaultConfig()
	cfg.MaxEjectionPercent = 50
	d := outlier.New(cfg, clk, nil)
	// seed healthy peers so cap allows one ejection
	for i := 0; i < 10; i++ {
		d.Record(fmt.Sprintf("good-%d", i), nil, 0)
	}
	for i := 0; i < 5; i++ {
		d.Record("bad:80", fmt.Errorf("e"), 0)
	}
	d.Sweep()
	if !d.IsEjected("bad:80") {
		t.Fatal("expected ejection")
	}
}
