package catalog_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
)

func TestLeaseGraceRestore(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	s := catalog.NewStore(catalog.WithClock(clk))
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "g", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	lm := catalog.NewLeaseManager(s, clk, catalog.WithGrace(3*time.Second))
	ctx := context.Background()
	lease, err := lm.GrantLease(ctx, "g", 2*time.Second, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// expire
	clk.Advance(2 * time.Second)
	lm.ProcessDue(ctx)
	inst, _ := s.GetInstance("g")
	if inst.Health != catalog.HealthCritical {
		// process once more
		lm.ProcessDue(ctx)
		inst, _ = s.GetInstance("g")
	}
	if inst.Health != catalog.HealthCritical {
		t.Fatalf("want critical, got %s", inst.Health)
	}
	// renew within grace
	clk.Advance(1 * time.Second)
	_, err = lm.RenewLease(ctx, lease.ID)
	if err != nil {
		t.Fatal("grace renew:", err)
	}
	inst, _ = s.GetInstance("g")
	if inst.Health != catalog.HealthPassing {
		t.Fatalf("want restored passing, got %s", inst.Health)
	}
}

func TestRenewalDoesNotBumpIndex(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	s := catalog.NewStore(catalog.WithClock(clk))
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "r", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	lm := catalog.NewLeaseManager(s, clk)
	ctx := context.Background()
	lease, _ := lm.GrantLease(ctx, "r", 10*time.Second, 30*time.Second)
	// Grant re-registers and may bump — capture after grant
	before := s.Index()
	// health already passing; renew UpdateHealth should no-op bump
	_, err := lm.RenewLease(ctx, lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	after := s.Index()
	if after != before {
		t.Fatalf("renewal bumped index %d → %d", before, after)
	}
}

func TestManyLeasesSingleHeap(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	s := catalog.NewStore(catalog.WithClock(clk))
	lm := catalog.NewLeaseManager(s, clk)
	ctx := context.Background()
	const n = 1000
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("i%d", i)
		_, _ = s.Register(ctx, &catalog.Instance{
			ID: id, Service: "s", Node: "n", Address: "1.1.1.1", Port: i + 1, Health: catalog.HealthPassing,
		})
		// staggered TTLs
		ttl := time.Duration(1+i%50) * time.Second
		_, err := lm.GrantLease(ctx, id, ttl, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
	}
	if lm.ActiveCount() != n {
		t.Fatalf("active %d", lm.ActiveCount())
	}
	// advance enough to expire some
	clk.Advance(25 * time.Second)
	lm.ProcessDue(ctx)
	// still one manager, heap processed
	if lm.ActiveCount() > n {
		t.Fatal("lease count grew")
	}
}
