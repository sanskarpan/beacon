package catalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
)

func TestLeaseExpiry(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	s := catalog.NewStore(catalog.WithClock(clk))
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "x", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	lm := catalog.NewLeaseManager(s, clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lm.Start(ctx)

	_, err := lm.GrantLease(ctx, "x", 5*time.Second, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Second)
	// allow fire
	time.Sleep(30 * time.Millisecond)
	clk.Advance(time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	inst, ok := s.GetInstance("x")
	if !ok {
		t.Fatal("should still exist")
	}
	if inst.Health != catalog.HealthCritical {
		// virtual clock + timer interaction may need more advances
		for i := 0; i < 20 && inst.Health != catalog.HealthCritical; i++ {
			clk.Advance(100 * time.Millisecond)
			time.Sleep(10 * time.Millisecond)
			inst, _ = s.GetInstance("x")
		}
	}
	if inst.Health != catalog.HealthCritical {
		t.Fatalf("want critical after TTL, got %s", inst.Health)
	}

	// removal after deregister_after
	clk.Advance(10 * time.Second)
	for i := 0; i < 20; i++ {
		clk.Advance(100 * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
		if _, ok := s.GetInstance("x"); !ok {
			return
		}
	}
	if _, ok := s.GetInstance("x"); ok {
		t.Fatal("should be removed after deregister_after")
	}
}

func TestLeaseRenewal(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	s := catalog.NewStore(catalog.WithClock(clk))
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "x", Service: "s", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	lm := catalog.NewLeaseManager(s, clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lm.Start(ctx)
	lease, _ := lm.GrantLease(ctx, "x", 5*time.Second, 30*time.Second)
	for i := 0; i < 5; i++ {
		clk.Advance(3 * time.Second)
		time.Sleep(10 * time.Millisecond)
		_, err := lm.RenewLease(ctx, lease.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	inst, ok := s.GetInstance("x")
	if !ok || inst.Health == catalog.HealthCritical {
		t.Fatal("renewal should keep alive")
	}
}
