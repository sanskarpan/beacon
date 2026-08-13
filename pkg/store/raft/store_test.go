package raft_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	rstore "github.com/sanskar/beacon/pkg/store/raft"
)

func TestCPLeaderWriteLinearizable(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	c := rstore.NewCluster([]string{"a", "b", "c"}, clk, bus)
	leader := rstore.NewStore(c.Node("a"))
	follower := rstore.NewStore(c.Node("b"))

	idx, err := leader.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "s", Node: "a", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	if err != nil || idx == 0 {
		t.Fatal(err, idx)
	}
	// replicated
	if !waitInst(follower, "1", time.Second) {
		t.Fatal("not on follower")
	}
}

func TestCPMinorityRejects(t *testing.T) {
	clk := clock.New()
	c := rstore.NewCluster([]string{"a", "b", "c"}, clk, nil)
	leader := rstore.NewStore(c.Node("a"))
	minority := rstore.NewStore(c.Node("c"))
	c.Partition([]string{"a", "b"}, []string{"c"})

	_, err := leader.Register(context.Background(), &catalog.Instance{
		ID: "x", Service: "s", Node: "a", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = minority.Register(context.Background(), &catalog.Instance{
		ID: "y", Service: "s", Node: "c", Address: "2.2.2.2", Port: 1, Health: catalog.HealthPassing,
	})
	if err == nil {
		t.Fatal("expected error on minority write")
	}
}

func TestCPStaleRead(t *testing.T) {
	clk := clock.New()
	c := rstore.NewCluster([]string{"a", "b", "c"}, clk, nil)
	leader := rstore.NewStore(c.Node("a"))
	follower := rstore.NewStore(c.Node("b"))
	_, _ = leader.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "s", Node: "a", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	waitInst(follower, "1", time.Second)
	res := follower.GetNow("s", catalog.QueryOptions{})
	if !res.Stale {
		t.Fatal("follower read should be stale")
	}
}

func TestCPHeal(t *testing.T) {
	clk := clock.New()
	c := rstore.NewCluster([]string{"a", "b", "c"}, clk, nil)
	leader := rstore.NewStore(c.Node("a"))
	c.Partition([]string{"a", "b"}, []string{"c"})
	_, _ = leader.Register(context.Background(), &catalog.Instance{
		ID: "h", Service: "s", Node: "a", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	c.Heal()
	if !waitInst(rstore.NewStore(c.Node("c")), "h", time.Second) {
		t.Fatal("heal did not sync")
	}
}

func waitInst(s *rstore.Store, id string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, ok := s.GetInstance(id); ok {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, ok := s.GetInstance(id)
	return ok
}
