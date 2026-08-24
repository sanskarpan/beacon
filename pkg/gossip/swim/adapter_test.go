package swim_test

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	beaconswim "github.com/sanskar/beacon/pkg/gossip/swim"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	"github.com/sanskar/beacon/pkg/testutil"
)

func TestSWIM_MultiNodeRegistrationPropagates(t *testing.T) {
	cluster := beaconswim.NewCluster(true)
	a, err := cluster.NewNode("n1", "127.0.0.1", 17946)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Stop()
	b, err := cluster.NewNode("n2", "127.0.0.1", 17947)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	c, err := cluster.NewNode("n3", "127.0.0.1", 17948)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	if n, err := b.Join([]string{"n1"}); err != nil || n < 1 {
		t.Fatalf("join b: n=%d err=%v", n, err)
	}
	if n, err := c.Join([]string{"n1"}); err != nil || n < 1 {
		t.Fatalf("join c: n=%d err=%v", n, err)
	}

	// Wait until membership converges (all see 3 alive).
	testutil.WaitUntil(t, 5*time.Second, 50*time.Millisecond, func() bool {
		return a.Size() >= 3 && b.Size() >= 3 && c.Size() >= 3
	}, "membership convergence")
	if a.Size() < 2 {
		t.Fatalf("membership not converged: a=%d b=%d c=%d", a.Size(), b.Size(), c.Size())
	}

	bus := events.NewBus(nil)
	cs1 := catalog.NewStore(catalog.WithBus(bus))
	cs2 := catalog.NewStore(catalog.WithBus(bus))
	cs3 := catalog.NewStore(catalog.WithBus(bus))
	s1 := gstore.New(gstore.Config{Local: cs1, Membership: a, Bus: bus})
	s2 := gstore.New(gstore.Config{Local: cs2, Membership: b, Bus: bus})
	s3 := gstore.New(gstore.Config{Local: cs3, Membership: c, Bus: bus})

	inst := &catalog.Instance{
		ID: "pay-1", Service: "payments", Node: "n1",
		Address: "10.0.0.1", Port: 8080, Health: catalog.HealthPassing,
	}
	if _, err := s1.Register(context.Background(), inst); err != nil {
		t.Fatal(err)
	}

	// Catalog delta piggybacks on SWIM Broadcast — wait for peers.
	testutil.WaitUntil(t, 3*time.Second, 20*time.Millisecond, func() bool {
		_, ok2 := s2.GetInstance("pay-1")
		_, ok3 := s3.GetInstance("pay-1")
		return ok2 && ok3
	}, "SWIM piggyback propagation")

	// Failure event: kill n1 → peers mark its instances critical.
	ch := make(chan gossip.MemberEvent, 8)
	b.Subscribe(ch)
	a.Fail()

	// Either membership event or catalog critical within bound.
	testutil.WaitUntil(t, 3*time.Second, 20*time.Millisecond, func() bool {
		if in, found := s2.GetInstance("pay-1"); found && in.Health == catalog.HealthCritical {
			return true
		}
		select {
		case ev := <-ch:
			if ev.Type == gossip.Failed {
				if in, found := s2.GetInstance("pay-1"); found && in.Health == catalog.HealthCritical {
					return true
				}
			}
		default:
		}
		return false
	}, "instance critical after SWIM failure")
}
