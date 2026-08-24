package consensus_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store/raft/consensus"
	raftlib "github.com/sanskarpan/raft-consensus/pkg/raft"
)

func TestConsensus_ThreeNodeRegisterAndRead(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	cluster, err := consensus.NewCluster([]string{"n1", "n2", "n3"}, clk, bus)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	leader := cluster.Leader(8 * time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	st := consensus.NewStore(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	inst := &catalog.Instance{
		ID: "pay-1", Service: "payments", Address: "10.0.0.1", Port: 8080,
		Health: catalog.HealthPassing, Node: "agent-1",
	}
	if _, err := st.Register(ctx, inst); err != nil {
		t.Fatalf("register: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range []string{"n1", "n2", "n3"} {
			n := cluster.Node(id)
			if _, found := n.FSM.Store().GetInstance("pay-1"); !found {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("registration did not replicate to all FSM nodes")
}

func TestConsensus_MinorityRejectsWrites(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	cluster, err := consensus.NewCluster([]string{"a", "b", "c"}, clk, bus)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	leader := cluster.Leader(8 * time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	others := []string{}
	for _, id := range []string{"a", "b", "c"} {
		if id != leader.ID {
			others = append(others, id)
		}
	}
	cluster.Partition([]string{leader.ID}, others)
	time.Sleep(2 * time.Second)

	st := consensus.NewStore(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = st.Register(ctx, &catalog.Instance{
		ID: "x", Service: "s", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	if err == nil && leader.Raft.State() == raftlib.StateLeader {
		t.Fatal("expected write rejection under minority partition")
	}
	if err != nil {
		t.Logf("write rejected as expected: %v", err)
	}

	cluster.Heal()
	var last error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		l := cluster.Leader(2 * time.Second)
		if l == nil {
			continue
		}
		st = consensus.NewStore(l)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
		_, last = st.Register(ctx2, &catalog.Instance{
			ID: "y", Service: "s", Address: "2.2.2.2", Port: 2, Health: catalog.HealthPassing,
		})
		cancel2()
		if last == nil {
			return
		}
	}
	t.Fatalf("cluster did not accept writes after heal: %v", last)
}

func TestConsensus_ApplyDeterministicNoTimeNow(t *testing.T) {
	fsm := consensus.NewCatalogFSM(clock.NewVirtual(time.Unix(100, 0).UTC()), nil)
	cmd := consensus.Command{
		Type:      consensus.CmdRegister,
		Timestamp: time.Unix(12345, 0).UTC(),
		TraceID:   "t1",
		Instance: &catalog.Instance{
			ID: "i", Service: "s", Address: "1.1.1.1", Port: 9, Health: catalog.HealthPassing,
		},
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fsm.Apply(b); err != nil {
		t.Fatal(err)
	}
	in, ok := fsm.Store().GetInstance("i")
	if !ok {
		t.Fatal("missing instance")
	}
	if in.Meta["raft_ts"] != cmd.Timestamp.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("timestamp not preserved from command: %v", in.Meta)
	}
	_ = errors.Is // keep errors available for future asserts
}
