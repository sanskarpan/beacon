package gossip_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/gossip"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
)

func seedIdentical(t *testing.T, sa, sb *gstore.Store, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		inst := &catalog.Instance{
			ID: fmt.Sprintf("i-%d", i), Service: "svc", Address: "10.0.0.1", Port: 8000 + i,
			Health: catalog.HealthPassing, Node: "n",
		}
		_, err := sa.Register(context.Background(), inst)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := sa.Local().GetInstance(inst.ID)
		if !ok {
			t.Fatal("missing after register")
		}
		// Peer applies the same delta with the same incarnation — no full dump.
		sb.ApplyDelta(gstore.Delta{
			Type:        gossip.DeltaRegister,
			Instance:    got.Clone(),
			InstanceID:  got.ID,
			Incarnation: got.Incarnation,
			Index:       got.ModifyIndex,
			Health:      got.Health,
		})
	}
}

// TestMerkle_RootEqualSkipsTransfer: identical catalogs exchange only digests.
func TestMerkle_RootEqualSkipsTransfer(t *testing.T) {
	cluster := gossip.NewCluster(nil)
	a := gossip.NewMemory(cluster, "a", "127.0.0.1", 1)
	b := gossip.NewMemory(cluster, "b", "127.0.0.1", 2)
	_, _ = b.Join([]string{"a"})

	sa := gstore.New(gstore.Config{Local: catalog.NewStore(), Membership: a})
	sb := gstore.New(gstore.Config{Local: catalog.NewStore(), Membership: b})
	seedIdentical(t, sa, sb, 50)

	da := sa.BuildDigest(true)
	db := sb.BuildDigest(true)
	if da.Root != db.Root {
		t.Fatalf("roots differ on identical data: a=%s b=%s count=%d/%d", da.Root, db.Root, da.Count, db.Count)
	}

	res := sb.MerkleSync(da, nil)
	if !res.RootEqual {
		t.Fatal("expected RootEqual")
	}
	if res.Transferred != 0 {
		t.Fatalf("equal digests must transfer 0, got %d", res.Transferred)
	}
}

// TestMerkle_Missing100Deltas: node B missing 100 instances recovers only those.
func TestMerkle_Missing100Deltas(t *testing.T) {
	// Separate fabrics so Register on A does not gossip-piggyback to B —
	// we are testing Merkle catch-up of *missed* deltas, not live broadcast.
	ca := gossip.NewCluster(nil)
	cb := gossip.NewCluster(nil)
	a := gossip.NewMemory(ca, "a", "127.0.0.1", 1)
	b := gossip.NewMemory(cb, "b", "127.0.0.1", 2)

	sa := gstore.New(gstore.Config{Local: catalog.NewStore(), Membership: a})
	sb := gstore.New(gstore.Config{Local: catalog.NewStore(), Membership: b})
	seedIdentical(t, sa, sb, 200)

	exclusive := make(map[string]*catalog.Instance)
	for i := 0; i < 100; i++ {
		inst := &catalog.Instance{
			ID: fmt.Sprintf("miss-%d", i), Service: "payments", Address: "10.0.0.2", Port: 2000 + i,
			Health: catalog.HealthPassing, Node: "n",
		}
		_, _ = sa.Register(context.Background(), inst)
		got, _ := sa.Local().GetInstance(inst.ID)
		exclusive[inst.ID] = got
	}

	da := sa.BuildDigest(true)
	db := sb.BuildDigest(true)
	if da.Root == db.Root {
		t.Fatal("roots should differ when B is missing 100 instances")
	}

	need := gstore.DiffLeaves(db.Leaves, da.Leaves)
	if len(need) != 100 {
		t.Fatalf("need 100 ids, got %d: %v", len(need), gstore.FormatDiff(need))
	}
	for _, id := range need {
		if _, ok := exclusive[id]; !ok {
			t.Fatalf("diff included non-exclusive id %s", id)
		}
	}

	res := sb.MerkleSync(da, sa.AllInstancesMap())
	if res.Transferred != 100 {
		t.Fatalf("transferred=%d want 100; need=%v", res.Transferred, res.Needed)
	}
	if res.FullDumpBytes > 0 && res.SentBytes >= res.FullDumpBytes {
		t.Fatalf("expected partial savings: sent=%d full=%d", res.SentBytes, res.FullDumpBytes)
	}
	for id := range exclusive {
		if _, ok := sb.Local().GetInstance(id); !ok {
			t.Fatalf("missing %s after MerkleSync", id)
		}
	}

	// Second round: roots equal, zero transfer.
	res2 := sb.MerkleSync(sa.BuildDigest(true), sa.AllInstancesMap())
	if !res2.RootEqual {
		t.Fatalf("roots should match after sync: transferred=%d", res2.Transferred)
	}
	if res2.Transferred != 0 {
		t.Fatalf("second sync transferred %d, want 0", res2.Transferred)
	}
}
