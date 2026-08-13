package raft_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/gossip"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	rstore "github.com/sanskar/beacon/pkg/store/raft"
)

func BenchmarkWriteAPvsCP(b *testing.B) {
	clk := clock.New()
	// AP
	gc := gossip.NewCluster(clk)
	m := gossip.NewMemory(gc, "n0", "127.0.0.1", 1)
	ap := gstore.New(gstore.Config{Local: catalog.NewStore(catalog.WithClock(clk)), Membership: m})
	// CP
	c := rstore.NewCluster([]string{"a", "b", "c"}, clk, nil)
	cp := rstore.NewStore(c.Node("a"))

	b.Run("ap", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			_, _ = ap.Register(ctx, &catalog.Instance{
				ID: fmt.Sprintf("ap-%d", i), Service: "s", Node: "n0",
				Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
			})
		}
	})
	b.Run("cp", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			_, _ = cp.Register(ctx, &catalog.Instance{
				ID: fmt.Sprintf("cp-%d", i), Service: "s", Node: "a",
				Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
			})
		}
	})
}
