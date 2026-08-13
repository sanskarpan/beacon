package sdk_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/sdk"
	"github.com/sanskar/beacon/pkg/store"
)

func TestNeverEmptyResolve(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "pay", Node: "n", Address: "1.1.1.1", Port: 8080, Health: catalog.HealthPassing,
	})
	c := sdk.New(sdk.Config{Registry: sdk.StoreAdapter{S: store.NewMemory(cs, "ap")}})
	insts, err := c.Resolve(context.Background(), "pay", catalog.QueryOptions{Passing: true})
	if err != nil || len(insts) != 1 {
		t.Fatal(err, insts)
	}
	// mark all critical
	_, _ = cs.UpdateHealth(context.Background(), "1", catalog.HealthCritical)
	insts, err = c.Resolve(context.Background(), "pay", catalog.QueryOptions{Passing: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(insts) == 0 {
		t.Fatal("must keep last good set")
	}
}

func TestDiskCache(t *testing.T) {
	dir := t.TempDir()
	cs := catalog.NewStore()
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "pay", Node: "n", Address: "1.1.1.1", Port: 8080, Health: catalog.HealthPassing,
	})
	c := sdk.New(sdk.Config{
		Registry: sdk.StoreAdapter{S: store.NewMemory(cs, "ap")},
		CacheDir: dir,
	})
	_, _ = c.Resolve(context.Background(), "pay", catalog.QueryOptions{})
	if _, err := os.Stat(filepath.Join(dir, "pay.json")); err != nil {
		t.Fatal("cache not written")
	}
}

func TestBackoffJitterSpread(t *testing.T) {
	// 100 clients reconnect delays should not all be equal
	seen := map[time.Duration]int{}
	for i := 0; i < 100; i++ {
		d := sdk.BackoffWithJitter(3, nil)
		seen[d]++
	}
	if len(seen) < 10 {
		t.Fatalf("insufficient jitter spread: %d unique values", len(seen))
	}
}

func TestLeaseRenewalKeepsAlive(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	cs := catalog.NewStore(catalog.WithClock(clk))
	c := sdk.New(sdk.Config{
		Registry: sdk.StoreAdapter{S: store.NewMemory(cs, "ap")},
		Clock:    clk,
	})
	inst := &catalog.Instance{
		ID: "1", Service: "pay", Node: "n", Address: "1.1.1.1", Port: 1,
		Health: catalog.HealthPassing,
		Lease:  &catalog.Lease{TTL: 4 * time.Second},
	}
	_, err := c.Register(context.Background(), inst)
	if err != nil {
		t.Fatal(err)
	}
	// advance past half TTL so renew fires
	clk.Advance(3 * time.Second)
	time.Sleep(20 * time.Millisecond)
	if _, ok := cs.GetInstance("1"); !ok {
		t.Fatal("should still be registered")
	}
	c.GracefulShutdown(context.Background())
}
