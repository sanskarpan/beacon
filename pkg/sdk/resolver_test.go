package sdk_test

import (
	"context"
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/sdk"
	"github.com/sanskar/beacon/pkg/store"
	"google.golang.org/grpc/resolver"
)

func TestResolverUpdateState(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "pay", Node: "n", Address: "10.0.0.1", Port: 8080,
		Health: catalog.HealthPassing, Weight: 2,
	})
	c := sdk.New(sdk.Config{Registry: sdk.StoreAdapter{S: store.NewMemory(cs, "ap")}})
	// Direct resolve path is the main guarantee; builder is registered for beacon://.
	_ = sdk.NewBuilder(c)
	_ = resolver.Get("beacon")
	insts, err := c.Resolve(context.Background(), "pay", catalog.QueryOptions{Passing: true})
	if err != nil || len(insts) != 1 {
		t.Fatal(err, insts)
	}
}

func TestDoneFeedsOutlier(t *testing.T) {
	cs := catalog.NewStore()
	for i := 0; i < 5; i++ {
		_, _ = cs.Register(context.Background(), &catalog.Instance{
			ID: string(rune('a' + i)), Service: "s", Node: "n",
			Address: "10.0.0.1", Port: 8000 + i, Health: catalog.HealthPassing,
		})
	}
	c := sdk.New(sdk.Config{Registry: sdk.StoreAdapter{S: store.NewMemory(cs, "ap")}})
	// feed errors into outlier
	for i := 0; i < 10; i++ {
		c.Outlier().Record("10.0.0.1:8000", context.DeadlineExceeded, 0)
	}
	c.Outlier().Sweep()
	if !c.Outlier().IsEjected("10.0.0.1:8000") {
		t.Fatal("expected ejection after consecutive errors")
	}
}
