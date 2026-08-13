package query_test

import (
	"context"
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/query"
	"github.com/sanskar/beacon/pkg/store"
)

func TestPreparedQueryFailover(t *testing.T) {
	local := catalog.NewStore()
	remote := catalog.NewStore()
	_, _ = remote.Register(context.Background(), &catalog.Instance{
		ID: "r1", Service: "pay", Node: "dc2-n1", Address: "10.1.0.1", Port: 8080,
		Health: catalog.HealthPassing,
	})
	qs := query.New(store.NewMemory(local, "ap"))
	qs.RegisterDC("dc2", store.NewMemory(remote, "ap"))
	_ = qs.Create(&query.PreparedQuery{
		Name: "pay-primary", Service: "pay", PassingOnly: true,
		Failover: query.Failover{Datacenters: []string{"dc2"}},
	})
	res, err := qs.Execute(context.Background(), "pay-primary")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Instances) != 1 || res.Instances[0].ID != "r1" {
		t.Fatalf("failover failed: %+v", res)
	}
	if !res.Stale {
		t.Fatal("cross-DC should mark stale")
	}
}
