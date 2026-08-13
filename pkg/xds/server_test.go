package xds_test

import (
	"context"
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/xds"
)

func TestADSOrdering(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "pay", Node: "n", Address: "1.1.1.1", Port: 8080, Health: catalog.HealthPassing,
	})
	s := xds.New(store.NewMemory(cs, "ap"), events.NewBus(nil))
	resps := s.HandleRequest(&xds.DiscoveryRequest{NodeID: "envoy-1", TypeURL: "ads"})
	if len(resps) < 4 {
		t.Fatalf("want ordered push of 4 types, got %d", len(resps))
	}
	// verify order matches AddOrder
	for i, typ := range xds.AddOrder {
		if resps[i].TypeURL != typ {
			t.Fatalf("order[%d]=%s want %s", i, resps[i].TypeURL, typ)
		}
	}
}

func TestNACKDoesNotResend(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "pay", Node: "n", Address: "1.1.1.1", Port: 8080, Health: catalog.HealthPassing,
	})
	bus := events.NewBus(nil)
	s := xds.New(store.NewMemory(cs, "ap"), bus)
	first := s.HandleRequest(&xds.DiscoveryRequest{NodeID: "e1", TypeURL: xds.TypeCluster})
	if len(first) == 0 {
		t.Fatal("expected push")
	}
	// NACK
	again := s.HandleRequest(&xds.DiscoveryRequest{
		NodeID: "e1", TypeURL: xds.TypeCluster,
		VersionInfo: first[0].VersionInfo, ResponseNonce: first[0].Nonce,
		ErrorDetail: &xds.ErrorDetail{Message: "reject"},
	})
	if again != nil {
		t.Fatal("NACK must not resend")
	}
}

func TestDeltaSmallerThanSotW(t *testing.T) {
	cs := catalog.NewStore()
	// 100 endpoints
	for i := 0; i < 100; i++ {
		_, _ = cs.Register(context.Background(), &catalog.Instance{
			ID: string(rune(i)), Service: "big", Node: "n",
			Address: "10.0.0.1", Port: 8000 + i, Health: catalog.HealthPassing,
		})
	}
	s := xds.New(store.NewMemory(cs, "ap"), nil)
	prev := s.BuildSnapshot("n")
	// one change
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "new", Service: "big", Node: "n", Address: "10.0.0.2", Port: 9999, Health: catalog.HealthPassing,
	})
	curr := s.BuildSnapshot("n")
	sotw := xds.SotWBytes(curr, xds.TypeEndpoint)
	delta := s.DeltaResponse("n", xds.TypeEndpoint, prev, curr)
	if delta.Bytes >= sotw {
		t.Fatalf("delta %d should be < sotw %d", delta.Bytes, sotw)
	}
}
