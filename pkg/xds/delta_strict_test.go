package xds_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/xds"
)

// TestDeltaStrict1000xAt5000Endpoints (TODO-031):
// SPEC §12/§20 requires >1000× byte reduction of Delta vs SotW at 5,000
// endpoints with a single change. Per-endpoint EDS makes one change O(1)
// resources in Delta while SotW ships all 5,000.
func TestDeltaStrict1000xAt5000Endpoints(t *testing.T) {
	const n = 5000
	cs := catalog.NewStore()
	for i := 0; i < n; i++ {
		_, _ = cs.Register(context.Background(), &catalog.Instance{
			ID:      fmt.Sprintf("inst-%05d", i),
			Service: "huge",
			Node:    "n",
			Address: "10.0.0.1",
			Port:    10000 + i,
			Health:  catalog.HealthPassing,
		})
	}
	s := xds.New(store.NewMemory(cs, "ap"), nil)
	prev := s.BuildSnapshot("n")

	// One single endpoint change.
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID:      fmt.Sprintf("inst-%05d", n),
		Service: "huge",
		Node:    "n",
		Address: "10.0.0.2",
		Port:    1,
		Health:  catalog.HealthPassing,
	})
	curr := s.BuildSnapshot("n")

	sotw := xds.SotWBytes(curr, xds.TypeEndpoint)
	delta := s.DeltaResponse("n", xds.TypeEndpoint, prev, curr)
	if delta == nil || delta.Bytes == 0 {
		t.Fatal("delta empty")
	}
	if len(delta.Resources) != 1 {
		t.Fatalf("expected exactly 1 changed resource in delta, got %d", len(delta.Resources))
	}
	if len(delta.RemovedResources) != 0 {
		t.Fatalf("expected no removed resources, got %v", delta.RemovedResources)
	}
	ratio := float64(sotw) / float64(delta.Bytes)
	if ratio <= 1000 {
		t.Fatalf("strict requirement failed: SotW/Delta ratio = %.0fx (need > 1000x); sotw=%d delta=%d",
			ratio, sotw, delta.Bytes)
	}
	t.Logf("SotW/Delta ratio = %.0fx at %d endpoints (sotw=%d bytes, delta=%d bytes)",
		ratio, n, sotw, delta.Bytes)
}

// TestDeltaStrictRemoveAt5000 ensures removal-only changes also stay tiny.
func TestDeltaStrictRemoveAt5000(t *testing.T) {
	const n = 5000
	cs := catalog.NewStore()
	for i := 0; i < n; i++ {
		_, _ = cs.Register(context.Background(), &catalog.Instance{
			ID:      fmt.Sprintf("inst-%05d", i),
			Service: "huge",
			Node:    "n",
			Address: "10.0.0.1",
			Port:    10000 + i,
			Health:  catalog.HealthPassing,
		})
	}
	s := xds.New(store.NewMemory(cs, "ap"), nil)
	prev := s.BuildSnapshot("n")

	// Deregister one instance.
	_, _ = cs.Deregister(context.Background(), "inst-00000")
	curr := s.BuildSnapshot("n")

	sotw := xds.SotWBytes(curr, xds.TypeEndpoint)
	delta := s.DeltaResponse("n", xds.TypeEndpoint, prev, curr)
	if delta == nil || delta.Bytes != 0 {
		t.Fatalf("removal-only delta should be ~0 bytes (only names), got bytes=%d", delta.Bytes)
	}
	if len(delta.RemovedResources) != 1 {
		t.Fatalf("expected 1 removed resource, got %v", delta.RemovedResources)
	}
	// Removing costs only the removed resource names, not a full SotW.
	if sotw > 0 && len(delta.RemovedResources) == 1 {
		t.Logf("removal delta carries %d removed name(s); SotW would be %d bytes", len(delta.RemovedResources), sotw)
	}
}
