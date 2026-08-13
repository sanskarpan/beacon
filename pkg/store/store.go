// Package store defines the CatalogStore interface with AP (gossip) and CP (raft) backends.
package store

import (
	"context"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/watch"
)

// CatalogStore is the common interface for AP and CP backends.
type CatalogStore interface {
	Register(ctx context.Context, inst *catalog.Instance) (uint64, error)
	Deregister(ctx context.Context, id string) (uint64, error)
	UpdateHealth(ctx context.Context, id string, h catalog.HealthStatus) (uint64, error)
	Get(ctx context.Context, service string, opts catalog.QueryOptions) (*catalog.Result, error)
	GetNow(service string, opts catalog.QueryOptions) *catalog.Result
	GetInstance(id string) (*catalog.Instance, bool)
	InstancesOnNode(node string) []*catalog.Instance
	ListServices() map[string][]string
	Index() uint64
	Snapshot() *catalog.Snapshot
	Restore(snap *catalog.Snapshot) error
	// Mode returns "ap" or "cp".
	Mode() string
}

// MemoryStore wraps catalog.Store as CatalogStore (single-node AP/local).
type MemoryStore struct {
	*catalog.Store
	mode string
}

// NewMemory wraps a catalog store.
func NewMemory(s *catalog.Store, mode string) *MemoryStore {
	if mode == "" {
		mode = "ap"
	}
	return &MemoryStore{Store: s, mode: mode}
}

func (m *MemoryStore) Mode() string { return m.mode }

// Ensure MemoryStore implements CatalogStore.
var _ CatalogStore = (*MemoryStore)(nil)

// WatchNotifier can push watch events when the store mutates.
type WatchNotifier interface {
	Notify(service string, ev watch.Event)
}
