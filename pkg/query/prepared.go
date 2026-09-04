// Package query implements prepared queries — saved discovery queries with
// failover policies (Consul-style).
package query

import (
	"context"
	"fmt"
	"sync"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/store"
)

// Failover defines cross-datacenter or tag failover.
type Failover struct {
	// Datacenters to try in order after the local DC fails to yield results.
	Datacenters []string `json:"datacenters,omitempty"`
	// NearestN tries the N nearest DCs (when topology is known).
	NearestN int `json:"nearest_n,omitempty"`
}

// PreparedQuery is a named, reusable discovery query.
type PreparedQuery struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Service     string            `json:"service"`
	Tags        []string          `json:"tags,omitempty"`
	PassingOnly bool              `json:"passing_only"`
	Filter      string            `json:"filter,omitempty"`
	Failover    Failover          `json:"failover,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
}

// Store holds prepared queries.
type Store struct {
	mu      sync.RWMutex
	byID    map[string]*PreparedQuery
	byName  map[string]string // name → id
	catalog store.CatalogStore
	// optional multi-DC catalogs
	remote map[string]store.CatalogStore // dc → store
}

// New creates a prepared-query store.
func New(cat store.CatalogStore) *Store {
	return &Store{
		byID:    make(map[string]*PreparedQuery),
		byName:  make(map[string]string),
		catalog: cat,
		remote:  make(map[string]store.CatalogStore),
	}
}

// RegisterDC attaches a remote datacenter catalog for failover.
func (s *Store) RegisterDC(dc string, cat store.CatalogStore) {
	s.mu.Lock()
	s.remote[dc] = cat
	s.mu.Unlock()
}

// Create upserts a prepared query.
func (s *Store) Create(q *PreparedQuery) error {
	if q.Name == "" || q.Service == "" {
		return fmt.Errorf("name and service required")
	}
	if q.ID == "" {
		q.ID = q.Name
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.byID[q.ID]; ok && old.Name != q.Name {
		delete(s.byName, old.Name)
	}
	cp := *q
	s.byID[q.ID] = &cp
	s.byName[q.Name] = q.ID
	return nil
}

// Delete removes a prepared query.
func (s *Store) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.byID[id]; ok {
		delete(s.byName, q.Name)
		delete(s.byID, id)
	}
}

// Get returns a prepared query by id or name.
func (s *Store) Get(idOrName string) (*PreparedQuery, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if q, ok := s.byID[idOrName]; ok {
		cp := *q
		return &cp, true
	}
	if id, ok := s.byName[idOrName]; ok {
		q := s.byID[id]
		cp := *q
		return &cp, true
	}
	return nil, false
}

// List all prepared queries.
func (s *Store) List() []*PreparedQuery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*PreparedQuery, 0, len(s.byID))
	for _, q := range s.byID {
		cp := *q
		out = append(out, &cp)
	}
	return out
}

// Execute runs a prepared query against the local catalog, then failover DCs.
func (s *Store) Execute(ctx context.Context, idOrName string) (*catalog.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q, ok := s.Get(idOrName)
	if !ok {
		return nil, fmt.Errorf("prepared query not found: %s", idOrName)
	}
	opts := catalog.QueryOptions{
		Tags:    q.Tags,
		Passing: q.PassingOnly,
		Filter:  q.Filter,
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	res := s.catalog.GetNow(q.Service, opts)
	if len(res.Instances) > 0 {
		return res, nil
	}
	// Failover — single RLock snapshot to avoid TOCTOU
	s.mu.RLock()
	dcs := append([]string(nil), q.Failover.Datacenters...)
	remotes := make(map[string]store.CatalogStore, len(dcs))
	for _, dc := range dcs {
		remotes[dc] = s.remote[dc]
	}
	s.mu.RUnlock()
	for _, dc := range dcs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		remote := remotes[dc]
		if remote == nil {
			continue
		}
		r := remote.GetNow(q.Service, opts)
		if len(r.Instances) > 0 {
			r.Stale = true // cross-DC
			return r, nil
		}
	}
	return res, nil // empty local
}
