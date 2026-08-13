package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/trace"
)

var (
	// ErrNotFound is returned when an instance or service is missing.
	ErrNotFound = errors.New("catalog: not found")
	// ErrInvalid is a bad registration.
	ErrInvalid = errors.New("catalog: invalid")
)

// Store is an in-memory catalog with monotonic indexing and secondary indexes.
//
// UpdateHealth bumps the index ONLY IF THE STATUS ACTUALLY CHANGED.
// A check every 5s on 10k instances is 2k updates/s; bumping on every tick
// would wake every watcher continuously in a healthy idle cluster.
type Store struct {
	mu        sync.RWMutex
	index     uint64
	services  map[string]*Service
	instances map[string]*Instance // by ID

	// secondary indexes
	byService map[string]map[string]struct{} // service -> instance IDs
	byNode    map[string]map[string]struct{}
	byTag     map[string]map[string]struct{} // tag -> instance IDs
	byHealth  map[HealthStatus]map[string]struct{}

	// waiters for blocking queries: service -> list of wait chans
	waiters map[string][]*indexWaiter

	bus     *events.Bus
	clk     clock.Clock
	batcher *IndexBatcher
	// if batching is enabled, mutations stage service names and flush later
	batchEnabled bool
}

type indexWaiter struct {
	minIndex uint64
	ch       chan uint64
}

// StoreOption configures a Store.
type StoreOption func(*Store)

// WithBus attaches an event bus.
func WithBus(b *events.Bus) StoreOption {
	return func(s *Store) { s.bus = b }
}

// WithClock injects a clock.
func WithClock(c clock.Clock) StoreOption {
	return func(s *Store) { s.clk = c }
}

// WithBatchWindow enables coalesced index bumps (default 50ms).
func WithBatchWindow(d time.Duration) StoreOption {
	return func(s *Store) {
		s.batchEnabled = true
		s.batcher = NewIndexBatcher(s.clk, d, s.flushBatch)
	}
}

// NewStore creates an empty catalog.
func NewStore(opts ...StoreOption) *Store {
	s := &Store{
		services:  make(map[string]*Service),
		instances: make(map[string]*Instance),
		byService: make(map[string]map[string]struct{}),
		byNode:    make(map[string]map[string]struct{}),
		byTag:     make(map[string]map[string]struct{}),
		byHealth:  make(map[HealthStatus]map[string]struct{}),
		waiters:   make(map[string][]*indexWaiter),
		clk:       clock.New(),
	}
	for _, o := range opts {
		o(s)
	}
	if s.batchEnabled && s.batcher == nil {
		s.batcher = NewIndexBatcher(s.clk, 50*time.Millisecond, s.flushBatch)
	}
	return s
}

// Index returns the current catalog index.
func (s *Store) Index() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index
}

// Register upserts an instance. Returns the new (or current, if batched pending) index.
func (s *Store) Register(ctx context.Context, inst *Instance) (uint64, error) {
	if inst == nil || inst.ID == "" || inst.Service == "" {
		return 0, fmt.Errorf("%w: id and service required", ErrInvalid)
	}
	if inst.Weight <= 0 {
		inst.Weight = 1
	}
	if inst.Health == "" {
		inst.Health = HealthPassing
	}
	for i := range inst.Checks {
		inst.Checks[i].Defaults()
	}
	// recompute aggregate from checks if present
	if len(inst.Checks) > 0 {
		statuses := make([]HealthStatus, len(inst.Checks))
		for i, c := range inst.Checks {
			statuses[i] = c.Status
		}
		inst.Health = Aggregate(statuses)
	}
	traceID := events.TraceFrom(ctx)
	if traceID == "" {
		traceID = inst.TraceID
	}
	if traceID == "" {
		traceID = trace.NewID()
	}
	inst.TraceID = traceID

	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.instances[inst.ID]
	cp := inst.Clone()
	if exists {
		cp.CreateIndex = existing.CreateIndex
		// preserve incarnation max
		if cp.Incarnation < existing.Incarnation {
			cp.Incarnation = existing.Incarnation
		}
	}

	idx := s.bumpLocked(cp.Service)
	if !exists {
		cp.CreateIndex = idx
	}
	cp.ModifyIndex = idx
	cp.Deregistered = false
	cp.LastKnownHealth = cp.Health

	if exists {
		s.unindexLocked(existing)
	}
	s.instances[cp.ID] = cp
	s.indexLocked(cp)

	svc, ok := s.services[cp.Service]
	if !ok {
		svc = &Service{Name: cp.Service, Namespace: inst.Meta["namespace"]}
		s.services[cp.Service] = svc
	}
	svc.ModifyIndex = idx

	if s.bus != nil {
		s.bus.Publish(events.Event{
			Kind:     events.EvInstanceRegistered,
			TraceID:  traceID,
			Service:  cp.Service,
			Instance: cp.ID,
			Node:     cp.Node,
			Index:    idx,
		})
	}
	return idx, nil
}

// Deregister removes an instance. Idempotent.
func (s *Store) Deregister(ctx context.Context, id string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inst, ok := s.instances[id]
	if !ok {
		return s.index, nil
	}
	traceID := events.TraceFrom(ctx)
	if traceID == "" {
		traceID = inst.TraceID
	}

	svcName := inst.Service
	s.unindexLocked(inst)
	delete(s.instances, id)

	idx := s.bumpLocked(svcName)
	if svc, ok := s.services[svcName]; ok {
		svc.ModifyIndex = idx
		// drop service if no instances remain
		if len(s.byService[svcName]) == 0 {
			delete(s.services, svcName)
			delete(s.byService, svcName)
		}
	}

	if s.bus != nil {
		s.bus.Publish(events.Event{
			Kind:     events.EvInstanceDeregistered,
			TraceID:  traceID,
			Service:  svcName,
			Instance: id,
			Node:     inst.Node,
			Index:    idx,
		})
	}
	return idx, nil
}

// UpdateHealth sets instance health. Index bumps ONLY if status changed.
func (s *Store) UpdateHealth(ctx context.Context, id string, status HealthStatus) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inst, ok := s.instances[id]
	if !ok {
		return s.index, ErrNotFound
	}
	if inst.Health == status {
		return s.index, nil // no change → no bump → no wakeups
	}

	prev := inst.Health
	s.unindexLocked(inst)
	inst.Health = status
	if status == HealthPassing || status == HealthWarning {
		inst.LastKnownHealth = status
	}
	idx := s.bumpLocked(inst.Service)
	inst.ModifyIndex = idx
	if svc, ok := s.services[inst.Service]; ok {
		svc.ModifyIndex = idx
	}
	s.indexLocked(inst)

	if s.bus != nil {
		s.bus.Publish(events.Event{
			Kind:     events.EvHealthChanged,
			TraceID:  events.TraceFrom(ctx),
			Service:  inst.Service,
			Instance: id,
			From:     string(prev),
			To:       string(status),
			Index:    idx,
		})
	}
	return idx, nil
}

// UpdateCheckStatus updates one check and re-aggregates instance health.
func (s *Store) UpdateCheckStatus(ctx context.Context, instanceID string, checkID CheckID, status HealthStatus, output string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	inst, ok := s.instances[instanceID]
	if !ok {
		return s.index, ErrNotFound
	}
	found := false
	for i := range inst.Checks {
		if inst.Checks[i].ID == checkID {
			inst.Checks[i].Status = status
			inst.Checks[i].Output = output
			found = true
			break
		}
	}
	if !found {
		return s.index, ErrNotFound
	}
	statuses := make([]HealthStatus, len(inst.Checks))
	for i, c := range inst.Checks {
		statuses[i] = c.Status
	}
	agg := Aggregate(statuses)
	if inst.Health == agg {
		return s.index, nil
	}
	prev := inst.Health
	s.unindexLocked(inst)
	inst.Health = agg
	idx := s.bumpLocked(inst.Service)
	inst.ModifyIndex = idx
	if svc, ok := s.services[inst.Service]; ok {
		svc.ModifyIndex = idx
	}
	s.indexLocked(inst)

	if s.bus != nil {
		s.bus.Publish(events.Event{
			Kind:     events.EvHealthChanged,
			TraceID:  events.TraceFrom(ctx),
			Service:  inst.Service,
			Instance: instanceID,
			From:     string(prev),
			To:       string(agg),
			Index:    idx,
			Detail:   fmt.Sprintf("check %s → %s", checkID, status),
		})
	}
	return idx, nil
}

// Get returns instances for a service, optionally blocking until MinIndex is exceeded.
func (s *Store) Get(ctx context.Context, service string, opts QueryOptions) (*Result, error) {
	for {
		s.mu.RLock()
		idx := s.serviceIndexLocked(service)
		if opts.MinIndex == 0 || idx > opts.MinIndex {
			res := s.snapshotServiceLocked(service, opts)
			s.mu.RUnlock()
			return res, nil
		}
		// Guard: future index (restart/restore) → treat as 0, return now.
		if opts.MinIndex > s.index {
			res := s.snapshotServiceLocked(service, opts)
			s.mu.RUnlock()
			return res, nil
		}
		// Register waiter
		w := &indexWaiter{minIndex: opts.MinIndex, ch: make(chan uint64, 1)}
		s.mu.RUnlock()

		s.mu.Lock()
		// re-check under write lock
		idx = s.serviceIndexLocked(service)
		if idx > opts.MinIndex || opts.MinIndex > s.index {
			res := s.snapshotServiceLocked(service, opts)
			s.mu.Unlock()
			return res, nil
		}
		s.waiters[service] = append(s.waiters[service], w)
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			s.removeWaiter(service, w)
			return nil, ctx.Err()
		case <-w.ch:
			// loop and re-read
		}
	}
}

// GetNow is a non-blocking get.
func (s *Store) GetNow(service string, opts QueryOptions) *Result {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotServiceLocked(service, opts)
}

// GetInstance returns one instance by ID.
func (s *Store) GetInstance(id string) (*Instance, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inst, ok := s.instances[id]
	if !ok {
		return nil, false
	}
	return inst.Clone(), true
}

// InstancesOnNode returns all instances registered on a node.
func (s *Store) InstancesOnNode(node string) []*Instance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.byNode[node]
	out := make([]*Instance, 0, len(ids))
	for id := range ids {
		if inst, ok := s.instances[id]; ok {
			out = append(out, inst.Clone())
		}
	}
	return out
}

// ListServices returns all service names with instance counts.
func (s *Store) ListServices() map[string][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]string, len(s.services))
	for name := range s.services {
		tags := map[string]struct{}{}
		for id := range s.byService[name] {
			if inst, ok := s.instances[id]; ok {
				for _, t := range inst.Tags {
					tags[t] = struct{}{}
				}
			}
		}
		list := make([]string, 0, len(tags))
		for t := range tags {
			list = append(list, t)
		}
		sort.Strings(list)
		out[name] = list
	}
	return out
}

// Snapshot captures the full catalog.
func (s *Store) Snapshot() *Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := &Snapshot{
		Index:     s.index,
		Services:  make(map[string]*Service, len(s.services)),
		Instances: make(map[string]*Instance, len(s.instances)),
	}
	for k, v := range s.services {
		cp := *v
		snap.Services[k] = &cp
	}
	for k, v := range s.instances {
		snap.Instances[k] = v.Clone()
	}
	return snap
}

// Restore replaces the catalog with a snapshot.
func (s *Store) Restore(snap *Snapshot) error {
	if snap == nil {
		return fmt.Errorf("%w: nil snapshot", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services = make(map[string]*Service)
	s.instances = make(map[string]*Instance)
	s.byService = make(map[string]map[string]struct{})
	s.byNode = make(map[string]map[string]struct{})
	s.byTag = make(map[string]map[string]struct{})
	s.byHealth = make(map[HealthStatus]map[string]struct{})
	s.index = snap.Index
	for k, v := range snap.Services {
		cp := *v
		s.services[k] = &cp
	}
	for _, inst := range snap.Instances {
		cp := inst.Clone()
		s.instances[cp.ID] = cp
		s.indexLocked(cp)
	}
	// wake all waiters
	for svc, ws := range s.waiters {
		for _, w := range ws {
			select {
			case w.ch <- s.index:
			default:
			}
		}
		delete(s.waiters, svc)
	}
	return nil
}

// SetInstanceIncarnation updates gossip incarnation without a health bump if unchanged.
func (s *Store) SetInstanceIncarnation(id string, inc uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst, ok := s.instances[id]; ok {
		inst.Incarnation = inc
	}
}

// ReplaceAll clears and loads instances (used by anti-entropy bulk paths).
func (s *Store) ReplaceAll(instances []*Instance) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.services = make(map[string]*Service)
	s.instances = make(map[string]*Instance)
	s.byService = make(map[string]map[string]struct{})
	s.byNode = make(map[string]map[string]struct{})
	s.byTag = make(map[string]map[string]struct{})
	s.byHealth = make(map[HealthStatus]map[string]struct{})
	s.index++
	idx := s.index
	for _, inst := range instances {
		cp := inst.Clone()
		cp.CreateIndex = idx
		cp.ModifyIndex = idx
		s.instances[cp.ID] = cp
		s.indexLocked(cp)
		if _, ok := s.services[cp.Service]; !ok {
			s.services[cp.Service] = &Service{Name: cp.Service, ModifyIndex: idx}
		} else {
			s.services[cp.Service].ModifyIndex = idx
		}
	}
	s.wakeAllLocked()
	return idx, nil
}

// --- internal ---

func (s *Store) bumpLocked(service string) uint64 {
	if s.batchEnabled && s.batcher != nil {
		// Stage the service; still allocate a provisional index for CreateIndex.
		s.index++
		s.batcher.Touch(service)
		return s.index
	}
	s.index++
	s.wakeServiceLocked(service, s.index)
	return s.index
}

func (s *Store) flushBatch(services []string, index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// index may already equal s.index from individual bumps
	if index > s.index {
		s.index = index
	}
	seen := map[string]struct{}{}
	for _, svc := range services {
		if _, ok := seen[svc]; ok {
			continue
		}
		seen[svc] = struct{}{}
		if sv, ok := s.services[svc]; ok {
			sv.ModifyIndex = s.index
		}
		s.wakeServiceLocked(svc, s.index)
	}
	if s.bus != nil {
		s.bus.Publish(events.Event{
			Kind:  events.EvIndexBumped,
			Index: s.index,
			Detail: fmt.Sprintf("batched %d services", len(seen)),
		})
	}
}

func (s *Store) serviceIndexLocked(service string) uint64 {
	if svc, ok := s.services[service]; ok {
		return svc.ModifyIndex
	}
	return s.index
}

func (s *Store) snapshotServiceLocked(service string, opts QueryOptions) *Result {
	ids := s.byService[service]
	out := make([]*Instance, 0, len(ids))
	for id := range ids {
		inst, ok := s.instances[id]
		if !ok {
			continue
		}
		if !matchFilters(inst, opts) {
			continue
		}
		out = append(out, inst.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return &Result{
		Service:   service,
		Instances: out,
		Index:     s.serviceIndexLocked(service),
	}
}

func matchFilters(inst *Instance, opts QueryOptions) bool {
	if opts.Passing && inst.Health != HealthPassing {
		return false
	}
	if len(opts.Health) > 0 {
		ok := false
		for _, h := range opts.Health {
			if inst.Health == h {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	for _, tag := range opts.Tags {
		found := false
		for _, t := range inst.Tags {
			if t == tag {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if opts.Locality != nil {
		if opts.Locality.Region != "" && inst.Locality.Region != opts.Locality.Region {
			return false
		}
		if opts.Locality.Zone != "" && inst.Locality.Zone != opts.Locality.Zone {
			return false
		}
	}
	for k, v := range opts.Meta {
		if inst.Meta[k] != v {
			return false
		}
	}
	if opts.Filter != "" && !matchFilterExpr(inst, opts.Filter) {
		return false
	}
	return true
}

// matchFilterExpr supports a tiny subset: Meta.X == "Y" and Health == passing
func matchFilterExpr(inst *Instance, expr string) bool {
	expr = strings.TrimSpace(expr)
	// split on &&
	parts := strings.Split(expr, "&&")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.HasPrefix(p, "Meta.") {
			rest := strings.TrimPrefix(p, "Meta.")
			kv := strings.SplitN(rest, "==", 2)
			if len(kv) != 2 {
				return false
			}
			key := strings.TrimSpace(kv[0])
			val := strings.Trim(strings.TrimSpace(kv[1]), `"'`)
			if inst.Meta[key] != val {
				return false
			}
		} else if strings.HasPrefix(p, "Health") {
			kv := strings.SplitN(p, "==", 2)
			if len(kv) != 2 {
				return false
			}
			val := strings.Trim(strings.TrimSpace(kv[1]), `"'`)
			if string(inst.Health) != val {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func (s *Store) indexLocked(inst *Instance) {
	setAdd(s.byService, inst.Service, inst.ID)
	setAdd(s.byNode, inst.Node, inst.ID)
	for _, t := range inst.Tags {
		setAdd(s.byTag, t, inst.ID)
	}
	setAddHealth(s.byHealth, inst.Health, inst.ID)
}

func (s *Store) unindexLocked(inst *Instance) {
	setDel(s.byService, inst.Service, inst.ID)
	setDel(s.byNode, inst.Node, inst.ID)
	for _, t := range inst.Tags {
		setDel(s.byTag, t, inst.ID)
	}
	setDelHealth(s.byHealth, inst.Health, inst.ID)
}

func setAdd(m map[string]map[string]struct{}, key, id string) {
	if m[key] == nil {
		m[key] = make(map[string]struct{})
	}
	m[key][id] = struct{}{}
}

func setDel(m map[string]map[string]struct{}, key, id string) {
	if s, ok := m[key]; ok {
		delete(s, id)
		if len(s) == 0 {
			delete(m, key)
		}
	}
}

func setAddHealth(m map[HealthStatus]map[string]struct{}, h HealthStatus, id string) {
	if m[h] == nil {
		m[h] = make(map[string]struct{})
	}
	m[h][id] = struct{}{}
}

func setDelHealth(m map[HealthStatus]map[string]struct{}, h HealthStatus, id string) {
	if s, ok := m[h]; ok {
		delete(s, id)
		if len(s) == 0 {
			delete(m, h)
		}
	}
}

func (s *Store) wakeServiceLocked(service string, idx uint64) {
	ws := s.waiters[service]
	if len(ws) == 0 {
		return
	}
	keep := ws[:0]
	for _, w := range ws {
		if idx > w.minIndex {
			select {
			case w.ch <- idx:
			default:
			}
		} else {
			keep = append(keep, w)
		}
	}
	if len(keep) == 0 {
		delete(s.waiters, service)
	} else {
		s.waiters[service] = keep
	}
}

func (s *Store) wakeAllLocked() {
	for svc, ws := range s.waiters {
		for _, w := range ws {
			select {
			case w.ch <- s.index:
			default:
			}
		}
		delete(s.waiters, svc)
	}
}

func (s *Store) removeWaiter(service string, target *indexWaiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ws := s.waiters[service]
	keep := ws[:0]
	for _, w := range ws {
		if w != target {
			keep = append(keep, w)
		}
	}
	if len(keep) == 0 {
		delete(s.waiters, service)
	} else {
		s.waiters[service] = keep
	}
}

// MarshalJSON for debugging.
func (s *Store) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Snapshot())
}
