// Package gossip implements the AP catalog backend with SWIM-piggybacked deltas.
//
// When membership declares a node dead, every instance on that node is marked
// critical immediately (~2s via SWIM vs ~15s via health checks) — the highest-
// value integration with the existing gossip project.
package gossip

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	"github.com/sanskar/beacon/pkg/watch"
)

// Delta is a catalog mutation piggybacked on gossip.
type Delta struct {
	Type        gossip.CatalogDeltaType `json:"type"`
	Instance    *catalog.Instance       `json:"instance,omitempty"`
	InstanceID  string                  `json:"instance_id,omitempty"`
	Health      catalog.HealthStatus    `json:"health,omitempty"`
	Index       uint64                  `json:"index"`
	Origin      gossip.NodeID           `json:"origin"`
	Incarnation uint64                  `json:"incarnation"`
	TraceID     string                  `json:"trace_id,omitempty"`
	// Origin time for convergence measurement
	OriginAt time.Time `json:"origin_at,omitempty"`
}

// Store is the AP gossip-replicated catalog.
type Store struct {
	mu          sync.Mutex
	local       *catalog.Store
	membership  gossip.Membership
	bus         *events.Bus
	watch       *watch.Registry
	clk         clock.Clock
	incarnation map[string]uint64 // per-instance origin incarnation
	// tombstones: instance ID → incarnation at deregister (monotone at equal incarnation)
	tombstones map[string]uint64
	// convergence tracking: traceID → set of nodes that have it
	converge map[string]map[string]time.Time
	self     string
	// pending full-state digests for anti-entropy overflow
	pendingFull bool
	stopCh      chan struct{}
	membershipCh chan gossip.MemberEvent
}

// Config for the gossip store.
type Config struct {
	Local      *catalog.Store
	Membership gossip.Membership
	Bus        *events.Bus
	Watch      *watch.Registry
	Clock      clock.Clock
}

// New creates an AP store and wires membership + broadcast handlers.
func New(cfg Config) *Store {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.New()
	}
	s := &Store{
		local:       cfg.Local,
		membership:  cfg.Membership,
		bus:         cfg.Bus,
		watch:       cfg.Watch,
		clk:         clk,
		incarnation: make(map[string]uint64),
		tombstones:  make(map[string]uint64),
		converge:    make(map[string]map[string]time.Time),
		self:        cfg.Membership.LocalName(),
		stopCh:      make(chan struct{}),
		membershipCh: make(chan gossip.MemberEvent, 64),
	}
	cfg.Membership.OnBroadcast(s.onBroadcast)
	cfg.Membership.Subscribe(s.membershipCh)
	go s.watchMembership()
	return s
}

// Close stops background goroutines.
func (s *Store) Close() {
	select {
	case <-s.stopCh:
		return
	default:
		close(s.stopCh)
		s.membership.Unsubscribe(s.membershipCh)
		close(s.membershipCh)
	}
}

func (s *Store) Mode() string { return "ap" }

func (s *Store) Register(ctx context.Context, inst *catalog.Instance) (uint64, error) {
	s.mu.Lock()
	s.incarnation[inst.ID]++
	inc := s.incarnation[inst.ID]
	inst.Incarnation = inc
	s.mu.Unlock()

	idx, err := s.local.Register(ctx, inst)
	if err != nil {
		return 0, err
	}
	d := Delta{
		Type:        gossip.DeltaRegister,
		Instance:    inst.Clone(),
		Index:       idx,
		Origin:      gossip.NodeID(s.self),
		Incarnation: inc,
		TraceID:     inst.TraceID,
		OriginAt:    s.clk.Now(),
	}
	s.broadcast(d)
	s.notifyWatch(inst.Service, "add", inst, idx, inst.TraceID)
	s.trackConverge(d.TraceID, s.self, d.OriginAt)
	return idx, nil
}

func (s *Store) Deregister(ctx context.Context, id string) (uint64, error) {
	inst, ok := s.local.GetInstance(id)
	svc := ""
	traceID := events.TraceFrom(ctx)
	if ok {
		svc = inst.Service
		if traceID == "" {
			traceID = inst.TraceID
		}
	}
	s.mu.Lock()
	if _, exists := s.incarnation[id]; exists {
		s.incarnation[id]++
	} else {
		// create tombstone even for unknown ID so deregister wins
		s.incarnation[id] = 1
		if tomb, ok := s.tombstones[id]; ok && tomb >= 1 {
			s.incarnation[id] = tomb + 1
		}
	}
	inc := s.incarnation[id]
	s.mu.Unlock()
	idx, err := s.local.Deregister(ctx, id)
	if err != nil {
		return 0, err
	}
	d := Delta{
		Type:        gossip.DeltaDeregister,
		InstanceID:  id,
		Instance:    inst,
		Index:       idx,
		Origin:      gossip.NodeID(s.self),
		Incarnation: inc,
		TraceID:     traceID,
		OriginAt:    s.clk.Now(),
	}
	s.broadcast(d)
	if svc != "" {
		s.notifyWatch(svc, "remove", inst, idx, traceID)
	}
	return idx, nil
}

func (s *Store) UpdateHealth(ctx context.Context, id string, h catalog.HealthStatus) (uint64, error) {
	// Check old health to avoid broadcasting when status unchanged (H1)
	var oldHealth catalog.HealthStatus
	if inst, ok := s.local.GetInstance(id); ok {
		oldHealth = inst.Health
	}
	idx, err := s.local.UpdateHealth(ctx, id, h)
	if err != nil {
		return 0, err
	}
	inst, ok := s.local.GetInstance(id)
	if !ok {
		return idx, nil
	}
	// Only broadcast if health actually changed (catalog no-bump invariant)
	if oldHealth == h {
		return idx, nil
	}
	s.mu.Lock()
	s.incarnation[id]++
	inc := s.incarnation[id]
	s.mu.Unlock()
	d := Delta{
		Type:        gossip.DeltaHealthChange,
		Instance:    inst,
		InstanceID:  id,
		Health:      h,
		Index:       idx,
		Origin:      gossip.NodeID(s.self),
		Incarnation: inc,
		TraceID:     events.TraceFrom(ctx),
		OriginAt:    s.clk.Now(),
	}
	s.broadcast(d)
	s.notifyWatch(inst.Service, "update", inst, idx, d.TraceID)
	return idx, nil
}

func (s *Store) Get(ctx context.Context, service string, opts catalog.QueryOptions) (*catalog.Result, error) {
	return s.local.Get(ctx, service, opts)
}
func (s *Store) GetNow(service string, opts catalog.QueryOptions) *catalog.Result {
	return s.local.GetNow(service, opts)
}
func (s *Store) GetInstance(id string) (*catalog.Instance, bool) {
	return s.local.GetInstance(id)
}
func (s *Store) InstancesOnNode(node string) []*catalog.Instance {
	return s.local.InstancesOnNode(node)
}
func (s *Store) ListServices() map[string][]string { return s.local.ListServices() }
func (s *Store) Index() uint64                     { return s.local.Index() }
func (s *Store) Snapshot() *catalog.Snapshot       { return s.local.Snapshot() }
func (s *Store) Restore(snap *catalog.Snapshot) error {
	return s.local.Restore(snap)
}

// ApplyDelta merges a remote delta. Higher incarnation wins; at equal
// incarnation, deregistration is monotone (not undone by a register).
func (s *Store) ApplyDelta(d Delta) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := d.InstanceID
	if id == "" && d.Instance != nil {
		id = d.Instance.ID
	}
	existingInc := s.incarnation[id]
	if tomb, ok := s.tombstones[id]; ok && tomb > existingInc {
		existingInc = tomb
	}
	if d.Incarnation < existingInc {
		return false // stale
	}
	if d.Incarnation == existingInc {
		// At equal incarnation, deregistration is monotone: a register cannot undo it.
		if d.Type == gossip.DeltaRegister {
			if _, tombed := s.tombstones[id]; tombed {
				return false
			}
			if inst, ok := s.local.GetInstance(id); ok && inst.Deregistered {
				return false
			}
		}
	}

	ctx := context.Background()
	if d.TraceID != "" {
		ctx = events.ContextWithTrace(ctx, d.TraceID)
	}

	applied := false
	switch d.Type {
	case gossip.DeltaRegister:
		if d.Instance != nil {
			if tomb, ok := s.tombstones[id]; ok && d.Incarnation <= tomb {
				return false
			}
			delete(s.tombstones, id)
			d.Instance.Incarnation = d.Incarnation
			_, _ = s.local.Register(ctx, d.Instance)
			s.incarnation[id] = d.Incarnation
			s.notifyWatch(d.Instance.Service, "add", d.Instance, d.Index, d.TraceID)
			applied = true
		}
	case gossip.DeltaDeregister:
		if id != "" {
			inst, _ := s.local.GetInstance(id)
			_, _ = s.local.Deregister(ctx, id)
			s.incarnation[id] = d.Incarnation
			s.tombstones[id] = d.Incarnation
			if inst != nil {
				s.notifyWatch(inst.Service, "remove", inst, d.Index, d.TraceID)
			}
			applied = true
		}
	case gossip.DeltaHealthChange:
		if id != "" {
			_, _ = s.local.UpdateHealth(ctx, id, d.Health)
			s.incarnation[id] = d.Incarnation
			if inst, ok := s.local.GetInstance(id); ok {
				s.notifyWatch(inst.Service, "update", inst, d.Index, d.TraceID)
			}
			applied = true
		}
	}

	if applied {
		if s.bus != nil {
			s.bus.Publish(events.Event{
				Kind:    events.EvGossipDelta,
				TraceID: d.TraceID,
				Node:    s.self,
				Index:   d.Index,
				Detail:  d.Type.String(),
			})
		}
		s.trackConvergeLocked(d.TraceID, s.self, s.clk.Now())
		// Memory fabric already full-meshes Broadcast. Multi-hop infection is
		// only needed on real SWIM transports; rebroadcast here would loop forever
		// because every peer re-applies with the same incarnation and re-sends.
	}
	return applied
}

func (s *Store) broadcast(d Delta) {
	payload, err := json.Marshal(d)
	if err != nil {
		return
	}
	if len(payload) > gossip.MaxPiggybackBytes {
		// overflow → anti-entropy path (not silently dropped)
		s.mu.Lock()
		s.pendingFull = true
		s.mu.Unlock()
		if s.bus != nil {
			s.bus.Publish(events.Event{
				Kind:   events.EvAntiEntropySync,
				Node:   s.self,
				Detail: "gossip payload >512, requires full sync",
				Meta:   map[string]any{"trace_id": d.TraceID, "instance": d.InstanceID},
			})
		}
		return
	}
	_ = s.membership.Broadcast(payload)
}

// NeedsFullSync reports whether a payload overflow requires anti-entropy.
func (s *Store) NeedsFullSync() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingFull
}

// ClearPendingFull clears the overflow flag after a successful FullSync/MerkleSync.
func (s *Store) ClearPendingFull() {
	s.mu.Lock()
	s.pendingFull = false
	s.mu.Unlock()
}

func (s *Store) onBroadcast(from gossip.NodeID, payload []byte) {
	var d Delta
	if err := json.Unmarshal(payload, &d); err != nil {
		return
	}
	_ = from
	s.ApplyDelta(d)
}

// watchMembership: node failure → all instances on that node critical immediately.
func (s *Store) watchMembership() {
	ch := s.membershipCh
	for {
		select {
		case <-s.stopCh:
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			switch ev.Type {
			case gossip.Failed, gossip.Leave:
				instances := s.local.InstancesOnNode(ev.Node.Name)
				for _, inst := range instances {
					// Critical, not deleted: node may return; keep registration + metadata.
					_, _ = s.local.UpdateHealth(context.Background(), inst.ID, catalog.HealthCritical)
				}
				if s.bus != nil {
					s.bus.Publish(events.Event{
						Kind:   events.EvNodeFailed,
						Node:   ev.Node.Name,
						Detail: "instances marked critical via gossip failure detection",
						Meta:   map[string]any{"instances_affected": len(instances)},
					})
				}
			case gossip.Join:
				// Restore last known health, then let checks re-verify. Do NOT assume passing.
				for _, inst := range s.local.InstancesOnNode(ev.Node.Name) {
					h := inst.LastKnownHealth
					if h == "" {
						h = catalog.HealthWarning
					}
					_, _ = s.local.UpdateHealth(context.Background(), inst.ID, h)
				}
				if s.bus != nil {
					s.bus.Publish(events.Event{Kind: events.EvNodeJoined, Node: ev.Node.Name})
				}
			}
		}
	}
}

func (s *Store) notifyWatch(service, kind string, inst *catalog.Instance, idx uint64, traceID string) {
	if s.watch == nil {
		return
	}
	var instances []*catalog.Instance
	if inst != nil {
		instances = []*catalog.Instance{inst}
	}
	s.watch.Notify(service, watch.Event{
		Kind:      kind,
		Service:   service,
		Instances: instances,
		Index:     idx,
		TraceID:   traceID,
	})
}

func (s *Store) trackConverge(traceID, node string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trackConvergeLocked(traceID, node, at)
}

func (s *Store) trackConvergeLocked(traceID, node string, at time.Time) {
	if traceID == "" {
		return
	}
	if s.converge[traceID] == nil {
		s.converge[traceID] = make(map[string]time.Time)
	}
	s.converge[traceID][node] = at
	// If all members have it, emit EvConverged.
	need := s.membership.Size()
	if need > 0 && len(s.converge[traceID]) >= need {
		var first, last time.Time
		for _, t := range s.converge[traceID] {
			if first.IsZero() || t.Before(first) {
				first = t
			}
			if last.IsZero() || t.After(last) {
				last = t
			}
		}
		if s.bus != nil {
			s.bus.Publish(events.Event{
				Kind:    events.EvConverged,
				TraceID: traceID,
				Elapsed: last.Sub(first),
				Detail:  "all members have delta",
			})
		}
	}
}

// FullSync exchanges full catalog digests (anti-entropy for missed deltas).
func (s *Store) FullSync(remote *catalog.Snapshot) error {
	if remote == nil {
		return nil
	}
	// Merge: for each remote instance, apply if newer incarnation.
	for _, inst := range remote.Instances {
		s.ApplyDelta(Delta{
			Type:        gossip.DeltaRegister,
			Instance:    inst,
			Incarnation: inst.Incarnation,
			Index:       inst.ModifyIndex,
		})
	}
	s.ClearPendingFull()
	return nil
}

// Local returns the underlying catalog.
func (s *Store) Local() *catalog.Store { return s.local }
