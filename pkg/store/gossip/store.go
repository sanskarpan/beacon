// Package gossip implements the AP catalog backend with SWIM-piggybacked deltas.
//
// When membership declares a node dead, every instance on that node is marked
// critical immediately (~2s via SWIM vs ~15s via health checks) — the highest-
// value integration with the existing gossip project.
package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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

type antiEntropyMessage struct {
	Kind      string        `json:"kind"`
	ID        string        `json:"id"`
	Requester gossip.NodeID `json:"requester,omitempty"`
	Provider  gossip.NodeID `json:"provider,omitempty"`
	Root      string        `json:"root,omitempty"`
	Count     int           `json:"count,omitempty"`
	Seq       int           `json:"seq,omitempty"`
	Total     int           `json:"total,omitempty"`
	Data      []byte        `json:"data,omitempty"`
}

type antiEntropyState struct {
	Instances  []*catalog.Instance `json:"instances"`
	Tombstones map[string]uint64   `json:"tombstones,omitempty"`
}

type antiEntropyTransfer struct {
	total  int
	chunks map[int][]byte
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
	pendingFull  bool
	lastIndex    map[gossip.NodeID]uint64
	pullID       string
	fetchSent    bool
	transfers    map[string]*antiEntropyTransfer
	aeSeq        uint64
	stopCh       chan struct{}
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
		local:        cfg.Local,
		membership:   cfg.Membership,
		bus:          cfg.Bus,
		watch:        cfg.Watch,
		clk:          clk,
		incarnation:  make(map[string]uint64),
		tombstones:   make(map[string]uint64),
		converge:     make(map[string]map[string]time.Time),
		self:         cfg.Membership.LocalName(),
		stopCh:       make(chan struct{}),
		membershipCh: make(chan gossip.MemberEvent, 64),
		lastIndex:    make(map[gossip.NodeID]uint64),
		transfers:    make(map[string]*antiEntropyTransfer),
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
		s.announceAntiEntropy()
		return
	}
	s.noteLocalIndex(d)
	_ = s.membership.Broadcast(payload)
}

// NeedsFullSync reports whether a payload overflow requires anti-entropy.
func (s *Store) NeedsFullSync() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingFull
}

// ClearPendingFull clears the overflow flag after a successful anti-entropy sync.
func (s *Store) ClearPendingFull() {
	s.mu.Lock()
	s.pendingFull = false
	s.mu.Unlock()
}

func (s *Store) onBroadcast(from gossip.NodeID, payload []byte) {
	var ae antiEntropyMessage
	if err := json.Unmarshal(payload, &ae); err == nil && ae.Kind != "" {
		s.handleAntiEntropy(from, ae)
		return
	}
	var d Delta
	if err := json.Unmarshal(payload, &d); err != nil {
		return
	}
	s.noteRemoteIndex(d)
	s.ApplyDelta(d)
}

func (s *Store) noteLocalIndex(d Delta) {
	if d.Origin == "" || d.Index == 0 {
		return
	}
	s.mu.Lock()
	if d.Index > s.lastIndex[d.Origin] {
		s.lastIndex[d.Origin] = d.Index
	}
	s.mu.Unlock()
}

func (s *Store) noteRemoteIndex(d Delta) {
	if d.Origin == "" || d.Index == 0 {
		return
	}
	s.mu.Lock()
	last := s.lastIndex[d.Origin]
	if d.Index > s.lastIndex[d.Origin] {
		s.lastIndex[d.Origin] = d.Index
	}
	gap := (last == 0 && d.Index > 1) || d.Index > last+1
	s.mu.Unlock()
	if gap {
		s.requestAntiEntropy()
	}
}

func (s *Store) nextAntiEntropyID() string {
	s.mu.Lock()
	s.aeSeq++
	id := fmt.Sprintf("%s/ae/%d", s.self, s.aeSeq)
	s.mu.Unlock()
	return id
}

func (s *Store) requestAntiEntropy() {
	s.mu.Lock()
	if s.pullID != "" {
		s.mu.Unlock()
		return
	}
	s.aeSeq++
	id := fmt.Sprintf("%s/pull/%d", s.self, s.aeSeq)
	s.pullID = id
	s.fetchSent = false
	s.pendingFull = true
	s.mu.Unlock()
	if !s.sendAntiEntropy(antiEntropyMessage{Kind: "ae_request", ID: id, Requester: gossip.NodeID(s.self)}) {
		s.mu.Lock()
		if s.pullID == id {
			s.pullID = ""
		}
		s.mu.Unlock()
	}
}

func (s *Store) announceAntiEntropy() {
	d := s.BuildDigest(false)
	_ = s.sendAntiEntropy(antiEntropyMessage{
		Kind: "ae_announce", ID: s.nextAntiEntropyID(), Provider: gossip.NodeID(s.self),
		Root: d.Root, Count: d.Count,
	})
}

func (s *Store) sendAntiEntropy(msg antiEntropyMessage) bool {
	payload, err := json.Marshal(msg)
	if err != nil || len(payload) > gossip.MaxPiggybackBytes {
		return false
	}
	return s.membership.Broadcast(payload) == nil
}

func (s *Store) handleAntiEntropy(from gossip.NodeID, msg antiEntropyMessage) {
	_ = from
	switch msg.Kind {
	case "ae_request":
		if msg.Requester == gossip.NodeID(s.self) {
			return
		}
		if !s.isAntiEntropyProvider(msg.Requester) {
			return
		}
		d := s.BuildDigest(false)
		_ = s.sendAntiEntropy(antiEntropyMessage{
			Kind: "ae_digest", ID: msg.ID, Requester: msg.Requester,
			Provider: gossip.NodeID(s.self), Root: d.Root, Count: d.Count,
		})
	case "ae_announce":
		if msg.Provider == gossip.NodeID(s.self) {
			return
		}
		local := s.BuildDigest(false)
		if local.Root == msg.Root && local.Count == msg.Count {
			return
		}
		s.mu.Lock()
		s.pendingFull = true
		s.mu.Unlock()
		_ = s.sendAntiEntropy(antiEntropyMessage{
			Kind: "ae_fetch", ID: msg.ID + "/" + s.self,
			Requester: gossip.NodeID(s.self), Provider: msg.Provider,
		})
	case "ae_digest":
		if msg.Requester != gossip.NodeID(s.self) || msg.Provider == gossip.NodeID(s.self) {
			return
		}
		d := s.BuildDigest(false)
		if d.Root == msg.Root && d.Count == msg.Count {
			s.finishPull(msg.ID)
			return
		}
		s.mu.Lock()
		if s.pullID != msg.ID || s.fetchSent {
			s.mu.Unlock()
			return
		}
		s.fetchSent = true
		s.mu.Unlock()
		_ = s.sendAntiEntropy(antiEntropyMessage{
			Kind: "ae_fetch", ID: msg.ID + "/" + s.self,
			Requester: gossip.NodeID(s.self), Provider: msg.Provider,
		})
	case "ae_fetch":
		if msg.Provider != gossip.NodeID(s.self) || msg.Requester == "" {
			return
		}
		s.sendAntiEntropyState(msg.ID, msg.Requester)
	case "ae_chunk":
		if msg.Requester != gossip.NodeID(s.self) || msg.Total <= 0 || msg.Seq < 0 || msg.Seq >= msg.Total {
			return
		}
		s.receiveAntiEntropyChunk(msg)
	}
}

func (s *Store) isAntiEntropyProvider(requester gossip.NodeID) bool {
	ids := make([]gossip.NodeID, 0)
	for _, member := range s.membership.Members() {
		if member.ID != "" && member.ID != requester && member.Status != gossip.StatusFailed && member.Status != gossip.StatusLeft {
			ids = append(ids, member.ID)
		}
	}
	if len(ids) == 0 {
		return true
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids[0] == gossip.NodeID(s.self)
}

func (s *Store) finishPull(id string) {
	s.mu.Lock()
	if s.pullID == id {
		s.pullID = ""
		s.fetchSent = false
		s.pendingFull = false
	}
	s.mu.Unlock()
}

func (s *Store) sendAntiEntropyState(id string, requester gossip.NodeID) {
	snap := s.local.Snapshot()
	instances := make([]*catalog.Instance, 0, len(snap.Instances))
	for _, inst := range snap.Instances {
		if inst != nil {
			instances = append(instances, inst.Clone())
		}
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].ID < instances[j].ID })
	s.mu.Lock()
	tombstones := make(map[string]uint64, len(s.tombstones))
	for k, v := range s.tombstones {
		tombstones[k] = v
	}
	s.mu.Unlock()
	raw, err := json.Marshal(antiEntropyState{Instances: instances, Tombstones: tombstones})
	if err != nil {
		return
	}
	const chunkBytes = 192
	total := (len(raw) + chunkBytes - 1) / chunkBytes
	if total == 0 {
		total = 1
	}
	for seq := 0; seq < total; seq++ {
		start := seq * chunkBytes
		end := start + chunkBytes
		if end > len(raw) {
			end = len(raw)
		}
		if !s.sendAntiEntropy(antiEntropyMessage{
			Kind: "ae_chunk", ID: id, Requester: requester,
			Provider: gossip.NodeID(s.self), Seq: seq, Total: total,
			Data: raw[start:end],
		}) {
			return
		}
	}
	s.ClearPendingFull()
}

func (s *Store) receiveAntiEntropyChunk(msg antiEntropyMessage) {
	s.mu.Lock()
	transfer := s.transfers[msg.ID]
	if transfer == nil {
		transfer = &antiEntropyTransfer{total: msg.Total, chunks: make(map[int][]byte, msg.Total)}
		s.transfers[msg.ID] = transfer
	}
	if transfer.total != msg.Total {
		s.mu.Unlock()
		return
	}
	if _, exists := transfer.chunks[msg.Seq]; exists {
		s.mu.Unlock()
		return
	}
	transfer.chunks[msg.Seq] = append([]byte(nil), msg.Data...)
	if len(transfer.chunks) != transfer.total {
		s.mu.Unlock()
		return
	}
	raw := make([]byte, 0)
	for seq := 0; seq < transfer.total; seq++ {
		chunk, ok := transfer.chunks[seq]
		if !ok {
			s.mu.Unlock()
			return
		}
		raw = append(raw, chunk...)
	}
	delete(s.transfers, msg.ID)
	if s.pullID != "" {
		s.pullID = ""
		s.fetchSent = false
	}
	s.mu.Unlock()
	var state antiEntropyState
	if json.Unmarshal(raw, &state) != nil {
		return
	}
	s.applyAntiEntropyState(state)
	s.ClearPendingFull()
}

func (s *Store) applyAntiEntropyState(state antiEntropyState) {
	for _, inst := range state.Instances {
		if inst == nil {
			continue
		}
		s.ApplyDelta(Delta{
			Type: gossip.DeltaRegister, Instance: inst.Clone(), InstanceID: inst.ID,
			Incarnation: inst.Incarnation, Index: inst.ModifyIndex, Health: inst.Health,
		})
	}
	ids := make([]string, 0, len(state.Tombstones))
	for id := range state.Tombstones {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		inst, _ := s.local.GetInstance(id)
		s.ApplyDelta(Delta{
			Type: gossip.DeltaDeregister, Instance: inst, InstanceID: id,
			Incarnation: state.Tombstones[id],
		})
	}
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
				// A join alone does not prove that a catalog gap exists. Triggering
				// a synchronous full-state exchange here can recursively flood the
				// in-memory transport during large cluster formation; gaps and
				// oversized deltas request anti-entropy explicitly below.
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
