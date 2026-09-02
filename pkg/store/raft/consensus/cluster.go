// Package consensus integrates the Raft-Consensus project as the CP catalog FSM.
//
// Catalog mutations are encoded as commands, submitted via raft.Apply, and applied
// deterministically (timestamps live inside the command; Apply never calls time.Now).
package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
	raftlib "github.com/sanskarpan/raft-consensus/pkg/raft"
)

var (
	globalNodesMu sync.RWMutex
	globalNodes   = map[string]*Node{}
)

func globalClusterNode(id string) *Node {
	globalNodesMu.RLock()
	n := globalNodes[id]
	globalNodesMu.RUnlock()
	return n
}

func registerGlobalNode(n *Node) {
	globalNodesMu.Lock()
	globalNodes[n.ID] = n
	globalNodesMu.Unlock()
}

func unregisterGlobalNode(id string) {
	globalNodesMu.Lock()
	delete(globalNodes, id)
	globalNodesMu.Unlock()
}

// CommandType for catalog log entries.
type CommandType int

const (
	CmdRegister CommandType = iota
	CmdDeregister
	CmdUpdateHealth
)

// Command is a replicated catalog mutation. Timestamps travel inside the
// command so Apply stays deterministic.
type Command struct {
	Type      CommandType          `json:"type"`
	Instance  *catalog.Instance    `json:"instance,omitempty"`
	ID        string               `json:"id,omitempty"`
	Health    catalog.HealthStatus `json:"health,omitempty"`
	Timestamp time.Time            `json:"timestamp"`
	TraceID   string               `json:"trace_id,omitempty"`
}

// CatalogFSM is the Raft state machine: catalog.Store.
type CatalogFSM struct {
	mu  sync.Mutex
	fsm *catalog.Store
	bus *events.Bus
	clk clock.Clock
}

// NewCatalogFSM creates an empty catalog FSM.
func NewCatalogFSM(clk clock.Clock, bus *events.Bus) *CatalogFSM {
	if clk == nil {
		clk = clock.New()
	}
	return &CatalogFSM{
		fsm: catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus)),
		bus: bus,
		clk: clk,
	}
}

// Apply implements raft.FSM. No time.Now — timestamps are in the command.
func (f *CatalogFSM) Apply(entry []byte) ([]byte, error) {
	var cmd Command
	if err := json.Unmarshal(entry, &cmd); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ctx := events.ContextWithTrace(context.Background(), cmd.TraceID)
	var idx uint64
	var err error
	switch cmd.Type {
	case CmdRegister:
		if cmd.Instance == nil {
			return nil, errors.New("register: nil instance")
		}
		// Preserve command timestamp in instance metadata for determinism audits.
		if cmd.Instance.Meta == nil {
			cmd.Instance.Meta = map[string]string{}
		}
		cmd.Instance.Meta["raft_ts"] = cmd.Timestamp.UTC().Format(time.RFC3339Nano)
		idx, err = f.fsm.Register(ctx, cmd.Instance)
	case CmdDeregister:
		idx, err = f.fsm.Deregister(ctx, cmd.ID)
	case CmdUpdateHealth:
		idx, err = f.fsm.UpdateHealth(ctx, cmd.ID, cmd.Health)
	default:
		return nil, fmt.Errorf("unknown command type %d", cmd.Type)
	}
	if err != nil {
		return nil, err
	}
	out, _ := json.Marshal(map[string]uint64{"index": idx})
	return out, nil
}

// Snapshot implements raft.FSM.
func (f *CatalogFSM) Snapshot() (raftlib.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	snap := f.fsm.Snapshot()
	b, err := json.Marshal(snap)
	if err != nil {
		return nil, err
	}
	return &bytesSnapshot{data: b, index: f.fsm.Index()}, nil
}

// Restore implements raft.FSM.
func (f *CatalogFSM) Restore(r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var snap catalog.Snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fsm.Restore(&snap)
}

// Store returns the underlying catalog (for reads).
func (f *CatalogFSM) Store() *catalog.Store {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fsm
}

type bytesSnapshot struct {
	data  []byte
	index uint64
}

func (s *bytesSnapshot) Index() uint64 { return s.index }
func (s *bytesSnapshot) Term() uint64  { return 0 }
func (s *bytesSnapshot) Reader() io.ReadCloser {
	return io.NopCloser(strings.NewReader(string(s.data)))
}

// --- in-process transport + stores (mirror raft-consensus test helpers) ---

type chanTransport struct {
	mu                sync.RWMutex
	localID           raftlib.ServerID
	peers             map[raftlib.ServerID]*chanTransport
	appendEntriesFn   func(req *raftlib.AppendEntriesRequest) *raftlib.AppendEntriesResponse
	requestVoteFn     func(req *raftlib.RequestVoteRequest) *raftlib.RequestVoteResponse
	installSnapshotFn func(req *raftlib.InstallSnapshotRequest) *raftlib.InstallSnapshotResponse
	drop              int32
	dropPeers         map[raftlib.ServerID]bool
}

func newChanTransport(id raftlib.ServerID) *chanTransport {
	return &chanTransport{localID: id, peers: make(map[raftlib.ServerID]*chanTransport), dropPeers: make(map[raftlib.ServerID]bool)}
}

func (t *chanTransport) SetLocalID(id raftlib.ServerID) {
	t.mu.Lock()
	t.localID = id
	t.mu.Unlock()
}

func (t *chanTransport) connect(other *chanTransport) {
	t.mu.Lock()
	t.peers[other.localID] = other
	t.mu.Unlock()
}

func (t *chanTransport) setDrop(v bool) {
	if v {
		atomic.StoreInt32(&t.drop, 1)
	} else {
		atomic.StoreInt32(&t.drop, 0)
	}
}

func (t *chanTransport) setDropPeer(target raftlib.ServerID, v bool) {
	t.mu.Lock()
	if v {
		t.dropPeers[target] = true
	} else {
		delete(t.dropPeers, target)
	}
	t.mu.Unlock()
}

func (t *chanTransport) isDropped(target raftlib.ServerID) bool {
	if atomic.LoadInt32(&t.drop) == 1 {
		return true
	}
	t.mu.RLock()
	dropped := t.dropPeers[target]
	t.mu.RUnlock()
	return dropped
}

func (t *chanTransport) AppendEntries(_ context.Context, target raftlib.ServerID, req *raftlib.AppendEntriesRequest) (*raftlib.AppendEntriesResponse, error) {
	if t.isDropped(target) {
		return nil, fmt.Errorf("network partition")
	}
	t.mu.RLock()
	peer, ok := t.peers[target]
	t.mu.RUnlock()
	if !ok || peer.appendEntriesFn == nil {
		return nil, fmt.Errorf("peer not found: %s", target)
	}
	return peer.appendEntriesFn(req), nil
}

func (t *chanTransport) RequestVote(_ context.Context, target raftlib.ServerID, req *raftlib.RequestVoteRequest) (*raftlib.RequestVoteResponse, error) {
	if t.isDropped(target) {
		return nil, fmt.Errorf("network partition")
	}
	t.mu.RLock()
	peer, ok := t.peers[target]
	t.mu.RUnlock()
	if !ok || peer.requestVoteFn == nil {
		return nil, fmt.Errorf("peer not found: %s", target)
	}
	return peer.requestVoteFn(req), nil
}

func (t *chanTransport) InstallSnapshot(_ context.Context, target raftlib.ServerID, req *raftlib.InstallSnapshotRequest) (*raftlib.InstallSnapshotResponse, error) {
	if t.isDropped(target) {
		return nil, fmt.Errorf("network partition")
	}
	t.mu.RLock()
	peer, ok := t.peers[target]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("peer not found: %s", target)
	}
	if peer.installSnapshotFn != nil {
		return peer.installSnapshotFn(req), nil
	}
	return &raftlib.InstallSnapshotResponse{}, nil
}

func (t *chanTransport) TimeoutNow(_ context.Context, _ raftlib.ServerID) error { return nil }
func (t *chanTransport) Close() error                                           { return nil }

type memLogStore struct {
	mu      sync.RWMutex
	entries map[uint64]*raftlib.LogEntry
	first   uint64
	last    uint64
}

func newMemLogStore() *memLogStore {
	return &memLogStore{entries: make(map[uint64]*raftlib.LogEntry)}
}

func (s *memLogStore) Append(entries []*raftlib.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		clone := e.Clone()
		s.entries[e.Index] = &clone
		if s.first == 0 || e.Index < s.first {
			s.first = e.Index
		}
		if e.Index > s.last {
			s.last = e.Index
		}
	}
	return nil
}

func (s *memLogStore) Get(idx uint64) (*raftlib.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[idx]
	if !ok {
		return nil, errors.New("not found")
	}
	clone := e.Clone()
	return &clone, nil
}

func (s *memLogStore) Iterate(start, stop uint64, f func(*raftlib.LogEntry) bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for idx := start; idx <= stop; idx++ {
		e, ok := s.entries[idx]
		if !ok {
			break
		}
		clone := e.Clone()
		if !f(&clone) {
			break
		}
	}
	return nil
}

func (s *memLogStore) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.first, nil
}
func (s *memLogStore) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.last, nil
}
func (s *memLogStore) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for idx := min; idx <= max; idx++ {
		delete(s.entries, idx)
	}
	if max >= s.last {
		s.last = 0
		if min > 1 {
			s.last = min - 1
		}
	}
	return nil
}
func (s *memLogStore) Close() error { return nil }

type memStableStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newMemStableStore() *memStableStore {
	return &memStableStore{data: make(map[string][]byte)}
}

func (s *memStableStore) Set(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	s.data[string(key)] = cp
	return nil
}
func (s *memStableStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	if !ok {
		return nil, nil
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, nil
}
func (s *memStableStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, string(key))
	return nil
}
func (s *memStableStore) Iterate(prefix []byte, f func(key, value []byte) bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.data {
		if strings.HasPrefix(k, string(prefix)) {
			if !f([]byte(k), v) {
				break
			}
		}
	}
	return nil
}
func (s *memStableStore) Sync() error  { return nil }
func (s *memStableStore) Close() error { return nil }

type memSnapshotStore struct{}

type noopSnap struct{}

func (n *noopSnap) Index() uint64         { return 0 }
func (n *noopSnap) Term() uint64          { return 0 }
func (n *noopSnap) Reader() io.ReadCloser { return io.NopCloser(strings.NewReader("")) }

type noopSink struct{ id string }

func (s *noopSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *noopSink) Close() error                { return nil }
func (s *noopSink) Cancel() error               { return nil }
func (s *noopSink) ID() string                  { return s.id }

func (m *memSnapshotStore) Create(_ raftlib.SnapshotVersion, index, term uint64, _ raftlib.Configuration) (raftlib.SnapshotSink, error) {
	return &noopSink{id: fmt.Sprintf("%d-%d", term, index)}, nil
}
func (m *memSnapshotStore) Open(id string) (raftlib.Snapshot, *raftlib.SnapshotMeta, error) {
	return &noopSnap{}, &raftlib.SnapshotMeta{ID: id}, nil
}
func (m *memSnapshotStore) List() ([]*raftlib.SnapshotMeta, error) { return nil, nil }
func (m *memSnapshotStore) Delete(string) error                    { return nil }

// Node is one CP participant backed by real Raft-Consensus.
type Node struct {
	ID   string
	Raft raftlib.Raft
	FSM  *CatalogFSM
	trans *chanTransport
	clk  clock.Clock
	bus  *events.Bus
}

// Cluster is a multi-node CP catalog cluster using Raft-Consensus.
type Cluster struct {
	mu    sync.Mutex
	nodes map[string]*Node
	clk   clock.Clock
	bus   *events.Bus
}

// NewCluster boots a real Raft cluster with CatalogFSM on each node.
// First leader is elected by the protocol (not assigned).
func NewCluster(ids []string, clk clock.Clock, bus *events.Bus) (*Cluster, error) {
	if clk == nil {
		clk = clock.New()
	}
	if len(ids) == 0 {
		return nil, errors.New("consensus: empty cluster")
	}
	// Stable order for configuration
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)

	cfg := raftlib.Configuration{}
	for _, id := range sorted {
		cfg.Servers = append(cfg.Servers, raftlib.Server{ID: raftlib.ServerID(id)})
	}

	c := &Cluster{
		nodes: make(map[string]*Node),
		clk:   clk,
		bus:   bus,
	}

	type built struct {
		id    string
		raft  raftlib.Raft
		fsm   *CatalogFSM
		trans *chanTransport
	}
	var builds []built

	for _, id := range sorted {
		trans := newChanTransport(raftlib.ServerID(id))
		fsm := NewCatalogFSM(clk, bus)
		r, err := raftlib.NewRaft(&raftlib.Config{
			LocalID:              raftlib.ServerID(id),
			ElectionTick:         5,
			HeartbeatTick:        1,
			InitialConfiguration: cfg,
		}, raftlib.ServerID(id), newMemLogStore(), newMemStableStore(), &memSnapshotStore{}, fsm, trans)
		if err != nil {
			return nil, err
		}
		// Wire RPC handlers via type assertion (same pattern as raftd).
		type rpcNode interface {
			HandleAppendEntriesRPC(*raftlib.AppendEntriesRequest) *raftlib.AppendEntriesResponse
			HandleRequestVoteRPC(*raftlib.RequestVoteRequest) *raftlib.RequestVoteResponse
			HandleInstallSnapshotRPC(*raftlib.InstallSnapshotRequest) *raftlib.InstallSnapshotResponse
		}
		rn := r.(rpcNode)
		trans.appendEntriesFn = func(req *raftlib.AppendEntriesRequest) *raftlib.AppendEntriesResponse {
			return rn.HandleAppendEntriesRPC(req)
		}
		trans.requestVoteFn = func(req *raftlib.RequestVoteRequest) *raftlib.RequestVoteResponse {
			return rn.HandleRequestVoteRPC(req)
		}
		trans.installSnapshotFn = func(req *raftlib.InstallSnapshotRequest) *raftlib.InstallSnapshotResponse {
			return rn.HandleInstallSnapshotRPC(req)
		}
		builds = append(builds, built{id: id, raft: r, fsm: fsm, trans: trans})
	}

	// Fully mesh transports.
	for i := range builds {
		for j := range builds {
			if i != j {
				builds[i].trans.connect(builds[j].trans)
			}
		}
	}

	for _, b := range builds {
		if err := b.raft.Start(); err != nil {
			return nil, err
		}
		n := &Node{
			ID:    b.id,
			Raft:  b.raft,
			FSM:   b.fsm,
			trans: b.trans,
			clk:   clk,
			bus:   bus,
		}
		c.nodes[b.id] = n
		registerGlobalNode(n)
	}
	return c, nil
}

// Shutdown stops all nodes.
func (c *Cluster) Shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		_ = n.Raft.Shutdown()
		unregisterGlobalNode(n.ID)
	}
}

// Node returns a node by id.
func (c *Cluster) Node(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Leader waits for a leader and returns it.
func (c *Cluster) Leader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, n := range c.nodes {
			if n.Raft.State() == raftlib.StateLeader {
				c.mu.Unlock()
				return n
			}
		}
		c.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// Partition isolates groupA from groupB (bidirectional per-peer drop).
// Intra-group traffic (b↔c inside majority) is preserved; only cross-group edges are dropped.
func (c *Cluster) Partition(groupA, groupB []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	setA := map[string]bool{}
	for _, id := range groupA {
		setA[id] = true
	}
	setB := map[string]bool{}
	for _, id := range groupB {
		setB[id] = true
	}
	for _, n := range c.nodes {
		for _, m := range c.nodes {
			if n.ID == m.ID {
				continue
			}
			cross := (setA[n.ID] && setB[m.ID]) || (setB[n.ID] && setA[m.ID])
			if cross {
				n.trans.setDropPeer(raftlib.ServerID(m.ID), true)
			}
		}
	}
}

// Heal clears all partitions.
func (c *Cluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		n.trans.setDrop(false)
		for _, m := range c.nodes {
			if n.ID == m.ID {
				continue
			}
			n.trans.setDropPeer(raftlib.ServerID(m.ID), false)
		}
	}
}

// Store is a CatalogStore bound to one consensus node.
type Store struct {
	node *Node
}

// NewStore wraps a node.
func NewStore(n *Node) *Store { return &Store{node: n} }

// Ensure Store implements store.CatalogStore.
var _ store.CatalogStore = (*Store)(nil)

func (s *Store) Mode() string { return "cp" }

func (s *Store) propose(ctx context.Context, cmd Command) (uint64, error) {
	if s.node.Raft.State() != raftlib.StateLeader {
		lid := string(s.node.Raft.Leader())
		if lid == "" || lid == s.node.ID {
			return 0, raftlib.ErrNotLeader
		}
		// Best-effort forward to leader if in same in-process cluster (test/sim).
		// In prod, follower returns ErrNotLeader with leader hint and client retries on leader.
		if leaderNode := globalClusterNode(lid); leaderNode != nil && leaderNode.Raft.State() == raftlib.StateLeader {
			b, err := json.Marshal(cmd)
			if err != nil {
				return 0, err
			}
			res, err := leaderNode.Raft.Apply(ctx, b)
			if err == nil {
				var out struct {
					Index uint64 `json:"index"`
				}
				if len(res) > 0 {
					_ = json.Unmarshal(res, &out)
				}
				return out.Index, nil
			}
		}
		return 0, fmt.Errorf("%w: leader is %s", raftlib.ErrNotLeader, lid)
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return 0, err
	}
	res, err := s.node.Raft.Apply(ctx, b)
	if err != nil {
		return 0, err
	}
	var out struct {
		Index uint64 `json:"index"`
	}
	if len(res) > 0 {
		_ = json.Unmarshal(res, &out)
	}
	return out.Index, nil
}

func (s *Store) Register(ctx context.Context, inst *catalog.Instance) (uint64, error) {
	return s.propose(ctx, Command{
		Type:      CmdRegister,
		Instance:  inst.Clone(),
		Timestamp: s.node.clk.Now(),
		TraceID:   events.TraceFrom(ctx),
	})
}

func (s *Store) Deregister(ctx context.Context, id string) (uint64, error) {
	return s.propose(ctx, Command{
		Type:      CmdDeregister,
		ID:        id,
		Timestamp: s.node.clk.Now(),
		TraceID:   events.TraceFrom(ctx),
	})
}

func (s *Store) UpdateHealth(ctx context.Context, id string, h catalog.HealthStatus) (uint64, error) {
	return s.propose(ctx, Command{
		Type:      CmdUpdateHealth,
		ID:        id,
		Health:    h,
		Timestamp: s.node.clk.Now(),
		TraceID:   events.TraceFrom(ctx),
	})
}

func (s *Store) Get(ctx context.Context, service string, opts catalog.QueryOptions) (*catalog.Result, error) {
	if opts.Consistent {
		if s.node.Raft.State() != raftlib.StateLeader {
			return nil, raftlib.ErrNotLeader
		}
		// ReadIndex linearizable barrier: confirms leadership and that our
		// state machine has applied up to the current commit index.
		if _, err := s.node.Raft.ReadIndex(ctx); err != nil {
			return nil, fmt.Errorf("readindex: %w", err)
		}
	}
	res, err := s.node.FSM.Store().Get(ctx, service, opts)
	if res != nil && s.node.Raft.State() != raftlib.StateLeader {
		res.Stale = true
	}
	return res, err
}

func (s *Store) GetNow(service string, opts catalog.QueryOptions) *catalog.Result {
	res := s.node.FSM.Store().GetNow(service, opts)
	if s.node.Raft.State() != raftlib.StateLeader {
		res.Stale = true
	}
	return res
}

func (s *Store) GetInstance(id string) (*catalog.Instance, bool) {
	return s.node.FSM.Store().GetInstance(id)
}
func (s *Store) InstancesOnNode(node string) []*catalog.Instance {
	return s.node.FSM.Store().InstancesOnNode(node)
}
func (s *Store) ListServices() map[string][]string { return s.node.FSM.Store().ListServices() }
func (s *Store) Index() uint64                     { return s.node.FSM.Store().Index() }
func (s *Store) Snapshot() *catalog.Snapshot       { return s.node.FSM.Store().Snapshot() }
func (s *Store) Restore(snap *catalog.Snapshot) error {
	return s.node.FSM.Store().Restore(snap)
}
