// Package raft implements the CP catalog backend.
//
// This is a single-process Raft-style replicated log with leader election
// simulation for tests and the consistency lab. Production would plug in the
// Raft-Consensus project as the state machine; here we preserve the semantics:
//  - writes go to the leader
//  - Apply is deterministic (no time.Now in Apply — timestamps in commands)
//  - partition → minority rejects writes
//  - linearizable reads via leader; stale reads anywhere with age header
package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
)

// CommandType for log entries.
type CommandType int

const (
	CmdRegister CommandType = iota
	CmdDeregister
	CmdUpdateHealth
)

// Command is a replicated log entry. Timestamps travel inside the command
// so Apply stays deterministic.
type Command struct {
	Type      CommandType           `json:"type"`
	Instance  *catalog.Instance     `json:"instance,omitempty"`
	ID        string                `json:"id,omitempty"`
	Health    catalog.HealthStatus  `json:"health,omitempty"`
	Timestamp time.Time             `json:"timestamp"`
	TraceID   string                `json:"trace_id,omitempty"`
}

// Node is one Raft participant.
type Node struct {
	mu          sync.Mutex
	id          string
	fsm         *catalog.Store
	log         []Command
	leader      string
	peers       map[string]*Node
	partitioned map[string]bool // peers we cannot reach
	bus         *events.Bus
	clk         clock.Clock
	term        uint64
	// last contact with leader for stale age
	lastContact time.Time
}

// Cluster is a Raft cluster for CP mode.
type Cluster struct {
	mu    sync.Mutex
	nodes map[string]*Node
	clk   clock.Clock
	bus   *events.Bus
}

// NewCluster boots a CP cluster; first node is leader.
func NewCluster(ids []string, clk clock.Clock, bus *events.Bus) *Cluster {
	if clk == nil {
		clk = clock.New()
	}
	c := &Cluster{
		nodes: make(map[string]*Node),
		clk:   clk,
		bus:   bus,
	}
	for _, id := range ids {
		n := &Node{
			id:          id,
			fsm:         catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus)),
			peers:       make(map[string]*Node),
			partitioned: make(map[string]bool),
			bus:         bus,
			clk:         clk,
			lastContact: clk.Now(),
		}
		c.nodes[id] = n
	}
	// wire peers
	for _, n := range c.nodes {
		for id, p := range c.nodes {
			if id != n.id {
				n.peers[id] = p
			}
		}
	}
	// elect first as leader
	if len(ids) > 0 {
		c.Elect(ids[0])
	}
	return c
}

// Elect sets the leader.
func (c *Cluster) Elect(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		n.mu.Lock()
		n.leader = id
		n.term++
		n.mu.Unlock()
	}
}

// Node returns a node by id.
func (c *Cluster) Node(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Partition splits the cluster: group A cannot talk to group B.
func (c *Cluster) Partition(a, b []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	setA := map[string]bool{}
	for _, x := range a {
		setA[x] = true
	}
	setB := map[string]bool{}
	for _, x := range b {
		setB[x] = true
	}
	for _, n := range c.nodes {
		n.mu.Lock()
		for peer := range n.peers {
			if (setA[n.id] && setB[peer]) || (setB[n.id] && setA[peer]) {
				n.partitioned[peer] = true
			}
		}
		n.mu.Unlock()
	}
}

// Heal clears partitions and syncs followers from leader.
func (c *Cluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	var leader *Node
	for _, n := range c.nodes {
		n.mu.Lock()
		n.partitioned = make(map[string]bool)
		if n.leader == n.id {
			leader = n
		}
		n.mu.Unlock()
	}
	if leader == nil {
		return
	}
	snap := leader.fsm.Snapshot()
	for _, n := range c.nodes {
		if n == leader {
			continue
		}
		_ = n.fsm.Restore(snap)
		n.mu.Lock()
		n.log = append([]Command(nil), leader.log...)
		n.lastContact = c.clk.Now()
		n.mu.Unlock()
	}
}

// ErrNotLeader is returned when a follower receives a write without forwarding.
var ErrNotLeader = errors.New("raft: not leader")

// ErrNoQuorum when minority cannot commit.
var ErrNoQuorum = errors.New("raft: no quorum")

// Store is a CatalogStore facade bound to one Raft node.
type Store struct {
	node *Node
}

// NewStore wraps a node.
func NewStore(n *Node) *Store { return &Store{node: n} }

func (s *Store) Mode() string { return "cp" }

func (s *Store) Register(ctx context.Context, inst *catalog.Instance) (uint64, error) {
	cmd := Command{
		Type:      CmdRegister,
		Instance:  inst.Clone(),
		Timestamp: s.node.clk.Now(),
		TraceID:   events.TraceFrom(ctx),
	}
	return s.node.propose(cmd)
}

func (s *Store) Deregister(ctx context.Context, id string) (uint64, error) {
	cmd := Command{
		Type:      CmdDeregister,
		ID:        id,
		Timestamp: s.node.clk.Now(),
		TraceID:   events.TraceFrom(ctx),
	}
	return s.node.propose(cmd)
}

func (s *Store) UpdateHealth(ctx context.Context, id string, h catalog.HealthStatus) (uint64, error) {
	cmd := Command{
		Type:      CmdUpdateHealth,
		ID:        id,
		Health:    h,
		Timestamp: s.node.clk.Now(),
		TraceID:   events.TraceFrom(ctx),
	}
	return s.node.propose(cmd)
}

func (s *Store) Get(ctx context.Context, service string, opts catalog.QueryOptions) (*catalog.Result, error) {
	if opts.Consistent {
		if !s.node.isLeader() {
			// forward to leader
			leader := s.node.leaderNode()
			if leader == nil {
				return nil, ErrNotLeader
			}
			return leader.fsm.Get(ctx, service, opts)
		}
	}
	res, err := s.node.fsm.Get(ctx, service, opts)
	if res != nil && !s.node.isLeader() {
		res.Stale = true
		s.node.mu.Lock()
		res.LastContact = s.node.clk.Now().Sub(s.node.lastContact)
		s.node.mu.Unlock()
	}
	return res, err
}

func (s *Store) GetNow(service string, opts catalog.QueryOptions) *catalog.Result {
	res := s.node.fsm.GetNow(service, opts)
	if !s.node.isLeader() {
		res.Stale = true
		s.node.mu.Lock()
		res.LastContact = s.node.clk.Now().Sub(s.node.lastContact)
		s.node.mu.Unlock()
	}
	return res
}

func (s *Store) GetInstance(id string) (*catalog.Instance, bool) {
	return s.node.fsm.GetInstance(id)
}
func (s *Store) InstancesOnNode(node string) []*catalog.Instance {
	return s.node.fsm.InstancesOnNode(node)
}
func (s *Store) ListServices() map[string][]string { return s.node.fsm.ListServices() }
func (s *Store) Index() uint64                     { return s.node.fsm.Index() }
func (s *Store) Snapshot() *catalog.Snapshot       { return s.node.fsm.Snapshot() }
func (s *Store) Restore(snap *catalog.Snapshot) error {
	return s.node.fsm.Restore(snap)
}

// FSM exposes the underlying catalog for tests.
func (s *Store) FSM() *catalog.Store { return s.node.fsm }

func (n *Node) isLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader == n.id
}

func (n *Node) leaderNode() *Node {
	n.mu.Lock()
	lid := n.leader
	n.mu.Unlock()
	if lid == n.id {
		return n
	}
	n.mu.Lock()
	p := n.peers[lid]
	blocked := n.partitioned[lid]
	n.mu.Unlock()
	if blocked {
		return nil
	}
	return p
}

func (n *Node) propose(cmd Command) (uint64, error) {
	if !n.isLeader() {
		leader := n.leaderNode()
		if leader == nil {
			return 0, fmt.Errorf("%w: cannot reach leader", ErrNotLeader)
		}
		return leader.propose(cmd)
	}

	// Quorum: need majority of peers reachable + self.
	n.mu.Lock()
	total := 1 + len(n.peers)
	reachable := 1
	for id := range n.peers {
		if !n.partitioned[id] {
			reachable++
		}
	}
	n.mu.Unlock()
	if reachable*2 <= total {
		return 0, ErrNoQuorum
	}

	// Append and apply on leader, then replicate.
	n.mu.Lock()
	n.log = append(n.log, cmd)
	n.mu.Unlock()

	idx, err := n.apply(cmd)
	if err != nil {
		return 0, err
	}

	// Replicate to reachable followers (sorted peer ids for determinism).
	n.mu.Lock()
	peerIDs := make([]string, 0, len(n.peers))
	for id := range n.peers {
		if !n.partitioned[id] {
			peerIDs = append(peerIDs, id)
		}
	}
	n.mu.Unlock()
	sort.Strings(peerIDs)
	for _, id := range peerIDs {
		n.mu.Lock()
		p := n.peers[id]
		n.mu.Unlock()
		p.appendAndApply(cmd)
	}
	return idx, nil
}

func (n *Node) appendAndApply(cmd Command) {
	n.mu.Lock()
	n.log = append(n.log, cmd)
	n.lastContact = n.clk.Now()
	n.mu.Unlock()
	_, _ = n.apply(cmd)
}

func (n *Node) apply(cmd Command) (uint64, error) {
	ctx := context.Background()
	if cmd.TraceID != "" {
		ctx = events.ContextWithTrace(ctx, cmd.TraceID)
	}
	switch cmd.Type {
	case CmdRegister:
		return n.fsm.Register(ctx, cmd.Instance)
	case CmdDeregister:
		return n.fsm.Deregister(ctx, cmd.ID)
	case CmdUpdateHealth:
		return n.fsm.UpdateHealth(ctx, cmd.ID, cmd.Health)
	default:
		b, _ := json.Marshal(cmd)
		return 0, fmt.Errorf("unknown command: %s", b)
	}
}
