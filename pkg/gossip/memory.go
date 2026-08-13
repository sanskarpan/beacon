package gossip

import (
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
)

// MemoryMembership is an in-process membership implementation for tests and sim.
// It models SWIM-like join/leave/fail and broadcasts payloads to all peers that
// share the same Cluster fabric.
type MemoryMembership struct {
	mu       sync.RWMutex
	self     Member
	members  map[NodeID]Member
	subs     map[chan<- MemberEvent]struct{}
	handlers []func(from NodeID, payload []byte)
	cluster  *Cluster
	clk      clock.Clock
	left     bool
}

// Cluster is a shared fabric that connects MemoryMembership instances.
type Cluster struct {
	mu    sync.RWMutex
	nodes map[string]*MemoryMembership // by name
	// optional network effects
	partition map[string]map[string]bool // from -> to blocked
	latency   time.Duration
	clk       clock.Clock
}

// NewCluster creates an empty gossip fabric.
func NewCluster(clk clock.Clock) *Cluster {
	if clk == nil {
		clk = clock.New()
	}
	return &Cluster{
		nodes:     make(map[string]*MemoryMembership),
		partition: make(map[string]map[string]bool),
		clk:       clk,
	}
}

// NewMemory creates a membership node and registers it with the cluster.
func NewMemory(cluster *Cluster, name, addr string, port int) *MemoryMembership {
	m := &MemoryMembership{
		self: Member{
			ID:     NodeID(name),
			Name:   name,
			Addr:   addr,
			Port:   port,
			Status: StatusAlive,
			Meta:   map[string]string{},
		},
		members: make(map[NodeID]Member),
		subs:    make(map[chan<- MemberEvent]struct{}),
		cluster: cluster,
		clk:     cluster.clk,
	}
	m.members[m.self.ID] = m.self
	cluster.mu.Lock()
	cluster.nodes[name] = m
	cluster.mu.Unlock()
	return m
}

func (m *MemoryMembership) Members() []Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Member, 0, len(m.members))
	for _, v := range m.members {
		out = append(out, v)
	}
	return out
}

func (m *MemoryMembership) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.members)
}

func (m *MemoryMembership) LocalName() string { return m.self.Name }

func (m *MemoryMembership) Join(seeds []string) (int, error) {
	reached := 0
	for _, s := range seeds {
		m.cluster.mu.RLock()
		peer, ok := m.cluster.nodes[s]
		m.cluster.mu.RUnlock()
		if !ok || m.cluster.blocked(m.self.Name, s) {
			continue
		}
		// Exchange full member lists.
		m.mergeFrom(peer.Members())
		peer.mergeFrom(m.Members())
		// Announce join.
		m.cluster.broadcastEvent(m, MemberEvent{Type: Join, Node: m.self, At: m.clk.Now()})
		reached++
	}
	return reached, nil
}

func (m *MemoryMembership) Leave() error {
	m.mu.Lock()
	m.left = true
	m.self.Status = StatusLeft
	m.self.Incarnation++
	self := m.self
	m.members[self.ID] = self
	m.mu.Unlock()
	m.cluster.broadcastEvent(m, MemberEvent{Type: Leave, Node: self, At: m.clk.Now()})
	return nil
}

// Fail simulates the failure detector declaring this node dead (from outside).
func (m *MemoryMembership) Fail() {
	m.mu.Lock()
	m.self.Status = StatusFailed
	m.self.Incarnation++
	self := m.self
	m.members[self.ID] = self
	m.mu.Unlock()
	m.cluster.broadcastEvent(m, MemberEvent{Type: Failed, Node: self, At: m.clk.Now()})
}

// Revive brings a failed node back.
func (m *MemoryMembership) Revive() {
	m.mu.Lock()
	m.self.Status = StatusAlive
	m.self.Incarnation++
	m.left = false
	self := m.self
	m.members[self.ID] = self
	m.mu.Unlock()
	m.cluster.broadcastEvent(m, MemberEvent{Type: Join, Node: self, At: m.clk.Now()})
}

func (m *MemoryMembership) Subscribe(ch chan<- MemberEvent) {
	m.mu.Lock()
	m.subs[ch] = struct{}{}
	m.mu.Unlock()
}

func (m *MemoryMembership) Unsubscribe(ch chan<- MemberEvent) {
	m.mu.Lock()
	delete(m.subs, ch)
	m.mu.Unlock()
}

func (m *MemoryMembership) Broadcast(payload []byte) error {
	if len(payload) > MaxPiggybackBytes {
		// Callers should fall back to anti-entropy; we still deliver truncated? No — reject.
		return ErrPayloadTooLarge
	}
	m.cluster.deliverBroadcast(m, payload)
	return nil
}

func (m *MemoryMembership) OnBroadcast(fn func(from NodeID, payload []byte)) {
	m.mu.Lock()
	m.handlers = append(m.handlers, fn)
	m.mu.Unlock()
}

func (m *MemoryMembership) mergeFrom(list []Member) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, other := range list {
		cur, ok := m.members[other.ID]
		if !ok || other.Incarnation > cur.Incarnation {
			m.members[other.ID] = other
			if other.ID != m.self.ID {
				// notify join/update asynchronously outside? emit under lock carefully
			}
		}
	}
}

func (m *MemoryMembership) applyMember(ev MemberEvent) {
	m.mu.Lock()
	cur, ok := m.members[ev.Node.ID]
	if ok && ev.Node.Incarnation < cur.Incarnation {
		m.mu.Unlock()
		return
	}
	m.members[ev.Node.ID] = ev.Node
	subs := make([]chan<- MemberEvent, 0, len(m.subs))
	for ch := range m.subs {
		subs = append(subs, ch)
	}
	m.mu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (m *MemoryMembership) handleBroadcast(from NodeID, payload []byte) {
	m.mu.RLock()
	handlers := append([]func(NodeID, []byte){}, m.handlers...)
	m.mu.RUnlock()
	for _, h := range handlers {
		h(from, payload)
	}
}

// Partition isolates two groups: nodes in a cannot talk to nodes in b (bidirectional).
func (c *Cluster) Partition(a, b []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, x := range a {
		if c.partition[x] == nil {
			c.partition[x] = make(map[string]bool)
		}
		for _, y := range b {
			c.partition[x][y] = true
			if c.partition[y] == nil {
				c.partition[y] = make(map[string]bool)
			}
			c.partition[y][x] = true
		}
	}
}

// Heal clears all partitions.
func (c *Cluster) Heal() {
	c.mu.Lock()
	c.partition = make(map[string]map[string]bool)
	// full mesh anti-entropy of membership
	nodes := make([]*MemoryMembership, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()
	for _, n := range nodes {
		for _, o := range nodes {
			if n == o {
				continue
			}
			n.mergeFrom(o.Members())
		}
	}
}

func (c *Cluster) blocked(from, to string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if m, ok := c.partition[from]; ok && m[to] {
		return true
	}
	return false
}

func (c *Cluster) broadcastEvent(origin *MemoryMembership, ev MemberEvent) {
	c.mu.RLock()
	targets := make([]*MemoryMembership, 0, len(c.nodes))
	for name, n := range c.nodes {
		if name == origin.self.Name {
			continue
		}
		if c.blocked(origin.self.Name, name) {
			continue
		}
		targets = append(targets, n)
	}
	c.mu.RUnlock()
	// Origin also sees its own events for local subscribers.
	origin.applyMember(ev)
	for _, t := range targets {
		t.applyMember(ev)
	}
}

func (c *Cluster) deliverBroadcast(origin *MemoryMembership, payload []byte) {
	c.mu.RLock()
	targets := make([]*MemoryMembership, 0, len(c.nodes))
	for name, n := range c.nodes {
		if name == origin.self.Name {
			continue
		}
		if c.blocked(origin.self.Name, name) {
			continue
		}
		targets = append(targets, n)
	}
	from := origin.self.ID
	c.mu.RUnlock()
	for _, t := range targets {
		t.handleBroadcast(from, payload)
	}
}

// ErrPayloadTooLarge is returned when a piggyback frame exceeds MaxPiggybackBytes.
var ErrPayloadTooLarge = errPayload("gossip: piggyback payload exceeds MaxPiggybackBytes")

type errPayload string

func (e errPayload) Error() string { return string(e) }
