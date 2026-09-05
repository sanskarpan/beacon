package gossip

import (
	"sort"
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
	seen     map[string]time.Time
	cluster  *Cluster
	clk      clock.Clock
	left     bool
}

// NetworkConfig models transport effects for sim/tests (latency, loss, fanout, reorder).
type NetworkConfig struct {
	Latency time.Duration
	Loss    float64 // 0..1
	Fanout  int     // 0 = full mesh, else bounded infection fanout
	Reorder bool
	TTL     int
}

// Cluster is a shared fabric that connects MemoryMembership instances.
type Cluster struct {
	mu    sync.RWMutex
	nodes map[string]*MemoryMembership // by name
	// optional network effects
	partition      map[string]map[string]bool // from -> to blocked
	latency        time.Duration
	clk            clock.Clock
	netCfg         NetworkConfig
	deliveredBytes int64
	sentBytes      int64
	maxHop         int
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
		seen:    make(map[string]time.Time),
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
	m.cluster.deliverBroadcast(m, broadcastMessage{
		ID:      newNonce(),
		Origin:  m.self.ID,
		TTL:     m.cluster.broadcastTTL(),
		Payload: append([]byte(nil), payload...),
	})
	return nil
}

func (m *MemoryMembership) OnBroadcast(fn func(from NodeID, payload []byte)) {
	m.mu.Lock()
	m.handlers = append(m.handlers, fn)
	m.mu.Unlock()
}

// SetNetwork updates transport modeling.
func (c *Cluster) SetNetwork(cfg NetworkConfig) {
	c.mu.Lock()
	c.netCfg = cfg
	c.latency = cfg.Latency
	c.mu.Unlock()
}

func (m *MemoryMembership) mergeFrom(list []Member) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, other := range list {
		cur, ok := m.members[other.ID]
		if !ok || other.Incarnation > cur.Incarnation {
			m.members[other.ID] = other
			//nolint:staticcheck // SA9003: intentionally no per-member notify here; membership events flow through broadcastEvent/applyMember to avoid lock-held fan-out.
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

type broadcastMessage struct {
	ID      string
	Origin  NodeID
	TTL     int
	Hops    int
	Payload []byte
}

func (m *MemoryMembership) handleBroadcast(msg broadcastMessage, previous NodeID) bool {
	if !m.markSeen(msg.ID) {
		return false
	}
	m.mu.RLock()
	handlers := append([]func(NodeID, []byte){}, m.handlers...)
	m.mu.RUnlock()
	for _, h := range handlers {
		h(msg.Origin, append([]byte(nil), msg.Payload...))
	}
	if msg.TTL > 1 {
		m.cluster.forwardBroadcast(m, previous, msg)
	}
	return true
}

func (m *MemoryMembership) markSeen(id string) bool {
	m.mu.Lock()
	if _, ok := m.seen[id]; ok {
		m.mu.Unlock()
		return false
	}
	m.seen[id] = m.clk.Now()
	if len(m.seen) > 4096 {
		cutoff := m.clk.Now().Add(-10 * time.Minute)
		for id, at := range m.seen {
			if at.Before(cutoff) {
				delete(m.seen, id)
			}
		}
		for len(m.seen) > 4096 {
			var oldestID string
			var oldest time.Time
			for id, at := range m.seen {
				if oldestID == "" || at.Before(oldest) {
					oldestID, oldest = id, at
				}
			}
			delete(m.seen, oldestID)
		}
	}
	m.mu.Unlock()
	return true
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
	return c.blockedLocked(from, to)
}

func (c *Cluster) blockedLocked(from, to string) bool {
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

func (c *Cluster) DeliveredBytes() int64 { c.mu.RLock(); defer c.mu.RUnlock(); return c.deliveredBytes }
func (c *Cluster) SentBytes() int64      { c.mu.RLock(); defer c.mu.RUnlock(); return c.sentBytes }
func (c *Cluster) MaxHop() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.maxHop == 0 {
		return 1
	}
	return c.maxHop
}

func (c *Cluster) broadcastTTL() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.netCfg.Fanout == 0 {
		return 1
	}
	if c.netCfg.TTL > 0 {
		return c.netCfg.TTL
	}
	return 16
}

func (c *Cluster) deliverBroadcast(origin *MemoryMembership, msg broadcastMessage) {
	if !origin.markSeen(msg.ID) {
		return
	}
	c.forwardBroadcast(origin, "", msg)
}

func (c *Cluster) forwardBroadcast(sender *MemoryMembership, previous NodeID, msg broadcastMessage) {
	c.mu.RLock()
	cfg := c.netCfg
	targets := c.broadcastTargetsLocked(sender, previous, msg, cfg.Fanout)
	clk := c.clk
	latency := cfg.Latency
	reorder := cfg.Reorder
	c.mu.RUnlock()
	for idx, t := range targets {
		t := t
		if cfg.Loss > 0 && c.dropped(msg.Payload, t.self.Name, cfg.Loss) {
			continue
		}
		delay := latency
		if reorder && latency > 10*time.Millisecond {
			delay = latency/2 + time.Duration(idx*7%int(latency/2+1))
		}
		next := msg
		next.TTL--
		next.Hops++
		c.mu.Lock()
		c.sentBytes += int64(len(msg.Payload))
		c.mu.Unlock()
		deliver := func() {
			if t.handleBroadcast(next, sender.self.ID) {
				c.recordDelivery(next)
			}
		}
		if delay > 0 && clk != nil {
			clk.AfterFunc(delay, deliver)
		} else {
			deliver()
		}
	}
}

func (c *Cluster) broadcastTargetsLocked(sender *MemoryMembership, previous NodeID, msg broadcastMessage, fanout int) []*MemoryMembership {
	names := make([]string, 0, len(c.nodes))
	for name := range c.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	if fanout == 0 {
		out := make([]*MemoryMembership, 0, len(names))
		for _, name := range names {
			if name == sender.self.Name || name == string(previous) || c.blockedLocked(sender.self.Name, name) {
				continue
			}
			out = append(out, c.nodes[name])
		}
		return out
	}
	if fanout < 1 {
		fanout = 1
	}
	origin := 0
	for i, name := range names {
		if name == string(msg.Origin) {
			origin = i
			break
		}
	}
	current := 0
	for i, name := range names {
		if name == sender.self.Name {
			current = (i - origin + len(names)) % len(names)
			break
		}
	}
	out := make([]*MemoryMembership, 0, fanout)
	for child := current*fanout + 1; child < len(names) && len(out) < fanout; child++ {
		name := names[(origin+child)%len(names)]
		if name == sender.self.Name || name == string(previous) || c.blockedLocked(sender.self.Name, name) {
			continue
		}
		out = append(out, c.nodes[name])
	}
	return out
}

func (c *Cluster) dropped(payload []byte, name string, loss float64) bool {
	h := 0
	for _, b := range payload {
		h = h*31 + int(b)
	}
	h += len(name)
	return float64(h%100)/100.0 < loss
}

func (c *Cluster) recordDelivery(msg broadcastMessage) {
	c.mu.Lock()
	c.deliveredBytes += int64(len(msg.Payload))
	if msg.Hops > c.maxHop {
		c.maxHop = msg.Hops
	}
	c.mu.Unlock()
}

// ErrPayloadTooLarge is returned when a piggyback frame exceeds MaxPiggybackBytes.
var ErrPayloadTooLarge = errPayload("gossip: piggyback payload exceeds MaxPiggybackBytes")

type errPayload string

func (e errPayload) Error() string { return string(e) }
