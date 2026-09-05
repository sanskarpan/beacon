package gossip

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
)

const (
	defaultProbeInterval = time.Second
	defaultProbeTimeout  = 500 * time.Millisecond
	defaultFailureAfter  = 3
	maxUDPFrame          = 1400
)

// UDPConfig configures the network membership implementation. BindAddr is a
// local interface (usually 0.0.0.0); AdvertiseAddr is the address peers use.
type UDPConfig struct {
	Name          string
	BindAddr      string
	AdvertiseAddr string
	Port          int
	Clock         clock.Clock
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration
	FailureAfter  int
	Fanout        int // 0 = default bounded fanout
	TTL           int // 0 = default bounded gossip TTL
}

// UDP is a network-backed membership and bounded gossip transport. It keeps
// the Beacon interface independent from a particular SWIM implementation while
// providing real cross-process discovery for the production binary.
type UDP struct {
	mu        sync.RWMutex
	self      Member
	members   map[NodeID]Member
	peers     map[NodeID]*net.UDPAddr
	lastSeen  map[NodeID]time.Time
	subs      map[chan<- MemberEvent]struct{}
	handlers  []func(from NodeID, payload []byte)
	seen      map[string]time.Time
	fanout    int
	ttl       int
	pending   map[string]chan bool
	conn      *net.UDPConn
	clk       clock.Clock
	interval  time.Duration
	timeout   time.Duration
	failAfter int
	stop      chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
}

var _ Membership = (*UDP)(nil)

// NewUDP starts a network membership node.
func NewUDP(cfg UDPConfig) (*UDP, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("gossip: node name is required")
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.New()
	}
	if cfg.BindAddr == "" {
		cfg.BindAddr = "0.0.0.0"
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = cfg.BindAddr
		if cfg.AdvertiseAddr == "0.0.0.0" {
			cfg.AdvertiseAddr = "127.0.0.1"
		}
	}
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = defaultProbeInterval
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = defaultProbeTimeout
	}
	if cfg.FailureAfter <= 0 {
		cfg.FailureAfter = defaultFailureAfter
	}
	if cfg.Fanout <= 0 {
		cfg.Fanout = 3
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 16
	}
	addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(cfg.BindAddr, strconv.Itoa(cfg.Port)))
	if err != nil {
		return nil, fmt.Errorf("gossip: resolve bind address: %w", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("gossip: listen %s: %w", addr, err)
	}
	actual := conn.LocalAddr().(*net.UDPAddr)
	u := &UDP{
		self:    Member{ID: NodeID(cfg.Name), Name: cfg.Name, Addr: cfg.AdvertiseAddr, Port: actual.Port, Status: StatusAlive, Meta: map[string]string{}},
		members: make(map[NodeID]Member), peers: make(map[NodeID]*net.UDPAddr),
		lastSeen: make(map[NodeID]time.Time), subs: make(map[chan<- MemberEvent]struct{}),
		seen: make(map[string]time.Time), fanout: cfg.Fanout, ttl: cfg.TTL,
		pending: make(map[string]chan bool), conn: conn, clk: cfg.Clock,
		interval: cfg.ProbeInterval, timeout: cfg.ProbeTimeout, failAfter: cfg.FailureAfter,
		stop: make(chan struct{}),
	}
	u.members[u.self.ID] = u.self
	u.lastSeen[u.self.ID] = cfg.Clock.Now()
	u.wg.Add(2)
	go u.readLoop()
	go u.probeLoop()
	return u, nil
}

func (u *UDP) Members() []Member {
	u.mu.RLock()
	defer u.mu.RUnlock()
	out := make([]Member, 0, len(u.members))
	for _, m := range u.members {
		m.Meta = cloneMeta(m.Meta)
		out = append(out, m)
	}
	return out
}

func (u *UDP) Size() int {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return len(u.members)
}

func (u *UDP) LocalName() string { return u.self.Name }

// Join sends a bounded membership exchange to each seed and waits for an ACK.
func (u *UDP) Join(seeds []string) (int, error) {
	reached := 0
	for _, seed := range seeds {
		addr, err := net.ResolveUDPAddr("udp", seed)
		if err != nil {
			continue
		}
		nonce := newNonce()
		ack := make(chan bool, 1)
		u.mu.Lock()
		u.pending[nonce] = ack
		u.mu.Unlock()
		u.mu.RLock()
		members := u.membersForFrameLocked()
		u.mu.RUnlock()
		err = u.send(addr, udpMessage{Type: "join", Nonce: nonce, Node: u.self, Members: members})
		if err == nil {
			select {
			case <-ack:
				reached++
			case <-u.clk.After(u.timeout):
			}
		}
		u.mu.Lock()
		delete(u.pending, nonce)
		u.mu.Unlock()
	}
	return reached, nil
}

func (u *UDP) Leave() error {
	u.mu.Lock()
	u.self.Status = StatusLeft
	u.self.Incarnation++
	self := u.self
	peers := u.peerAddrsLocked()
	u.members[self.ID] = self
	u.mu.Unlock()
	for _, addr := range peers {
		_ = u.send(addr, udpMessage{Type: "member", Node: self})
	}
	return nil
}

func (u *UDP) Subscribe(ch chan<- MemberEvent) {
	u.mu.Lock()
	u.subs[ch] = struct{}{}
	u.mu.Unlock()
}

func (u *UDP) Unsubscribe(ch chan<- MemberEvent) {
	u.mu.Lock()
	delete(u.subs, ch)
	u.mu.Unlock()
}

func (u *UDP) Broadcast(payload []byte) error {
	if len(payload) > MaxPiggybackBytes {
		return ErrPayloadTooLarge
	}
	id := newNonce()
	if !u.markSeen(id) {
		return nil
	}
	return u.forwardBroadcast(udpMessage{
		Type: "broadcast", ID: id, From: u.self.ID, Via: u.self.ID,
		TTL: u.ttl, Payload: append([]byte(nil), payload...),
	}, "")
}

func (u *UDP) OnBroadcast(fn func(from NodeID, payload []byte)) {
	u.mu.Lock()
	u.handlers = append(u.handlers, fn)
	u.mu.Unlock()
}

// Close stops the network node. It is an alias for Stop for callers that own
// the concrete transport.
func (u *UDP) Close() error { u.Stop(); return nil }

func (u *UDP) Stop() {
	u.stopOnce.Do(func() {
		close(u.stop)
		_ = u.conn.Close()
	})
	u.wg.Wait()
}

type udpMessage struct {
	Type    string   `json:"type"`
	ID      string   `json:"id,omitempty"`
	TTL     int      `json:"ttl,omitempty"`
	Hops    int      `json:"hops,omitempty"`
	Nonce   string   `json:"nonce,omitempty"`
	From    NodeID   `json:"from,omitempty"`
	Via     NodeID   `json:"via,omitempty"`
	Node    Member   `json:"node,omitempty"`
	Members []Member `json:"members,omitempty"`
	Payload []byte   `json:"payload,omitempty"`
}

func (u *UDP) readLoop() {
	defer u.wg.Done()
	buf := make([]byte, maxUDPFrame)
	for {
		_ = u.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, addr, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-u.stop:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			continue
		}
		var msg udpMessage
		if n == 0 || n > maxUDPFrame || json.Unmarshal(buf[:n], &msg) != nil {
			continue
		}
		u.handleMessage(addr, msg)
	}
}

func (u *UDP) handleMessage(addr *net.UDPAddr, msg udpMessage) {
	if msg.Node.ID != "" && msg.Node.ID != u.self.ID {
		u.mergeMember(msg.Node, addr)
	}
	switch msg.Type {
	case "join":
		u.mergeMembers(msg.Members, addr)
		if msg.Node.ID != "" {
			u.mergeMember(msg.Node, addr)
		}
		u.mu.RLock()
		members := u.membersForFrameLocked()
		u.mu.RUnlock()
		_ = u.send(addr, udpMessage{Type: "join_ack", Nonce: msg.Nonce, Node: u.self, Members: members})
	case "join_ack":
		u.mergeMembers(msg.Members, addr)
		u.signal(msg.Nonce, true)
	case "ping":
		_ = u.send(addr, udpMessage{Type: "ack", Nonce: msg.Nonce, Node: u.self})
	case "ack":
		u.signal(msg.Nonce, true)
	case "member":
		// mergeMember above applies join/update/leave status.
	case "broadcast":
		if msg.ID == "" || !u.markSeen(msg.ID) {
			return
		}
		u.mu.RLock()
		handlers := append([]func(NodeID, []byte){}, u.handlers...)
		u.mu.RUnlock()
		for _, fn := range handlers {
			go fn(msg.From, append([]byte(nil), msg.Payload...))
		}
		if msg.TTL > 1 {
			_ = u.forwardBroadcast(msg, msg.Via)
		}
	}
}

func (u *UDP) mergeMembers(list []Member, addr *net.UDPAddr) {
	for _, m := range list {
		u.mergeMember(m, addr)
	}
}

func (u *UDP) mergeMember(m Member, addr *net.UDPAddr) {
	if m.ID == "" || m.ID == u.self.ID {
		return
	}
	u.mu.Lock()
	cur, exists := u.members[m.ID]
	if exists && m.Incarnation < cur.Incarnation {
		u.mu.Unlock()
		return
	}
	m.Meta = cloneMeta(m.Meta)
	peerAddr := addr
	if m.Addr != "" && m.Port > 0 {
		if advertised, err := net.ResolveUDPAddr("udp", net.JoinHostPort(m.Addr, strconv.Itoa(m.Port))); err == nil {
			peerAddr = advertised
		}
	}
	u.members[m.ID] = m
	u.peers[m.ID] = peerAddr
	u.lastSeen[m.ID] = u.clk.Now()
	changed := !exists || cur.Status != m.Status || cur.Incarnation != m.Incarnation
	subs := u.subscribersLocked()
	u.mu.Unlock()
	if changed {
		typ := Update
		switch {
		case !exists || m.Status == StatusAlive && cur.Status != StatusAlive:
			typ = Join
		case m.Status == StatusLeft:
			typ = Leave
		case m.Status == StatusFailed:
			typ = Failed
		}
		ev := MemberEvent{Type: typ, Node: m, At: u.clk.Now()}
		for _, ch := range subs {
			select {
			case ch <- ev:
			default:
			}
		}
	}
}

func (u *UDP) probeLoop() {
	defer u.wg.Done()
	ticker := time.NewTicker(u.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			u.probe()
		case <-u.stop:
			return
		}
	}
}

func (u *UDP) probe() {
	u.mu.RLock()
	peers := make(map[NodeID]*net.UDPAddr, len(u.peers))
	for id, addr := range u.peers {
		peers[id] = addr
	}
	u.mu.RUnlock()
	for id, addr := range peers {
		nonce := newNonce()
		ack := make(chan bool, 1)
		u.mu.Lock()
		u.pending[nonce] = ack
		u.mu.Unlock()
		_ = u.send(addr, udpMessage{Type: "ping", Nonce: nonce, Node: u.self})
		select {
		case <-ack:
			u.mu.Lock()
			u.lastSeen[id] = u.clk.Now()
			u.mu.Unlock()
		case <-u.clk.After(u.timeout):
			u.markFailed(id)
		}
		u.mu.Lock()
		delete(u.pending, nonce)
		u.mu.Unlock()
	}
}

func (u *UDP) markFailed(id NodeID) {
	u.mu.Lock()
	m, ok := u.members[id]
	if !ok || m.Status == StatusFailed || m.Status == StatusLeft {
		u.mu.Unlock()
		return
	}
	last := u.lastSeen[id]
	if u.clk.Now().Sub(last) < time.Duration(u.failAfter)*u.interval {
		u.mu.Unlock()
		return
	}
	m.Status = StatusFailed
	m.Incarnation++
	u.members[id] = m
	subs := u.subscribersLocked()
	u.mu.Unlock()
	ev := MemberEvent{Type: Failed, Node: m, At: u.clk.Now()}
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (u *UDP) peerAddrsLocked() []*net.UDPAddr {
	out := make([]*net.UDPAddr, 0, len(u.peers))
	for _, addr := range u.peers {
		if addr != nil {
			out = append(out, addr)
		}
	}
	return out
}

type udpPeer struct {
	id   NodeID
	addr *net.UDPAddr
}

func (u *UDP) forwardBroadcast(msg udpMessage, previous NodeID) error {
	u.mu.RLock()
	peers := u.broadcastPeersLocked(msg, previous)
	u.mu.RUnlock()
	var firstErr error
	for _, peer := range peers {
		next := msg
		next.TTL--
		next.Hops++
		next.Via = u.self.ID
		if err := u.send(peer.addr, next); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (u *UDP) broadcastPeersLocked(msg udpMessage, previous NodeID) []udpPeer {
	ids := make([]NodeID, 0, len(u.peers)+1)
	for id := range u.peers {
		ids = append(ids, id)
	}
	ids = append(ids, u.self.ID)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) == 0 {
		return nil
	}
	origin := 0
	for i, id := range ids {
		if id == msg.From {
			origin = i
			break
		}
	}
	current := 0
	for i, id := range ids {
		if id == u.self.ID {
			current = (i - origin + len(ids)) % len(ids)
			break
		}
	}
	out := make([]udpPeer, 0, u.fanout)
	for child := current*u.fanout + 1; child < len(ids) && len(out) < u.fanout; child++ {
		id := ids[(origin+child)%len(ids)]
		if id == u.self.ID || id == previous {
			continue
		}
		addr := u.peers[id]
		if addr != nil {
			out = append(out, udpPeer{id: id, addr: addr})
		}
	}
	// Membership views can lag behind one another. Fill unused slots from the
	// remaining peers so a node that learned a new branch can continue the
	// infection without reverting to a broadcast storm.
	for _, id := range ids {
		if len(out) >= u.fanout || id == u.self.ID || id == previous {
			continue
		}
		addr := u.peers[id]
		if addr == nil {
			continue
		}
		already := false
		for _, peer := range out {
			if peer.id == id {
				already = true
				break
			}
		}
		if !already {
			out = append(out, udpPeer{id: id, addr: addr})
		}
	}
	return out
}

func (u *UDP) markSeen(id string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, ok := u.seen[id]; ok {
		return false
	}
	u.seen[id] = u.clk.Now()
	if len(u.seen) > 4096 {
		cutoff := u.clk.Now().Add(-10 * time.Minute)
		for seenID, at := range u.seen {
			if at.Before(cutoff) {
				delete(u.seen, seenID)
			}
		}
		for len(u.seen) > 4096 {
			var oldestID string
			var oldest time.Time
			for seenID, at := range u.seen {
				if oldestID == "" || at.Before(oldest) {
					oldestID, oldest = seenID, at
				}
			}
			delete(u.seen, oldestID)
		}
	}
	return true
}

func (u *UDP) subscribersLocked() []chan<- MemberEvent {
	out := make([]chan<- MemberEvent, 0, len(u.subs))
	for ch := range u.subs {
		out = append(out, ch)
	}
	return out
}

func (u *UDP) membersForFrameLocked() []Member {
	out := make([]Member, 0, len(u.members))
	for _, member := range u.members {
		out = append(out, member)
		b, err := json.Marshal(udpMessage{Type: "join_ack", Node: u.self, Members: out})
		if err != nil || len(b) > maxUDPFrame {
			out = out[:len(out)-1]
			break
		}
	}
	return out
}

func (u *UDP) signal(nonce string, ok bool) {
	if nonce == "" {
		return
	}
	u.mu.RLock()
	ch := u.pending[nonce]
	u.mu.RUnlock()
	if ch != nil {
		select {
		case ch <- ok:
		default:
		}
	}
}

func (u *UDP) send(addr *net.UDPAddr, msg udpMessage) error {
	if addr == nil {
		return errors.New("gossip: nil peer address")
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if len(b) > maxUDPFrame {
		return fmt.Errorf("gossip: frame too large: %d", len(b))
	}
	_, err = u.conn.WriteToUDP(b, addr)
	return err
}

func newNonce() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(b)
}

func cloneMeta(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
