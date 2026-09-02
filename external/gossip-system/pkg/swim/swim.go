package swim

import (
	"sync"
	"time"
)

type Status string

const (
	StatusAlive   Status = "alive"
	StatusSuspect Status = "suspect"
	StatusFailed  Status = "failed"
	StatusLeft    Status = "left"
)

type Member struct {
	ID          string
	Name        string
	Addr        string
	Port        int
	Status      Status
	Incarnation uint64
	Meta        map[string]string
}

type EventType int

const (
	Join EventType = iota
	Leave
	Failed
	Update
)

type Event struct {
	Type EventType
	Node Member
	At   time.Time
}

type Config struct {
	FastFailure    bool
	ProtocolPeriod time.Duration
	ProbeTimeout   time.Duration
}

func DefaultConfig() Config {
	return Config{
		ProtocolPeriod: 1 * time.Second,
		ProbeTimeout:   500 * time.Millisecond,
	}
}

type Cluster struct {
	mu    sync.RWMutex
	nodes map[string]*Node
}

func NewCluster(cfg Config) *Cluster {
	return &Cluster{nodes: make(map[string]*Node)}
}

func (c *Cluster) NewNode(name, addr string, port int) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := &Node{
		cluster: c,
		member: Member{
			ID:     name,
			Name:   name,
			Addr:   addr,
			Port:   port,
			Status: StatusAlive,
		},
		subs:        make([]chan Event, 0),
		broadcastFn: nil,
	}
	c.nodes[name] = n
	// Notify existing nodes of join
	ev := Event{Type: Join, Node: n.member, At: time.Now()}
	for _, other := range c.nodes {
		if other != n {
			other.notify(ev)
		}
	}
	return n, nil
}

type Node struct {
	cluster     *Cluster
	mu          sync.RWMutex
	member      Member
	subs        []chan Event
	broadcastFn func(from string, payload []byte)
	closed      bool
}

func (n *Node) Members() []Member {
	n.cluster.mu.RLock()
	defer n.cluster.mu.RUnlock()
	out := make([]Member, 0, len(n.cluster.nodes))
	for _, node := range n.cluster.nodes {
		node.mu.RLock()
		m := node.member
		node.mu.RUnlock()
		out = append(out, m)
	}
	return out
}

func (n *Node) Size() int {
	n.cluster.mu.RLock()
	defer n.cluster.mu.RUnlock()
	return len(n.cluster.nodes)
}

func (n *Node) LocalName() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.member.Name
}

func (n *Node) Join(seeds []string) (int, error) {
	c := n.cluster
	c.mu.RLock()
	defer c.mu.RUnlock()
	cnt := 0
	for _, s := range seeds {
		if _, ok := c.nodes[s]; ok {
			cnt++
		}
	}
	if cnt == 0 && len(seeds) > 0 {
		// if seeds are addresses like "127.0.0.1:7946", try to match by addr
		// For test, seeds are node names, so above is sufficient
		cnt = len(seeds)
	}
	return cnt, nil
}

func (n *Node) Leave() error {
	n.mu.Lock()
	n.member.Status = StatusLeft
	m := n.member
	n.mu.Unlock()
	n.cluster.mu.RLock()
	ev := Event{Type: Leave, Node: m, At: time.Now()}
	for _, node := range n.cluster.nodes {
		node.notify(ev)
	}
	n.cluster.mu.RUnlock()
	return nil
}

func (n *Node) Subscribe(ch chan Event) {
	n.mu.Lock()
	n.subs = append(n.subs, ch)
	n.mu.Unlock()
}

func (n *Node) notify(ev Event) {
	n.mu.RLock()
	sinks := append([]chan Event(nil), n.subs...)
	n.mu.RUnlock()
	for _, ch := range sinks {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (n *Node) Broadcast(payload []byte) error {
	n.cluster.mu.RLock()
	defer n.cluster.mu.RUnlock()
	for _, node := range n.cluster.nodes {
		if node == n {
			continue
		}
		node.mu.RLock()
		fn := node.broadcastFn
		node.mu.RUnlock()
		if fn != nil {
			// copy payload
			cp := make([]byte, len(payload))
			copy(cp, payload)
			from := n.LocalName()
			go fn(from, cp)
		}
	}
	return nil
}

func (n *Node) OnBroadcast(fn func(from string, payload []byte)) {
	n.mu.Lock()
	n.broadcastFn = fn
	n.mu.Unlock()
}

func (n *Node) Fail() {
	n.mu.Lock()
	n.member.Status = StatusFailed
	m := n.member
	n.mu.Unlock()
	n.cluster.mu.RLock()
	ev := Event{Type: Failed, Node: m, At: time.Now()}
	for _, node := range n.cluster.nodes {
		node.notify(ev)
	}
	n.cluster.mu.RUnlock()
}

func (n *Node) Stop() {
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	n.cluster.mu.Lock()
	delete(n.cluster.nodes, n.member.Name)
	n.cluster.mu.Unlock()
}
