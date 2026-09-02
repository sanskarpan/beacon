// Package swim adapts the Gossip-Protocol SWIM project behind beacon's
// gossip.Membership seam. Catalog deltas use Broadcast/OnBroadcast so they
// piggyback on the existing SWIM stream — we do not run a second protocol.
package swim

import (
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/gossip"
	gswim "gossip-system/pkg/swim"
)

// Ensure Adapter implements gossip.Membership.
var _ gossip.Membership = (*Adapter)(nil)

// Cluster wraps the SWIM fabric used by one or more Adapter nodes.
type Cluster struct {
	inner *gswim.Cluster
}

// NewCluster creates a SWIM fabric. fastFailure tightens probe periods for tests.
func NewCluster(fastFailure bool) *Cluster {
	cfg := gswim.DefaultConfig()
	cfg.FastFailure = fastFailure
	if fastFailure {
		cfg.ProtocolPeriod = 100 * time.Millisecond
		cfg.ProbeTimeout = 30 * time.Millisecond
	}
	return &Cluster{inner: gswim.NewCluster(cfg)}
}

// Inner returns the underlying SWIM cluster (partitions, advanced tests).
func (c *Cluster) Inner() *gswim.Cluster { return c.inner }

// Adapter is a gossip.Membership backed by real SWIM.
type Adapter struct {
	node *gswim.Node
	mu   sync.Mutex
	bridges map[chan<- gossip.MemberEvent]chan gswim.Event
}

// NewNode starts a SWIM member on the cluster and returns a Membership adapter.
func (c *Cluster) NewNode(name, addr string, port int) (*Adapter, error) {
	n, err := c.inner.NewNode(name, addr, port)
	if err != nil {
		return nil, err
	}
	return &Adapter{node: n, bridges: make(map[chan<- gossip.MemberEvent]chan gswim.Event)}, nil
}

// Members implements gossip.Membership.
func (a *Adapter) Members() []gossip.Member {
	raw := a.node.Members()
	out := make([]gossip.Member, len(raw))
	for i, m := range raw {
		out[i] = toBeaconMember(m)
	}
	return out
}

// Size implements gossip.Membership.
func (a *Adapter) Size() int { return a.node.Size() }

// LocalName implements gossip.Membership.
func (a *Adapter) LocalName() string { return a.node.LocalName() }

// Join implements gossip.Membership.
func (a *Adapter) Join(seeds []string) (int, error) { return a.node.Join(seeds) }

// Leave implements gossip.Membership.
func (a *Adapter) Leave() error { return a.node.Leave() }

// Subscribe implements gossip.Membership.
func (a *Adapter) Subscribe(ch chan<- gossip.MemberEvent) {
	// Bridge: SWIM events → beacon MemberEvent
	bridge := make(chan gswim.Event, 64)
	a.mu.Lock()
	a.bridges[ch] = bridge
	a.mu.Unlock()
	a.node.Subscribe(bridge)
	go func() {
		for ev := range bridge {
			select {
			case ch <- gossip.MemberEvent{
				Type: toBeaconType(ev.Type),
				Node: toBeaconMember(ev.Node),
				At:   ev.At,
			}:
			default:
			}
		}
	}()
}

// Unsubscribe removes a previously subscribed channel.
func (a *Adapter) Unsubscribe(ch chan<- gossip.MemberEvent) {
	a.mu.Lock()
	bridge, ok := a.bridges[ch]
	if ok {
		delete(a.bridges, ch)
		close(bridge)
	}
	a.mu.Unlock()
}

// Broadcast implements gossip.Membership — piggybacks on SWIM.
func (a *Adapter) Broadcast(payload []byte) error { return a.node.Broadcast(payload) }

// OnBroadcast implements gossip.Membership.
func (a *Adapter) OnBroadcast(fn func(from gossip.NodeID, payload []byte)) {
	a.node.OnBroadcast(func(from string, payload []byte) {
		fn(gossip.NodeID(from), payload)
	})
}

// Fail simulates SWIM failure detection for tests.
func (a *Adapter) Fail() { a.node.Fail() }

// Stop tears down the node.
func (a *Adapter) Stop() {
	a.mu.Lock()
	for _, bridge := range a.bridges {
		close(bridge)
	}
	a.bridges = make(map[chan<- gossip.MemberEvent]chan gswim.Event)
	a.mu.Unlock()
	a.node.Stop()
}

// Node returns the underlying SWIM node.
func (a *Adapter) Node() *gswim.Node { return a.node }

func toBeaconMember(m gswim.Member) gossip.Member {
	st := gossip.StatusAlive
	switch m.Status {
	case gswim.StatusSuspect:
		st = gossip.StatusSuspect
	case gswim.StatusFailed:
		st = gossip.StatusFailed
	case gswim.StatusLeft:
		st = gossip.StatusLeft
	}
	return gossip.Member{
		ID:          gossip.NodeID(m.ID),
		Name:        m.Name,
		Addr:        m.Addr,
		Port:        m.Port,
		Status:      st,
		Incarnation: m.Incarnation,
		Meta:        m.Meta,
	}
}

func toBeaconType(t gswim.EventType) gossip.MemberEventType {
	switch t {
	case gswim.Join:
		return gossip.Join
	case gswim.Leave:
		return gossip.Leave
	case gswim.Failed:
		return gossip.Failed
	default:
		return gossip.Update
	}
}
