// Package gossip defines the membership/propagation seam with the existing SWIM project.
//
// beacon depends on this interface, not on gossip-system internals, so the
// transport can be swapped for a test double or a different membership impl.
//
// Integration: the Gossip-Protocol project (SWIM membership, failure detection,
// piggyback) is adapted behind Membership. Catalog deltas piggyback on the
// existing gossip stream — we do not run a second gossip protocol.
package gossip

import "time"

// NodeID identifies a gossip member.
type NodeID string

// Member is a node in the membership pool.
type Member struct {
	ID          NodeID            `json:"id"`
	Name        string            `json:"name"`
	Addr        string            `json:"addr"`
	Port        int               `json:"port"`
	Meta        map[string]string `json:"meta,omitempty"`
	Status      MemberStatus      `json:"status"`
	Incarnation uint64            `json:"incarnation"`
}

// MemberStatus is node-level liveness.
type MemberStatus string

const (
	StatusAlive   MemberStatus = "alive"
	StatusSuspect MemberStatus = "suspect"
	StatusFailed  MemberStatus = "failed"
	StatusLeft    MemberStatus = "left"
)

// MemberEventType classifies membership changes.
type MemberEventType int

const (
	// Join is a new or recovered node.
	Join MemberEventType = iota
	// Leave is a graceful departure.
	Leave
	// Failed is a failure-detector declaration.
	Failed
	// Update is a metadata/incarnation change.
	Update
)

func (t MemberEventType) String() string {
	switch t {
	case Join:
		return "join"
	case Leave:
		return "leave"
	case Failed:
		return "failed"
	case Update:
		return "update"
	default:
		return "unknown"
	}
}

// MemberEvent is a membership change notification.
type MemberEvent struct {
	Type MemberEventType `json:"type"`
	Node Member          `json:"node"`
	At   time.Time       `json:"at,omitempty"`
}

// Membership is the contract with the SWIM project.
//
// Node-level liveness feeds service removal: when a node is Failed/Left,
// every instance on that node is marked critical immediately — ~7× faster
// than waiting for per-instance health checks to time out.
type Membership interface {
	// Members returns a snapshot of the current pool.
	Members() []Member
	// Size is the number of known members (including self).
	Size() int
	// LocalName returns this node's name.
	LocalName() string
	// Join contacts seed addresses and returns how many were reached.
	Join(seeds []string) (int, error)
	// Leave gracefully departs the pool.
	Leave() error
	// Subscribe sends membership events on ch. Multiple subscribers are allowed.
	// The channel should be buffered; slow consumers drop.
	Subscribe(ch chan<- MemberEvent)
	// Unsubscribe removes a previously subscribed channel.
	Unsubscribe(ch chan<- MemberEvent)
	// Broadcast piggybacks a payload on the existing gossip stream.
	Broadcast(payload []byte) error
	// OnBroadcast registers a handler for payloads received from peers.
	// Multiple handlers are invoked for each message.
	OnBroadcast(fn func(from NodeID, payload []byte))
}

// CatalogDeltaType is the kind of service-catalog mutation propagated by gossip.
type CatalogDeltaType int

const (
	DeltaRegister CatalogDeltaType = iota
	DeltaDeregister
	DeltaHealthChange
)

func (t CatalogDeltaType) String() string {
	switch t {
	case DeltaRegister:
		return "register"
	case DeltaDeregister:
		return "deregister"
	case DeltaHealthChange:
		return "health"
	default:
		return "unknown"
	}
}

// MaxPiggybackBytes bounds a single gossip piggyback frame.
// Overflow goes to the anti-entropy path.
const MaxPiggybackBytes = 512
