// Package lab hosts interactive labs driven by the console (AP vs CP, etc.).
package lab

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	rstore "github.com/sanskar/beacon/pkg/store/raft"
)

// ConsistencyLab runs real AP (gossip) and CP (raft lab) backends side by side
// so the console can drive partition/heal and read live divergence (TODO-050).
type ConsistencyLab struct {
	mu          sync.Mutex
	clk         clock.Clock
	bus         *events.Bus
	apCluster   *gossip.Cluster
	apA, apB    *gstore.Store
	cp          *rstore.Cluster
	partitioned bool
	seq         int
}

// NewConsistencyLab boots dual AP nodes + a 3-node CP cluster.
func NewConsistencyLab(clk clock.Clock, bus *events.Bus) *ConsistencyLab {
	if clk == nil {
		clk = clock.New()
	}
	if bus == nil {
		bus = events.NewBus(clk)
	}
	// Full-mesh AP fabric for predictable lab writes (Fanout 0).
	apC := gossip.NewCluster(clk)
	apC.SetNetwork(gossip.NetworkConfig{Fanout: 0})

	memA := gossip.NewMemory(apC, "ap-a", "127.0.0.1", 7946)
	memB := gossip.NewMemory(apC, "ap-b", "127.0.0.1", 7947)
	_, _ = memB.Join([]string{"ap-a"})

	csA := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	csB := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	sa := gstore.New(gstore.Config{Local: csA, Membership: memA, Bus: bus})
	sb := gstore.New(gstore.Config{Local: csB, Membership: memB, Bus: bus})

	cp := rstore.NewCluster([]string{"cp-1", "cp-2", "cp-3"}, clk, bus)

	return &ConsistencyLab{
		clk: clk, bus: bus,
		apCluster: apC, apA: sa, apB: sb,
		cp: cp,
	}
}

// Status is the console payload.
type Status struct {
	Partitioned   bool   `json:"partitioned"`
	APAInstances  int    `json:"ap_a_instances"`
	APBInstances  int    `json:"ap_b_instances"`
	Divergence    int    `json:"divergence"`
	APWriteNote   string `json:"ap_write_note"`
	CPMajorityOK  bool   `json:"cp_majority_ok"`
	CPMinorityOK  bool   `json:"cp_minority_ok"`
	CPMinorityMsg string `json:"cp_minority_msg"`
	CPLeader      string `json:"cp_leader"`
	CPIndexLeader uint64 `json:"cp_index_leader"`
	CPIndexMinor  uint64 `json:"cp_index_minority"`
}

// Snapshot returns live counters from real catalogs.
func (l *ConsistencyLab) Snapshot() Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	aIDs := instanceIDs(l.apA)
	bIDs := instanceIDs(l.apB)
	div := symmetricDiff(aIDs, bIDs)

	stLead := rstore.NewStore(l.cp.Node("cp-1"))
	stMin := rstore.NewStore(l.cp.Node("cp-3"))

	note := "ACCEPTED both sides (gossip converges)"
	if l.partitioned {
		note = "ACCEPTED independently (may diverge)"
	}

	minOK := !l.partitioned
	minMsg := "OK (has quorum path via leader)"
	if l.partitioned {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		_, err := stMin.Register(ctx, &catalog.Instance{
			ID: fmt.Sprintf("probe-min-%d", time.Now().UnixNano()), Service: "_lab",
			Address: "0.0.0.0", Port: 1, Health: catalog.HealthPassing, Node: "cp-3",
		})
		cancel()
		if err != nil {
			minOK = false
			minMsg = err.Error()
		} else {
			minOK = true
			minMsg = "unexpected accept"
		}
	}

	return Status{
		Partitioned:   l.partitioned,
		APAInstances:  len(aIDs),
		APBInstances:  len(bIDs),
		Divergence:    div,
		APWriteNote:   note,
		CPMajorityOK:  true,
		CPMinorityOK:  minOK,
		CPMinorityMsg: minMsg,
		CPLeader:      "cp-1",
		CPIndexLeader: stLead.Index(),
		CPIndexMinor:  stMin.Index(),
	}
}

// Partition splits AP fabric and isolates CP minority (cp-3).
func (l *ConsistencyLab) Partition() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.partitioned {
		return
	}
	l.apCluster.Partition([]string{"ap-a"}, []string{"ap-b"})
	l.cp.Partition([]string{"cp-1", "cp-2"}, []string{"cp-3"})
	l.partitioned = true
	if l.bus != nil {
		l.bus.Publish(events.Event{Kind: "lab.partition", Detail: "AP split + CP minority isolated"})
	}
}

// Heal restores AP mesh and CP replication.
func (l *ConsistencyLab) Heal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.apCluster.Heal()
	l.cp.Heal()
	l.partitioned = false
	if l.bus != nil {
		l.bus.Publish(events.Event{Kind: "lab.heal", Detail: "AP + CP healed"})
	}
}

// WriteAP registers on side a or b.
func (l *ConsistencyLab) WriteAP(side string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	id := fmt.Sprintf("ap-%s-%d", side, l.seq)
	inst := &catalog.Instance{
		ID: id, Service: "lab", Address: "10.0.0.1", Port: 9000 + l.seq,
		Health: catalog.HealthPassing, Node: "lab-" + side,
	}
	var err error
	switch side {
	case "b":
		_, err = l.apB.Register(context.Background(), inst)
	default:
		_, err = l.apA.Register(context.Background(), inst)
	}
	return id, err
}

// WriteCP registers via the leader (or minority if forceMinority).
func (l *ConsistencyLab) WriteCP(forceMinority bool) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	id := fmt.Sprintf("cp-%d", l.seq)
	inst := &catalog.Instance{
		ID: id, Service: "lab", Address: "10.0.0.2", Port: 9100 + l.seq,
		Health: catalog.HealthPassing, Node: "lab",
	}
	nodeID := "cp-1"
	if forceMinority {
		nodeID = "cp-3"
	}
	st := rstore.NewStore(l.cp.Node(nodeID))
	_, err := st.Register(context.Background(), inst)
	return id, err
}

func instanceIDs(s *gstore.Store) map[string]struct{} {
	m := s.AllInstancesMap()
	out := make(map[string]struct{}, len(m))
	for id := range m {
		if len(id) >= 9 && id[:9] == "probe-min" {
			continue
		}
		out[id] = struct{}{}
	}
	return out
}

func symmetricDiff(a, b map[string]struct{}) int {
	n := 0
	for id := range a {
		if _, ok := b[id]; !ok {
			n++
		}
	}
	for id := range b {
		if _, ok := a[id]; !ok {
			n++
		}
	}
	return n
}
