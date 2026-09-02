package gossip_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/gossip"
)

// TestWAN_MultiDCConvergence proves that catalog deltas injected in one DC
// propagate to a second DC via WAN flood + deliver. Two independent gossip
// clusters (one per DC) are linked by WANPools. DC1 floods a digest; the
// remote DC2 receives it via Deliver (simulating network transit).
func TestWAN_MultiDCConvergence(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))

	// --- DC1: 5-node LAN gossip cluster ---
	dc1Cluster := gossip.NewCluster(clk)
	dc1Cluster.SetNetwork(gossip.NetworkConfig{Latency: 10 * time.Millisecond, Fanout: 3})
	dc1Nodes := make([]*gossip.MemoryMembership, 5)
	for i := 0; i < 5; i++ {
		dc1Nodes[i] = gossip.NewMemory(dc1Cluster, fmt.Sprintf("dc1-n%d", i), "10.1.0.1", 9000+i)
	}

	// --- DC2: 5-node LAN gossip cluster ---
	dc2Cluster := gossip.NewCluster(clk)
	dc2Cluster.SetNetwork(gossip.NetworkConfig{Latency: 10 * time.Millisecond, Fanout: 3})
	dc2Nodes := make([]*gossip.MemoryMembership, 5)
	for i := 0; i < 5; i++ {
		dc2Nodes[i] = gossip.NewMemory(dc2Cluster, fmt.Sprintf("dc2-n%d", i), "10.2.0.1", 9100+i)
	}

	// --- WAN pools linking the two DCs ---
	wanDC1 := gossip.NewWAN("dc1")
	wanDC2 := gossip.NewWAN("dc2")

	// Register gateways.
	wanDC1.JoinDC("dc2", []gossip.Member{{Name: "dc2-gw", Addr: "10.2.0.1"}})
	wanDC2.JoinDC("dc1", []gossip.Member{{Name: "dc1-gw", Addr: "10.1.0.1"}})

	// Collect payloads arriving in DC2 from DC1.
	var mu sync.Mutex
	dc2Received := make(map[string]uint64) // payload string → index
	wanDC2.OnFlood("dc1", func(fromDC string, index uint64, payload []byte) {
		mu.Lock()
		dc2Received[string(payload)] = index
		mu.Unlock()
	})

	// DC1 LAN: inject a catalog delta (broadcast on the gossip fabric).
	payload := []byte(`{"type":"register","id":"svc-1","svc":"payments","node":"dc1-n0","addr":"10.1.0.1","port":8080}`)
	_ = dc1Nodes[0].Broadcast(payload)

	// Advance virtual clock until DC1 cluster converges.
	deadline := clk.Now().Add(5 * time.Second)
	for clk.Now().Before(deadline) {
		if dc1Cluster.DeliveredBytes() >= int64(len(payload))*int64(len(dc1Nodes)-1) {
			break
		}
		clk.Advance(5 * time.Millisecond)
	}
	if dc1Cluster.DeliveredBytes() < int64(len(payload))*int64(len(dc1Nodes)-1) {
		t.Fatal("DC1 gossip did not converge within 5s virtual")
	}

	// --- WAN flood + deliver: DC1 pushes, DC2 receives ---
	// In production, Flood serializes the payload and sends it over the wire.
	// The remote DC receives it and calls Deliver. We simulate both sides.
	wanDC1.Flood(1, payload)             // DC1 side: fires handlers on DC1's pool for dc2
	wanDC2.Deliver("dc1", 1, payload) // DC2 side: simulates receiving from dc1

	// Verify DC2 received the payload.
	mu.Lock()
	got, ok := dc2Received[string(payload)]
	mu.Unlock()
	if !ok {
		t.Fatal("DC2 did not receive WAN flood from DC1")
	}
	if got != 1 {
		t.Fatalf("DC2 got index %d, want 1", got)
	}

	// --- Reverse: DC2 floods, DC1 receives ---
	dc1Received := make(map[string]uint64)
	wanDC1.OnFlood("dc2", func(fromDC string, index uint64, payload []byte) {
		mu.Lock()
		dc1Received[string(payload)] = index
		mu.Unlock()
	})

	payload2 := []byte(`{"type":"register","id":"svc-2","svc":"users","node":"dc2-n0","addr":"10.2.0.1","port":9000}`)
	wanDC2.Flood(2, payload2)
	wanDC1.Deliver("dc2", 2, payload2)

	mu.Lock()
	got2, ok2 := dc1Received[string(payload2)]
	mu.Unlock()
	if !ok2 {
		t.Fatal("DC1 did not receive WAN flood from DC2")
	}
	if got2 != 2 {
		t.Fatalf("DC1 got index %d, want 2", got2)
	}
}

// TestWAN_PartitionBetweenDCs proves that when two groups in a cluster are
// partitioned, membership events (Fail) from one side do NOT reach the other.
// After healing, NEW events propagate correctly to both sides.
func TestWAN_PartitionBetweenDCs(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	cluster := gossip.NewCluster(clk)
	cluster.SetNetwork(gossip.NetworkConfig{Latency: 5 * time.Millisecond, Fanout: 3})

	// Build nodes: dc1 group (n0..n2) and dc2 group (n3..n5).
	dc1Names := []string{"dc1-n0", "dc1-n1", "dc1-n2"}
	dc2Names := []string{"dc2-n0", "dc2-n1", "dc2-n2"}
	allNames := append(dc1Names, dc2Names...)

	nodes := make(map[string]*gossip.MemoryMembership, len(allNames))
	for i, name := range allNames {
		nodes[name] = gossip.NewMemory(cluster, name, "127.0.0.1", 8000+i)
	}

	// Subscribe to membership events on each group.
	var mu sync.Mutex
	dc1Events := 0
	dc2Events := 0

	subscribe := func(name string, counter *int) {
		ch := make(chan gossip.MemberEvent, 64)
		nodes[name].Subscribe(ch)
		go func() {
			for range ch {
				mu.Lock()
				*counter++
				mu.Unlock()
			}
		}()
	}
	for _, name := range dc1Names {
		subscribe(name, &dc1Events)
	}
	for _, name := range dc2Names {
		subscribe(name, &dc2Events)
	}

	// Partition DC1 from DC2.
	cluster.Partition(dc1Names, dc2Names)

	// Fail a DC1 node — events should NOT reach DC2.
	nodes["dc1-n0"].Fail()

	clk.Advance(500 * time.Millisecond)

	mu.Lock()
	dc2DuringPartition := dc2Events
	mu.Unlock()

	if dc2DuringPartition > 0 {
		t.Fatalf("DC2 saw events during partition: %d", dc2DuringPartition)
	}

	// Heal the partition.
	cluster.Heal()
	clk.Advance(500 * time.Millisecond)

	// Now trigger a NEW event (fail another dc1 node) — should reach DC2.
	mu.Lock()
	dc2BeforePostHeal := dc2Events
	mu.Unlock()

	nodes["dc1-n1"].Fail()
	clk.Advance(500 * time.Millisecond)
	// Allow async subscriber goroutines to run (real time yield).
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	dc2AfterPostHeal := dc2Events
	mu.Unlock()

	if dc2AfterPostHeal <= dc2BeforePostHeal {
		t.Fatal("DC2 did not receive events after heal")
	}
}
