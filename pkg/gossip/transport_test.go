package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
)

// TestTransportLatencyDelaysDelivery verifies one-way latency is honored: no
// node sees the payload before Latency has elapsed (TODO-045).
func TestTransportLatencyDelaysDelivery(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	cluster := NewCluster(clk)
	cluster.SetNetwork(NetworkConfig{Latency: 100 * time.Millisecond, Fanout: 2})

	const n = 5
	got := make([]*sync.Mutex, n) // per-node receive lock
	for i := 0; i < n; i++ {
		got[i] = &sync.Mutex{}
	}
	nodes := make([]*MemoryMembership, n)
	delivered := make([]bool, n)
	for i := 0; i < n; i++ {
		idx := i
		nodes[i] = NewMemory(cluster, "n"+itoa(i), "127.0.0.1", 8000+i)
		nodes[i].OnBroadcast(func(from NodeID, p []byte) {
			got[idx].Lock()
			delivered[idx] = true
			got[idx].Unlock()
		})
	}

	payload := []byte{'0'}
	if err := nodes[0].Broadcast(payload); err != nil {
		t.Fatal(err)
	}
	// Before any advance, nothing may be delivered.
	for i := 1; i < n; i++ {
		got[i].Lock()
		if delivered[i] {
			t.Fatalf("node %d delivered before latency elapsed", i)
		}
		got[i].Unlock()
	}
	// Advance just under one latency hop — nothing (except hops scheduled at
	// exactly Latency from origin, which fire at t=100ms, not before).
	clk.Advance(99 * time.Millisecond)
	for i := 1; i < n; i++ {
		got[i].Lock()
		if delivered[i] {
			t.Fatalf("node %d delivered before 100ms latency", i)
		}
		got[i].Unlock()
	}
	// Advance past the latency — infection rounds cover the cluster.
	clk.Advance(2 * time.Second)
	converged := 1 // origin
	for i := 1; i < n; i++ {
		got[i].Lock()
		if delivered[i] {
			converged++
		}
		got[i].Unlock()
	}
	if converged != n {
		t.Fatalf("latency model: only %d/%d received payload", converged, n)
	}
}

// TestTransportLossStillConverges runs a lossy link scenario: with 25% per-hop
// loss and bounded fanout, redundant infection rounds still deliver the payload
// to every node (TODO-045 headline claim: loss > 0 does not break convergence).
func TestTransportLossStillConverges(t *testing.T) {
	for trial := 0; trial < 3; trial++ {
		clk := clock.NewVirtual(time.Unix(0, 0).UTC())
		cluster := NewCluster(clk)
		cluster.SetNetwork(NetworkConfig{Latency: 10 * time.Millisecond, Loss: 0.25, Fanout: 3})

		const n = 20
		nodes := make([]*MemoryMembership, n)
		var mu sync.Mutex
		received := make([]bool, n)
		for i := 0; i < n; i++ {
			idx := i
			nodes[i] = NewMemory(cluster, "n"+itoa(i), "127.0.0.1", 9000+i)
			nodes[i].OnBroadcast(func(from NodeID, p []byte) {
				mu.Lock()
				received[idx] = true
				mu.Unlock()
			})
		}
		if err := nodes[0].Broadcast([]byte{'0'}); err != nil {
			t.Fatal(err)
		}
		// Give the infection plenty of time to reach everyone despite drops.
		clk.Advance(5 * time.Second)
		converged := 1
		for i := 1; i < n; i++ {
			mu.Lock()
			if received[i] {
				converged++
			}
			mu.Unlock()
		}
		if converged != n {
			t.Fatalf("trial %d: lossy gossip converged only %d/%d", trial, converged, n)
		}
	}
}

// TestTransportLossBlocksAll verifies that Loss=1.0 drops every hop — nothing
// gets through the lossy data plane (TODO-045 loss ceiling).
func TestTransportLossBlocksAll(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	cluster := NewCluster(clk)
	cluster.SetNetwork(NetworkConfig{Latency: 10 * time.Millisecond, Loss: 1.0, Fanout: 3})

	const n = 6
	nodes := make([]*MemoryMembership, n)
	var mu sync.Mutex
	received := 0
	for i := 0; i < n; i++ {
		nodes[i] = NewMemory(cluster, "n"+itoa(i), "127.0.0.1", 10000+i)
		nodes[i].OnBroadcast(func(from NodeID, p []byte) {
			mu.Lock()
			received++
			mu.Unlock()
		})
	}
	if err := nodes[0].Broadcast([]byte("x")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(5 * time.Second)
	mu.Lock()
	defer mu.Unlock()
	if received != 0 {
		t.Fatalf("Loss=1.0: %d deliveries leaked through", received)
	}
}

// TestTransportReorder verifies reorder jitter makes per-hop delivery times
// differ so arrivals can land out of order. Uses a two-hop path: with Reorder
// the two hops land at distinct virtual times with high probability.
func TestTransportReorder(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	cluster := NewCluster(clk)
	// Reorder jitter spans [Latency/2, Latency); with many hops arrivals spread.
	cluster.SetNetwork(NetworkConfig{Latency: 50 * time.Millisecond, Reorder: true, Fanout: 2})

	const n = 8
	type rec struct {
		at time.Time
	}
	recs := make([]rec, n)
	nodes := make([]*MemoryMembership, n)
	for i := 0; i < n; i++ {
		idx := i
		nodes[i] = NewMemory(cluster, "n"+itoa(i), "127.0.0.1", 11000+i)
		nodes[i].OnBroadcast(func(from NodeID, p []byte) {
			recs[idx].at = clk.Now()
		})
	}
	if err := nodes[0].Broadcast([]byte{'0'}); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)

	// Collect distinct delivery times; reorder should spread them across
	// multiple distinct virtual instants (not all equal).
	seen := map[time.Time]bool{}
	distinct := 0
	for i := 1; i < n; i++ {
		at := recs[i].at
		if at.IsZero() {
			t.Fatalf("node %d not delivered under reorder model", i)
		}
		if !seen[at] {
			seen[at] = true
			distinct++
		}
	}
	if distinct < 2 {
		t.Fatalf("reorder: all %d deliveries landed at one instant (%v)", distinct, seen)
	}
}

func TestTransportBoundedFanoutAndDuplicateSuppression(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	cluster := NewCluster(clk)
	cluster.SetNetwork(NetworkConfig{Fanout: 2, TTL: 8})
	nodes := make([]*MemoryMembership, 7)
	received := make([]int, len(nodes))
	for i := range nodes {
		nodes[i] = NewMemory(cluster, "n"+itoa(i), "127.0.0.1", 12000+i)
		idx := i
		nodes[i].OnBroadcast(func(NodeID, []byte) { received[idx]++ })
	}
	payload := []byte("bounded")
	if err := nodes[0].Broadcast(payload); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)
	for i := 1; i < len(nodes); i++ {
		if received[i] != 1 {
			t.Fatalf("node %d received %d copies, want 1", i, received[i])
		}
	}
	if received[0] != 0 {
		t.Fatalf("origin received %d copies", received[0])
	}
	if got, want := cluster.SentBytes(), int64(len(payload)*(len(nodes)-1)); got != want {
		t.Fatalf("bounded fanout sent %d bytes, want %d", got, want)
	}

	msg := broadcastMessage{ID: "duplicate", Origin: "n0", TTL: 1, Payload: []byte("x")}
	if !nodes[1].handleBroadcast(msg, "n0") {
		t.Fatal("first delivery was suppressed")
	}
	if nodes[1].handleBroadcast(msg, "n0") {
		t.Fatal("duplicate delivery was not suppressed")
	}
	if received[1] != 2 {
		t.Fatalf("duplicate reached handler %d times, want one additional delivery", received[1])
	}
}

func TestTransportTTLBoundsInfection(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	cluster := NewCluster(clk)
	cluster.SetNetwork(NetworkConfig{Fanout: 2, TTL: 2})
	nodes := make([]*MemoryMembership, 6)
	received := make([]int, len(nodes))
	for i := range nodes {
		nodes[i] = NewMemory(cluster, "n"+itoa(i), "127.0.0.1", 13000+i)
		idx := i
		nodes[i].OnBroadcast(func(NodeID, []byte) { received[idx]++ })
	}
	_ = nodes[0].Broadcast([]byte("ttl"))
	clk.Advance(time.Second)
	if received[1] == 0 || received[2] == 0 {
		t.Fatal("TTL=2 did not deliver the first infection hop")
	}
	for i := 3; i < len(nodes); i++ {
		if received[i] != 0 {
			t.Fatalf("node %d received beyond TTL: %d", i, received[i])
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
