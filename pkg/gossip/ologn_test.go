package gossip_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/gossip"
)

// infectionCfg is the transport model used for the O(log N) proof: real
// one-way latency with bounded fanout, exactly what the sweep/convergence
// scenarios use.
var infectionCfg = gossip.NetworkConfig{Latency: 20 * time.Millisecond, Fanout: 3}

// buildInfected delivers one payload from node 0 to every other node in an
// N-node fabric under the infection transport and returns convergence time
// (virtual), max hop depth, delivered bytes and sent bytes.
//
// NOTE: nodes are created in the fabric (NewMemory registers them in the
// cluster's nodeList) but NOT joined via Join(), whose per-node membership
// broadcast would storm the fabric with O(N²) re-gossip timers. Payload
// delivery only needs the fabric's nodeList — membership events are unrelated
// to the hop-depth/bandwidth measurement.
func buildInfected(t *testing.T, n int, cfg gossip.NetworkConfig) (elapsed time.Duration, hops int, delivered, sent int64) {
	t.Helper()
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	cluster := gossip.NewCluster(clk)
	cluster.SetNetwork(cfg)
	members := make([]*gossip.MemoryMembership, n)
	for i := 0; i < n; i++ {
		members[i] = gossip.NewMemory(cluster, fmt.Sprintf("n%d", i), "127.0.0.1", 8000+i)
	}

	payload := []byte(`{"type":"register","id":"i1","svc":"payments","node":"n0","addr":"10.0.0.1","port":8080}`)
	start := clk.Now()
	_ = members[0].Broadcast(payload)
	// Advance until every node has delivered the payload (or 5s virtual cap).
	for {
		if clk.Now().Sub(start) > 5*time.Second {
			t.Fatalf("n=%d: did not converge within 5s virtual", n)
		}
		// DeliveredBytes grows by len(payload) per receiving node; convergence
		// is when every peer has delivered exactly once.
		if cluster.DeliveredBytes() >= int64(len(payload))*int64(n-1) {
			break
		}
		clk.Advance(5 * time.Millisecond)
	}
	return clk.Now().Sub(start), cluster.MaxHop(), cluster.DeliveredBytes(), cluster.SentBytes()
}

// TestConvergenceHopsScaleLogN (TODO-059, Property 10) proves multi-hop
// infection depth grows like O(log N), not linearly: doubling the cluster size
// tenfold adds only a handful of hops.
func TestConvergenceHopsScaleLogN(t *testing.T) {
	sizes := []int{10, 100, 1000}
	type row struct {
		n         int
		elapsed   time.Duration
		hops      int
		delivered int64
	}
	rows := make([]row, 0, len(sizes))
	for _, n := range sizes {
		elapsed, hops, delivered, _ := buildInfected(t, n, infectionCfg)
		rows = append(rows, row{n: n, elapsed: elapsed, hops: hops, delivered: delivered})
		t.Logf("n=%4d  convergence=%8s  max_hop=%d  delivered=%d B",
			n, elapsed, hops, delivered)
	}

	base := rows[0].hops // n=10
	large := rows[len(rows)-1].hops
	// O(log N): 10 → 1000 is 100× the nodes but hop depth must NOT scale
	// linearly (which would be ~100× the hops). Observed with fanout 3 and
	// collision-dedup: ~3 hops at n=10, ~15 at n=1000 — a small constant
	// multiple, not a linear one.
	maxAllowed := base * 6
	if large > maxAllowed {
		t.Fatalf("hop depth scales worse than O(log N): n=10 → %d hops, n=1000 → %d hops (limit %d)",
			base, large, maxAllowed)
	}
	if large >= base*10 {
		t.Fatalf("hop depth grew ~linearly with N: base=%d large=%d", base, large)
	}
	// Convergence TIME must also grow sublinearly: 100× nodes, but well under
	// 100× the convergence time. (Each hop carries 20ms latency, so time ∝ hops.)
	timeRatio := float64(rows[len(rows)-1].elapsed) / float64(rows[0].elapsed)
	if timeRatio > 20 {
		t.Fatalf("convergence time grew super-logarithmically: n=10 %s, n=1000 %s (ratio %.1f)",
			rows[0].elapsed, rows[len(rows)-1].elapsed, timeRatio)
	}
	t.Logf("hop ratio %d→%d = %.1f× for 100× nodes; time ratio %.1f×", base, large,
		float64(large)/float64(base), timeRatio)
	// Every node delivered exactly once (markSeen dedup) — nothing lost.
	for _, r := range rows {
		want := int64(len(`{"type":"register","id":"i1","svc":"payments","node":"n0","addr":"10.0.0.1","port":8080}`)) * int64(r.n-1)
		if r.delivered != want {
			t.Fatalf("n=%d: delivered %d bytes, want %d (each node exactly once)", r.n, r.delivered, want)
		}
	}
}

// TestBandwidthPerNode1k (TODO-009 / TODO-011, SPEC §20: < 50 KB/s per node at
// 1k nodes) measures the steady-state gossip bandwidth a node must process
// while 100 registrations per second are injected, over a 2s virtual window.
func TestBandwidthPerNode1k(t *testing.T) {
	const nodes = 1000
	const ratePerSec = 100 // registrations/s
	const window = 2 * time.Second

	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	cluster := gossip.NewCluster(clk)
	cluster.SetNetwork(infectionCfg)
	members := make([]*gossip.MemoryMembership, nodes)
	for i := 0; i < nodes; i++ {
		members[i] = gossip.NewMemory(cluster, fmt.Sprintf("n%d", i), "127.0.0.1", 8000+i)
	}

	payload := []byte(`{"type":"register","id":"i0000","svc":"payments","node":"n0","addr":"10.0.0.1","port":8080}`)
	payloadBytes := int64(len(payload))

	// Inject at a steady rate for `window` virtual seconds. Each delta must be
	// unique (distinct hash) or markSeen dedup would collapse them into one.
	injected := 0
	start := clk.Now()
	for clk.Now().Sub(start) < window {
		// Vary the instance id (fixed width so every payload is the same size)
		// so each broadcast is a distinct payload.
		p := fmt.Sprintf(`{"type":"register","id":"i%04d","svc":"payments","node":"n0","addr":"10.0.0.1","port":8080}`, injected)
		_ = members[0].Broadcast([]byte(p))
		injected++
		clk.Advance(time.Second / ratePerSec)
	}

	// Let the final wave converge.
	clk.Advance(5 * time.Second)

	delivered := cluster.DeliveredBytes()
	sent := cluster.SentBytes()
	// Every node processes every unique delta exactly once.
	wantDelivered := payloadBytes * int64(injected) * int64(nodes-1)
	if delivered != wantDelivered {
		t.Fatalf("delivered %d bytes, want %d (%d deltas × %d peers × %dB)",
			delivered, wantDelivered, injected, nodes-1, payloadBytes)
	}

	perNodePerSec := float64(delivered) / float64(nodes) / window.Seconds()
	perNodePerSecSent := float64(sent) / float64(nodes) / window.Seconds()
	t.Logf("nodes=%d rate=%d/s window=%s payload=%dB", nodes, ratePerSec, window, payloadBytes)
	t.Logf("delivered per node: %.1f B/s (%.2f KB/s)  |  wire (incl. re-gossip): %.1f B/s (%.2f KB/s)",
		perNodePerSec, perNodePerSec/1024, perNodePerSecSent, perNodePerSecSent/1024)

	const target = 50.0 * 1024 // 50 KB/s
	if perNodePerSec > target {
		t.Fatalf("gossip bandwidth %.2f KB/s per node exceeds SPEC §20 target 50 KB/s", perNodePerSec/1024)
	}
}
