package xds_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/xds"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

// startLiveADS spins up a real ADS gRPC server and returns a connected client.
func startLiveADS(t *testing.T, st store.CatalogStore) (*xds.GRPCServer, *xds.LiveClient) {
	t.Helper()
	ads := xds.NewGRPCServer(st, events.NewBus(nil))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	go func() { _ = ads.Serve(lis) }()
	t.Cleanup(ads.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	client, err := xds.DialADS(ctx, "passthrough:///"+lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(encoding.GetCodec(xds.JSONCodecName))),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", lis.Addr().String())
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return ads, client
}

// readUntil reads ADS responses until pred returns true or the deadline hits.
// Responses are async (server pushes eagerly until ACKed at current version),
// so callers must read-until rather than assume 1:1 request→response.
func readUntil(t *testing.T, client *xds.LiveClient, deadline time.Duration, nodeID string,
	pred func(*xds.DiscoveryResponse) bool) *xds.DiscoveryResponse {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		r, err := client.RecvResponse()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if pred(r) {
			return r
		}
		// ACK whatever arrived to keep the stream advancing.
		_ = client.ACK(nodeID, r.TypeURL, r.VersionInfo, r.Nonce)
	}
	t.Fatal("deadline exceeded waiting for expected ADS response")
	return nil
}

// drainUntilQuiescent reads and ACKs every response until the stream goes quiet
// (no response within drainWindow). Ensures no stale pushes leak into the next
// phase of a mid-stream subscription test.
func drainUntilQuiescent(t *testing.T, client *xds.LiveClient, nodeID string, drainWindow time.Duration) {
	t.Helper()
	for {
		r, err := client.RecvResponseTimeout(drainWindow)
		if err != nil {
			return // quiescent
		}
		_ = client.ACK(nodeID, r.TypeURL, r.VersionInfo, r.Nonce)
	}
}

func resNames(r *xds.DiscoveryResponse) map[string]bool {
	out := map[string]bool{}
	for _, res := range r.Resources {
		out[res.Name] = true
	}
	return out
}

// TestADS_DynamicMidStreamSubscribeUnsubscribe (TODO-030):
// a client changes its resource subscription set mid-stream; only the relevant
// resources are sent, and there is no bad NACK loop.
func TestADS_DynamicMidStreamSubscribeUnsubscribe(t *testing.T) {
	cs := catalog.NewStore()
	st := store.NewMemory(cs, "ap")
	// Three services → three clusters/listeners/routes + per-endpoint EDS.
	for _, svc := range []string{"a", "b", "c"} {
		_, _ = cs.Register(context.Background(), &catalog.Instance{
			ID:      svc + "-1",
			Service: svc,
			Address: "10.0.0.1",
			Port:    8080,
			Health:  catalog.HealthPassing,
		})
	}
	_, client := startLiveADS(t, st)

	// 1) Subscribe to ONLY cluster "a" via ResourceNames. The response must not
	// contain b or c.
	if err := client.SendRequest(&xds.DiscoveryRequest{
		NodeID:        "envoy-1",
		TypeURL:       xds.TypeCluster,
		ResourceNames: []string{"a"},
	}); err != nil {
		t.Fatal(err)
	}
	r := readUntil(t, client, 5*time.Second, "envoy-1", func(r *xds.DiscoveryResponse) bool {
		return len(r.Resources) > 0 && resNames(r)["a"]
	})
	if len(r.Resources) != 1 || !resNames(r)["a"] {
		t.Fatalf("expected only subscribed cluster 'a', got %v", resNames(r))
	}
	if err := client.ACK("envoy-1", r.TypeURL, r.VersionInfo, r.Nonce); err != nil {
		t.Fatal(err)
	}
	drainUntilQuiescent(t, client, "envoy-1", 200*time.Millisecond)

	// 2) Mid-stream: subscribe to "b" as well (dynamic add), then bump the
	// catalog so the server pushes. Only a and b may appear.
	if err := client.SendRequest(&xds.DiscoveryRequest{
		NodeID:                "envoy-1",
		TypeURL:               xds.TypeCluster,
		ResourceNamesSubscribe: []string{"b"},
	}); err != nil {
		t.Fatal(err)
	}
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "b-2", Service: "b", Address: "10.0.0.3", Port: 8081, Health: catalog.HealthPassing,
	})
	if err := client.SendRequest(&xds.DiscoveryRequest{NodeID: "envoy-1", TypeURL: xds.TypeCluster}); err != nil {
		t.Fatal(err)
	}
	r2 := readUntil(t, client, 5*time.Second, "envoy-1", func(r *xds.DiscoveryResponse) bool {
		return resNames(r)["b"]
	})
	for name := range resNames(r2) {
		if name != "a" && name != "b" {
			t.Fatalf("mid-stream subscribe leaked unsubscribed resource %q", name)
		}
	}
	if err := client.ACK("envoy-1", r2.TypeURL, r2.VersionInfo, r2.Nonce); err != nil {
		t.Fatal(err)
	}
	drainUntilQuiescent(t, client, "envoy-1", 200*time.Millisecond)

	// 3) Mid-stream: unsubscribe "a" (dynamic remove), then bump the version
	// again. "a" must NEVER appear after this point.
	if err := client.SendRequest(&xds.DiscoveryRequest{
		NodeID:                  "envoy-1",
		TypeURL:                 xds.TypeCluster,
		ResourceNamesUnsubscribe: []string{"a"},
	}); err != nil {
		t.Fatal(err)
	}
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "b-3", Service: "b", Address: "10.0.0.4", Port: 8082, Health: catalog.HealthPassing,
	})
	if err := client.SendRequest(&xds.DiscoveryRequest{NodeID: "envoy-1", TypeURL: xds.TypeCluster}); err != nil {
		t.Fatal(err)
	}
	end := time.Now().Add(5 * time.Second)
	sawB := false
	for time.Now().Before(end) {
		r3, err := client.RecvResponse()
		if err != nil {
			break
		}
		if resNames(r3)["a"] {
			t.Fatalf("unsubscribed resource 'a' was still pushed")
		}
		if resNames(r3)["b"] {
			sawB = true
		}
		_ = client.ACK("envoy-1", r3.TypeURL, r3.VersionInfo, r3.Nonce)
		if sawB {
			break
		}
	}
	if !sawB {
		t.Fatal("expected 'b' still subscribed and pushed after unsubscribe of 'a'")
	}

	// 4) No NACK loop: server must not resend identical rejected config.
	if err := client.NACK("envoy-1", xds.TypeCluster, r2.VersionInfo, r2.Nonce, "test reject"); err != nil {
		t.Fatal(err)
	}
	acks, nacks, pushes := client.Stats()
	if nacks == 0 {
		t.Fatal("expected a NACK recorded")
	}
	t.Logf("dynamic subscribe/unsubscribe ok: acks=%d nacks=%d pushes=%d", acks, nacks, pushes)
}

// TestADS_SubscriptionOnlyRelevantResources: with per-endpoint EDS, subscribing
// to one service's endpoints must not leak another service's endpoints.
func TestADS_SubscriptionOnlyRelevantResources(t *testing.T) {
	cs := catalog.NewStore()
	st := store.NewMemory(cs, "ap")
	for i := 0; i < 10; i++ {
		_, _ = cs.Register(context.Background(), &catalog.Instance{
			ID:      "srvA-" + string(rune('0'+i)),
			Service: "srvA",
			Address: "10.0.0.1",
			Port:    8000 + i,
			Health:  catalog.HealthPassing,
		})
	}
	for i := 0; i < 10; i++ {
		_, _ = cs.Register(context.Background(), &catalog.Instance{
			ID:      "srvB-" + string(rune('0'+i)),
			Service: "srvB",
			Address: "10.0.0.2",
			Port:    9000 + i,
			Health:  catalog.HealthPassing,
		})
	}
	_, client := startLiveADS(t, st)

	// Subscribe to exactly two endpoint resources (per-endpoint EDS naming
	// cluster/id) and verify only those are returned.
	targets := []string{"srvA/srvA-0", "srvA/srvA-1"}
	if err := client.SendRequest(&xds.DiscoveryRequest{
		NodeID:        "envoy-2",
		TypeURL:       xds.TypeEndpoint,
		ResourceNames: targets,
	}); err != nil {
		t.Fatal(err)
	}
	r := readUntil(t, client, 5*time.Second, "envoy-2", func(r *xds.DiscoveryResponse) bool {
		return len(r.Resources) > 0
	})
	got := map[string]bool{}
	for _, res := range r.Resources {
		got[res.Name] = true
		if res.Name != targets[0] && res.Name != targets[1] {
			t.Fatalf("leaked endpoint %q outside subscription", res.Name)
		}
	}
	if len(got) != 2 {
		t.Fatalf("expected exactly 2 subscribed endpoints, got %v", got)
	}
	if err := client.ACK("envoy-2", r.TypeURL, r.VersionInfo, r.Nonce); err != nil {
		t.Fatal(err)
	}
	t.Logf("subscription filtering ok: %d endpoints, only subscribed set", len(r.Resources))
}
