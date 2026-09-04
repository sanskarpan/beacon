package xds_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/mesh"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/xds"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

// TestADS_SDSMultiplexedOnStream (TODO-029):
// secrets are pushed over the SAME ADS stream (TypeSecret) with version/nonce;
// rotation at 50% of leaf lifetime triggers a new push with a new version.
func TestADS_SDSMultiplexedOnStream(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(1_700_000_000, 0))
	ca, err := mesh.NewCA(clk)
	if err != nil {
		t.Fatal(err)
	}
	id := mesh.Identity{Namespace: "prod", ServiceAccount: "payments"}
	ca.Entitle("envoy-1", id.URI())
	sds := mesh.NewSDS(ca, clk)

	cs := catalog.NewStore()
	st := store.NewMemory(cs, "ap")
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "e1", Service: "payments", Address: "10.0.0.1", Port: 8080, Health: catalog.HealthPassing,
	})

	ads := xds.NewGRPCServer(st, nil).WithSDS(mesh.NewSDSXDS(sds))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	go func() { _ = ads.Serve(lis) }()
	t.Cleanup(ads.Stop)

	client := dialTo(t, lis.Addr().String())
	defer client.Close()

	// 1) Subscribe to the secret type; a secret must arrive with version + nonce.
	if err := client.SendRequest(&xds.DiscoveryRequest{
		NodeID:        "envoy-1",
		TypeURL:       xds.TypeSecret,
		ResourceNames: []string{id.URI()},
	}); err != nil {
		t.Fatal(err)
	}
	r := readUntil(t, client, 5*time.Second, "envoy-1", func(r *xds.DiscoveryResponse) bool {
		return r.TypeURL == xds.TypeSecret && len(r.Resources) > 0
	})
	if r.VersionInfo == "" || r.Nonce == "" {
		t.Fatal("secret push missing version/nonce")
	}
	if r.Resources[0].Name != id.URI() {
		t.Fatalf("unexpected secret name %q", r.Resources[0].Name)
	}
	v1 := r.VersionInfo
	if err := client.ACK("envoy-1", r.TypeURL, r.VersionInfo, r.Nonce); err != nil {
		t.Fatal(err)
	}

	// 2) Rotate at 50% of leaf lifetime → next request must push a NEW version.
	clk.Advance(13 * time.Hour) // > 50% of 24h leaf
	if err := client.SendRequest(&xds.DiscoveryRequest{
		NodeID:  "envoy-1",
		TypeURL: xds.TypeSecret,
	}); err != nil {
		t.Fatal(err)
	}
	r2 := readUntil(t, client, 5*time.Second, "envoy-1", func(r *xds.DiscoveryResponse) bool {
		return r.TypeURL == xds.TypeSecret && r.VersionInfo != v1
	})
	if r2.VersionInfo == v1 {
		t.Fatal("rotation did not produce a new version")
	}
	if err := client.ACK("envoy-1", r2.TypeURL, r2.VersionInfo, r2.Nonce); err != nil {
		t.Fatal(err)
	}
	acks, nacks, pushes := client.Stats()
	t.Logf("SDS over ADS: acks=%d nacks=%d pushes=%d v1=%s v2=%s", acks, nacks, pushes, v1, r2.VersionInfo)
}

// TestSDS_UnauthorizedIdentityRejectedOverADS (TODO-036): a workload that is
// NOT entitled to a SPIFFE URI must not receive a cert over the ADS path.
func TestSDS_UnauthorizedIdentityRejectedOverADS(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(1_700_000_000, 0))
	ca, _ := mesh.NewCA(clk)
	good := mesh.Identity{Namespace: "prod", ServiceAccount: "payments"}
	evil := mesh.Identity{Namespace: "prod", ServiceAccount: "evil"}
	ca.Entitle("envoy-1", good.URI()) // only good is entitled
	sds := mesh.NewSDS(ca, clk)

	cs := catalog.NewStore()
	st := store.NewMemory(cs, "ap")
	ads := xds.NewGRPCServer(st, nil).WithSDS(mesh.NewSDSXDS(sds))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	go func() { _ = ads.Serve(lis) }()
	t.Cleanup(ads.Stop)

	client := dialTo(t, lis.Addr().String())
	defer client.Close()

	if err := client.SendRequest(&xds.DiscoveryRequest{
		NodeID:        "envoy-1",
		TypeURL:       xds.TypeSecret,
		ResourceNames: []string{evil.URI()},
	}); err != nil {
		t.Fatal(err)
	}
	// The evil secret must NOT be pushed: the snapshot build skips secrets that
	// fail entitlement. Wait briefly and assert no secret resource arrived.
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		r, err := client.RecvResponseTimeout(500 * time.Millisecond)
		if err != nil {
			break // quiescent
		}
		for _, res := range r.Resources {
			if res.Name == evil.URI() {
				t.Fatalf("unauthorized identity %q was pushed as a secret", evil.URI())
			}
		}
	}
}

func dialTo(t *testing.T, addr string) *xds.LiveClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	client, err := xds.DialADS(ctx, "passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(encoding.GetCodec(xds.JSONCodecName))),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
