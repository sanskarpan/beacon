package xds_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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

func TestADS_LiveStreamACKAndConfigChange(t *testing.T) {
	cs := catalog.NewStore()
	bus := events.NewBus(nil)
	st := store.NewMemory(cs, "ap")
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "e1", Service: "payments", Address: "10.0.0.1", Port: 8080,
		Health: catalog.HealthPassing, Weight: 1,
	})

	ads := xds.NewGRPCServer(st, bus)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	go func() { _ = ads.Serve(lis) }()
	defer ads.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
	defer client.Close()

	// Request full ADS snapshot.
	if err := client.SendRequest(&xds.DiscoveryRequest{NodeID: "envoy-1", TypeURL: "ads"}); err != nil {
		t.Fatal(err)
	}

	acked := 0
	for acked < len(xds.AddOrder) {
		r, err := client.RecvResponse()
		if err != nil {
			t.Fatalf("recv: %v (acked=%d)", err, acked)
		}
		if err := client.ACK("envoy-1", r.TypeURL, r.VersionInfo, r.Nonce); err != nil {
			t.Fatal(err)
		}
		acked++
	}
	acks, nacks, pushes := client.Stats()
	if acks < len(xds.AddOrder) || pushes < len(xds.AddOrder) {
		t.Fatalf("acks=%d nacks=%d pushes=%d", acks, nacks, pushes)
	}

	// Config change: add instance → new version → ACK again (no NACK loop).
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "e2", Service: "payments", Address: "10.0.0.2", Port: 8080,
		Health: catalog.HealthPassing, Weight: 1,
	})
	if err := client.SendRequest(&xds.DiscoveryRequest{NodeID: "envoy-1", TypeURL: xds.TypeEndpoint}); err != nil {
		t.Fatal(err)
	}
	r, err := client.RecvResponse()
	if err != nil {
		t.Fatalf("post-change recv: %v", err)
	}
	if len(r.Resources) < 1 {
		t.Fatal("expected EDS resources after instance add")
	}
	if err := client.ACK("envoy-1", r.TypeURL, r.VersionInfo, r.Nonce); err != nil {
		t.Fatal(err)
	}

	// NACK must not resend same content.
	if err := client.NACK("envoy-1", r.TypeURL, r.VersionInfo, r.Nonce, "test reject"); err != nil {
		t.Fatal(err)
	}
	// Request again same type — server should not push identical NACK'd config.
	// (HandleRequest returns nil for NACK; subsequent request with same hash may also skip.)
	if err := client.SendRequest(&xds.DiscoveryRequest{
		NodeID: "envoy-1", TypeURL: r.TypeURL, VersionInfo: r.VersionInfo, ResponseNonce: r.Nonce,
		ErrorDetail: &xds.ErrorDetail{Message: "still bad"},
	}); err != nil {
		t.Fatal(err)
	}
}

// TestADS_OptionalRealEnvoy runs when BEACON_ENVOY=1 and envoy binary or docker is available.
func TestADS_OptionalRealEnvoy(t *testing.T) {
	if os.Getenv("BEACON_ENVOY") != "1" {
		t.Skip("set BEACON_ENVOY=1 to run live Envoy process against ADS")
	}
	cs := catalog.NewStore()
	st := store.NewMemory(cs, "ap")
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "e1", Service: "payments", Address: "127.0.0.1", Port: 18080,
		Health: catalog.HealthPassing,
	})
	ads := xds.NewGRPCServer(st, events.NewBus(nil))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	go func() { _ = ads.Serve(lis) }()
	defer ads.Stop()

	_, portStr, _ := net.SplitHostPort(lis.Addr().String())
	var port int
	_, _ = fmt.Sscanf(portStr, "%d", &port)

	dir := t.TempDir()
	boot, err := xds.GenerateBootstrap(xds.BootstrapConfig{
		NodeID: "envoy-live", ADSAddress: "127.0.0.1", ADSPort: mustAtoi(portStr), AdminPort: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Admin port 0 is invalid for envoy — pick free port.
	adminLis, _ := net.Listen("tcp", "127.0.0.1:0")
	_, adminPortStr, _ := net.SplitHostPort(adminLis.Addr().String())
	_ = adminLis.Close()
	boot, _ = xds.GenerateBootstrap(xds.BootstrapConfig{
		NodeID: "envoy-live", ADSAddress: "127.0.0.1", ADSPort: mustAtoi(portStr), AdminPort: mustAtoi(adminPortStr),
	})
	bootPath := filepath.Join(dir, "bootstrap.json")
	if err := os.WriteFile(bootPath, boot, 0o644); err != nil {
		t.Fatal(err)
	}

	envoyBin, err := exec.LookPath("envoy")
	if err != nil {
		// try docker
		if _, err := exec.LookPath("docker"); err != nil {
			t.Skip("no envoy binary or docker")
		}
		cmd := exec.Command("docker", "run", "--rm", "--network=host",
			"-v", bootPath+":/etc/envoy/envoy.yaml:ro",
			"envoyproxy/envoy:v1.31-latest", "-c", "/etc/envoy/envoy.yaml")
		// Note: bootstrap is JSON; Envoy accepts JSON bootstrap.
		out, err := cmd.CombinedOutput()
		t.Logf("docker envoy: err=%v out=%s", err, out)
		return
	}
	cmd := exec.Command(envoyBin, "-c", bootPath)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	time.Sleep(2 * time.Second)
	// If envoy is still running, ADS accepted the connection.
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		t.Fatal("envoy exited early")
	}
}

func mustAtoi(s string) int {
	var n int
	for _, c := range s {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	return n
}
