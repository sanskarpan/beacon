package mesh_test

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/mesh"
)

// TestControlPlaneMTLS_HTTP (TODO-035): an HTTP control-plane endpoint served
// with optional mTLS accepts an entitled agent and rejects a workload that is
// not in allowedClients.
func TestControlPlaneMTLS_HTTP(t *testing.T) {
	clk := clock.New()
	ca, err := mesh.NewCA(clk)
	if err != nil {
		t.Fatal(err)
	}
	serverID := mesh.Identity{Namespace: "prod", ServiceAccount: "server"}
	agentID := mesh.Identity{Namespace: "prod", ServiceAccount: "agent"}
	evilID := mesh.Identity{Namespace: "prod", ServiceAccount: "evil"}
	ca.Entitle("server", serverID.URI())
	ca.Entitle("agent", agentID.URI())
	ca.Entitle("evil", evilID.URI())

	// Only agent may call the control plane.
	srvTLS, err := mesh.ServerTLSConfig(ca, serverID, "server", []string{"agent"})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	httpSrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})}
	tlsLn := tls.NewListener(ln, srvTLS)
	go func() { _ = httpSrv.Serve(tlsLn) }()
	defer func() { _ = httpSrv.Close() }()

	agentTLS, err := mesh.ClientTLSConfig(ca, agentID, "agent")
	if err != nil {
		t.Fatal(err)
	}
	evilTLS, err := mesh.ClientTLSConfig(ca, evilID, "evil")
	if err != nil {
		t.Fatal(err)
	}

	get := func(cfg *tls.Config) (int, error) {
		client := &http.Client{Transport: &http.Transport{
			TLSClientConfig: cfg,
		}}
		resp, err := client.Get("https://" + ln.Addr().String() + "/")
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, nil
	}

	// Agent (allowed) → 200.
	code, err := get(agentTLS)
	if err != nil || code != http.StatusOK {
		t.Fatalf("agent call: code=%d err=%v (want 200)", code, err)
	}

	// Evil (not allowed) → TLS handshake rejection.
	done := make(chan error, 1)
	go func() {
		_, err := get(evilTLS)
		done <- err
	}()
	tmr := time.NewTimer(3 * time.Second)
	defer tmr.Stop()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("evil workload reached control plane — mTLS authn not enforced")
		}
	case <-tmr.C:
		t.Fatal("evil call did not fail")
	}
}

// TestControlPlaneMTLS_GRPCStyleHandshake: same enforcement at the TLS layer as
// a gRPC server would see via creds (no app-level check needed).
func TestControlPlaneMTLS_GRPCStyleHandshake(t *testing.T) {
	clk := clock.New()
	ca, _ := mesh.NewCA(clk)
	serverID := mesh.Identity{Namespace: "prod", ServiceAccount: "control"}
	agentID := mesh.Identity{Namespace: "prod", ServiceAccount: "agent"}
	ca.Entitle("control", serverID.URI())
	ca.Entitle("agent", agentID.URI())

	srvTLS, err := mesh.ServerTLSConfig(ca, serverID, "control", []string{"agent"})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		buf := make([]byte, 1)
		_, err = conn.Read(buf) // drives the TLS handshake server-side
		serverErr <- err
	}()

	agentTLS, _ := mesh.ClientTLSConfig(ca, agentID, "agent")
	conn, err := tls.Dial("tcp", ln.Addr().String(), agentTLS)
	if err != nil {
		t.Fatalf("agent handshake failed: %v", err)
	}
	_, _ = conn.Write([]byte("x"))
	_ = conn.Close()
	if err := <-serverErr; err != nil {
		t.Fatalf("server-side handshake failed: %v", err)
	}
}

// TestUnauthorizedWorkloadCannotGetCert (TODO-036, hard e2e): the CA/SDS refuses
// a workload that requests a SPIFFE identity it is not entitled to — both at
// the CA level and through the SDS API path.
func TestUnauthorizedWorkloadCannotGetCert(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(1_700_000_000, 0))
	ca, _ := mesh.NewCA(clk)
	good := mesh.Identity{Namespace: "prod", ServiceAccount: "payments"}
	evil := mesh.Identity{Namespace: "prod", ServiceAccount: "evil"}
	ca.Entitle("workload-1", good.URI())

	// 1) Direct CA sign: unauthorized must error.
	if _, err := ca.Sign("workload-1", evil); err == nil {
		t.Fatal("CA signed unauthorized identity")
	}
	// Entitled still works.
	if _, err := ca.Sign("workload-1", good); err != nil {
		t.Fatalf("entitled sign failed: %v", err)
	}

	// 2) SDS API path: unauthorized identity must fail at Fetch.
	sds := mesh.NewSDS(ca, clk)
	if _, err := sds.Fetch("workload-1", evil); err == nil {
		t.Fatal("SDS issued cert for unauthorized identity")
	}
	// 3) Over the ADS secret channel (full e2e in pkg/xds): also refused.
	if _, err := mesh.NewSDSXDS(sds).Get("workload-1", evil.URI()); err == nil {
		t.Fatal("ADS secret channel issued cert for unauthorized identity")
	}

	// Sanity: the same workload CAN get the entitled cert.
	res, err := sds.Fetch("workload-1", good)
	if err != nil {
		t.Fatalf("entitled SDS fetch failed: %v", err)
	}
	if len(res.CertChain) == 0 {
		t.Fatal("empty cert chain")
	}
}

var _ = fmt.Sprintf
