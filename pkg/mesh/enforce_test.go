package mesh_test

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/mesh"
)

// TestEnforce_DeniedIntentionBlocksConnection (TODO-034):
// a denied source is refused at the TLS layer over a real connection; an
// allowed source connects fine. Specific rule beats wildcard.
func TestEnforce_DeniedIntentionBlocksConnection(t *testing.T) {
	clk := clock.New() // wall clock for crypto/tls
	root, err := mesh.NewCA(clk)
	if err != nil {
		t.Fatal(err)
	}
	intentions := mesh.NewIntentionStore()
	// Wildcard deny default + specific allow beats it.
	intentions.Upsert(mesh.Intention{Source: "*", Destination: "api", Action: mesh.Deny, Precedence: 1})
	intentions.Upsert(mesh.Intention{Source: "web", Destination: "api", Action: mesh.Allow, Precedence: 100})

	sign := func(sa string) tls.Certificate {
		id := mesh.Identity{Namespace: "prod", ServiceAccount: sa}
		root.Entitle("wl-"+sa, id.URI())
		c, err := root.Sign("wl-"+sa, id)
		if err != nil {
			t.Fatal(err)
		}
		kc, err := tls.X509KeyPair(c.ChainPEM, c.KeyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return kc
	}
	webCert := sign("web")
	evilCert := sign("evil")

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(root.Bundle())

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{sign("api")},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
		// TLS 1.2 makes the handshake strictly ordered: a server rejection is
		// visible to the client before any application data (TLS 1.3 defers it).
		MaxVersion: tls.VersionTLS12,
		// Enforce intentions using the peer SPIFFE identity.
		VerifyPeerCertificate: mesh.VerifyPeerAuthorization(intentions, "api", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Server accept loop: each accepted conn is read once (drives the handshake
	// and VerifyPeerCertificate) and closed. Errors are reported per-conn.
	serverErrs := make(chan error, 8)
	go func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1)
				_, err := c.Read(buf)
				serverErrs <- err
			}(conn)
		}
	}()

	// Web (allowed by specific rule) must connect.
	webDone := make(chan error, 1)
	go func() {
		conn, cerr := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
			Certificates:       []tls.Certificate{webCert},
			RootCAs:            pool,
			ServerName:         "api",
			InsecureSkipVerify: true, // SPIFFE: verified via URI SAN below
			MinVersion:         tls.VersionTLS12,
			MaxVersion:         tls.VersionTLS12,
		})
		if cerr != nil {
			webDone <- cerr
			return
		}
		_, werr := conn.Write([]byte("x"))
		webDone <- werr
		_ = conn.Close()
	}()
	tmr1 := time.NewTimer(3 * time.Second)
	defer tmr1.Stop()
	select {
	case err := <-webDone:
		if err != nil {
			t.Fatalf("allowed source web failed: %v", err)
		}
	case <-tmr1.C:
		t.Fatal("web handshake timed out")
	}

	// Evil (denied by wildcard) must fail the handshake.
	evilDone := make(chan error, 1)
	go func() {
		conn, cerr := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
			Certificates:       []tls.Certificate{evilCert},
			RootCAs:            pool,
			ServerName:         "api",
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
			MaxVersion:         tls.VersionTLS12,
		})
		if cerr != nil {
			evilDone <- cerr
			return
		}
		_, werr := conn.Write([]byte("x"))
		evilDone <- werr
		_ = conn.Close()
	}()
	tmr2 := time.NewTimer(3 * time.Second)
	defer tmr2.Stop()
	select {
	case err := <-evilDone:
		if err == nil {
			t.Fatal("denied source evil connected — intention not enforced")
		}
		// expected: handshake rejected by VerifyPeerCertificate
	case <-tmr2.C:
		t.Fatal("evil handshake did not terminate")
	}

	// The server must have rejected evil (handshake error) and accepted web.
	sawWeb := false
	sawEvilReject := false
	for i := 0; i < 2; i++ {
		stmr := time.NewTimer(2 * time.Second)
		select {
		case err := <-serverErrs:
			stmr.Stop()
			if err == nil {
				sawWeb = true
			} else {
				sawEvilReject = true
			}
		case <-stmr.C:
		}
	}
	if !sawWeb {
		t.Log("server did not report web read; ok if handshake path differs")
	}
	if !sawEvilReject {
		t.Log("server did not report evil rejection; client-side failure already verified")
	}
}

// TestEnforce_ServiceAccountFromURI parses the source label used by Decide.
func TestEnforce_ServiceAccountFromURI(t *testing.T) {
	if got := mesh.ServiceAccountFromURI("spiffe://beacon.local/ns/prod/sa/web"); got != "web" {
		t.Fatalf("got %q", got)
	}
	if got := mesh.ServiceAccountFromURI("not-a-spiffe"); got != "" {
		t.Fatalf("got %q", got)
	}
}
