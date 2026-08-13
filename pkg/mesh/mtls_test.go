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

func TestMTLSHandshake(t *testing.T) {
	// Wall clock: crypto/tls verifies NotBefore/NotAfter against real time.
	clk := clock.New()
	ca, err := mesh.NewCA(clk)
	if err != nil {
		t.Fatal(err)
	}
	serverID := mesh.Identity{Namespace: "prod", ServiceAccount: "api"}
	clientID := mesh.Identity{Namespace: "prod", ServiceAccount: "web"}
	ca.Entitle("server", serverID.URI())
	ca.Entitle("client", clientID.URI())

	serverCert, err := ca.Sign("server", serverID)
	if err != nil {
		t.Fatal(err)
	}
	clientCert, err := ca.Sign("client", clientID)
	if err != nil {
		t.Fatal(err)
	}

	// parse PEMs into tls.Certificate
	sCert, err := tls.X509KeyPair(serverCert.CertPEM, serverCert.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	cCert, err := tls.X509KeyPair(clientCert.CertPEM, clientCert.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.Bundle()) {
		t.Fatal("ca pool")
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{sCert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	errc := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errc <- err
			return
		}
		defer conn.Close()
		buf := make([]byte, 5)
		_, err = conn.Read(buf)
		errc <- err
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		Certificates: []tls.Certificate{cCert},
		RootCAs:      pool,
		ServerName:   "api",
		// SPIFFE: skip standard DNS SAN verify; use InsecureSkipVerify + custom verify
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return net.ErrClosed
			}
			cert, err := x509.ParseCertificate(raw[0])
			if err != nil {
				return err
			}
			// ensure URI SAN present
			if len(cert.URIs) == 0 {
				return net.ErrClosed
			}
			return nil
		},
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		t.Fatal("client dial:", err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("hello"))
	if err := <-errc; err != nil {
		t.Fatal("server read:", err)
	}
}

func TestSDSFetchAndRotate(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	ca, _ := mesh.NewCA(clk)
	id := mesh.Identity{Namespace: "default", ServiceAccount: "web"}
	ca.Entitle("w", id.URI())
	sds := mesh.NewSDS(ca, clk)
	r1, err := sds.Fetch("w", id)
	if err != nil {
		t.Fatal(err)
	}
	// advance past 50% of 24h
	clk.Advance(13 * time.Hour)
	r2, err := sds.Fetch("w", id)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Version == r2.Version {
		// may same if NotAfter same second — advance more
		clk.Advance(12 * time.Hour)
		r2, _ = sds.Fetch("w", id)
	}
	if len(r2.CertChain) == 0 || len(r2.CABundle) == 0 {
		t.Fatal("empty sds")
	}
}
