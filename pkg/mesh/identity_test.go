package mesh_test

import (
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/mesh"
)

func TestSPIFFECert(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(1_700_000_000, 0))
	ca, err := mesh.NewCA(clk)
	if err != nil {
		t.Fatal(err)
	}
	id := mesh.Identity{Namespace: "prod", ServiceAccount: "payments"}
	ca.Entitle("workload-1", id.URI())
	cert, err := ca.Sign("workload-1", id)
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.CertPEM) == 0 || len(cert.KeyPEM) == 0 {
		t.Fatal("empty pem")
	}
	if id.URI() != "spiffe://beacon.local/ns/prod/sa/payments" {
		t.Fatal(id.URI())
	}
}

func TestEntitlementDenied(t *testing.T) {
	ca, _ := mesh.NewCA(nil)
	ca.Entitle("w1", "spiffe://beacon.local/ns/prod/sa/a")
	_, err := ca.Sign("w1", mesh.Identity{Namespace: "prod", ServiceAccount: "b"})
	if err == nil {
		t.Fatal("should deny")
	}
}

func TestRotationAtHalfLife(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	ca, _ := mesh.NewCA(clk)
	cert, err := ca.Sign("w", mesh.Identity{Namespace: "default", ServiceAccount: "web"})
	if err != nil {
		t.Fatal(err)
	}
	mid := cert.NotBefore.Add(cert.NotAfter.Sub(cert.NotBefore) / 2)
	if !mesh.ShouldRotate(cert, mid) {
		t.Fatal("should rotate at 50%")
	}
	if mesh.ShouldRotate(cert, cert.NotBefore.Add(time.Hour)) {
		t.Fatal("should not rotate early")
	}
}

func TestIntentionPrecedence(t *testing.T) {
	s := mesh.NewIntentionStore()
	s.Upsert(mesh.Intention{Source: "*", Destination: "db", Action: mesh.Deny, Precedence: 1})
	s.Upsert(mesh.Intention{Source: "api", Destination: "db", Action: mesh.Allow, Precedence: 100})
	if s.Decide("api", "db") != mesh.Allow {
		t.Fatal("specific allow should win")
	}
	if s.Decide("other", "db") != mesh.Deny {
		t.Fatal("wildcard deny")
	}
}
