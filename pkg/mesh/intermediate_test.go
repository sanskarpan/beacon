package mesh_test

import (
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/mesh"
)

// TestIntermediateCAHierarchy (TODO-033): root → intermediate → leaf; the leaf
// is signed by the intermediate, and Bundle() distributes the chain.
func TestIntermediateCAHierarchy(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(1_700_000_000, 0))
	root, err := mesh.NewCA(clk)
	if err != nil {
		t.Fatal(err)
	}
	inter, err := root.NewIntermediateCA(clk)
	if err != nil {
		t.Fatal(err)
	}
	id := mesh.Identity{Namespace: "prod", ServiceAccount: "payments"}
	root.Entitle("workload-1", id.URI())

	cert, err := inter.Sign("workload-1", id)
	if err != nil {
		t.Fatal(err)
	}

	// Leaf must be verifiable against the root via the chain.
	if err := verifyChain(root, inter, cert, clk.Now()); err != nil {
		t.Fatalf("chain verification failed: %v", err)
	}

	// Root bundle = trust anchor (root only).
	if !pemContains(root.Bundle(), "beacon-ca") || pemContains(root.Bundle(), "beacon-intermediate") {
		t.Fatal("root bundle should contain only the root")
	}
	// Intermediate's bundle must carry BOTH the intermediate and the root
	// (TODO-033: bundle distribution includes chain).
	interBundle := inter.Bundle()
	if !pemContains(interBundle, "beacon-ca") || !pemContains(interBundle, "beacon-intermediate") {
		t.Fatal("intermediate bundle missing chain")
	}
	// The leaf chain presented to peers includes the intermediate.
	if !pemContains(cert.ChainPEM, "beacon-intermediate") {
		t.Fatal("leaf ChainPEM missing intermediate")
	}
}

// TestIntermediateEntitlementEnforced: entitlements live on the root; an
// intermediate must not silently issue for an unauthorized workload.
func TestIntermediateEntitlementEnforced(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(1_700_000_000, 0))
	root, _ := mesh.NewCA(clk)
	inter, _ := root.NewIntermediateCA(clk)
	id := mesh.Identity{Namespace: "prod", ServiceAccount: "payments"}
	root.Entitle("good", id.URI())

	if _, err := inter.Sign("good", id); err != nil {
		t.Fatalf("entitled workload denied: %v", err)
	}
	if _, err := inter.Sign("evil", id); err == nil {
		t.Fatal("intermediate issued cert for unentitled workload")
	}
}

func verifyChain(root, inter *mesh.CA, cert *mesh.Certificate, now time.Time) error {
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(root.Bundle())
	// Intermediates pool: everything except the root (leaf chain includes the
	// intermediate; the root is the trust anchor).
	intermediates := x509.NewCertPool()
	intermediates.AppendCertsFromPEM(inter.Bundle())

	var leaf *x509.Certificate
	rest := cert.ChainPEM
	for len(rest) > 0 {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return err
		}
		if !c.IsCA && leaf == nil {
			leaf = c
		}
	}
	if leaf == nil {
		return x509.CertificateInvalidError{Reason: x509.NotAuthorizedToSign}
	}
	_, err := leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		CurrentTime:   now,
	})
	return err
}

func pemContains(pemBytes []byte, cn string) bool {
	rest := pemBytes
	for len(rest) > 0 {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err == nil && c.Subject.CommonName == cn {
			return true
		}
	}
	return false
}
