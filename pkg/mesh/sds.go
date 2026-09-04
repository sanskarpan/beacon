package mesh

import (
	"fmt"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
)

// SDSResource is a secret pushed over ADS (SDS).
type SDSResource struct {
	Name       string
	CertChain  []byte
	PrivateKey []byte
	CABundle   []byte
	Version    string
	NotAfter   time.Time
}

// SDS serves secrets for workloads, rotating at 50% of leaf lifetime.
type SDS struct {
	mu    sync.Mutex
	ca    *CA
	clk   clock.Clock
	certs map[string]*Certificate // spiffe URI → cert
	// workload → identity entitlement already on CA
}

// NewSDS creates an SDS bound to a CA.
func NewSDS(ca *CA, clk clock.Clock) *SDS {
	if clk == nil {
		clk = clock.New()
	}
	return &SDS{ca: ca, clk: clk, certs: make(map[string]*Certificate)}
}

// Fetch returns (and optionally rotates) the secret for a SPIFFE identity.
// Signing happens outside the cache lock so concurrent Fetch calls for
// different identities do not serialize on x509 creation.
func (s *SDS) Fetch(workload string, id Identity) (*SDSResource, error) {
	uri := id.URI()
	s.mu.Lock()
	cert, ok := s.certs[uri]
	now := s.clk.Now()
	needsSign := !ok || ShouldRotate(cert, now)
	s.mu.Unlock()
	if needsSign {
		c, err := s.ca.Sign(workload, id)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		if cur, ok := s.certs[uri]; !ok || ShouldRotate(cur, s.clk.Now()) {
			s.certs[uri] = c
			cert = c
		} else {
			cert = cur
		}
		s.mu.Unlock()
	}
	bundle := s.ca.Bundle()
	return &SDSResource{
		Name:       uri,
		CertChain:  cert.CertPEM,
		PrivateKey: cert.KeyPEM,
		CABundle:   bundle,
		Version:    fmt.Sprintf("%d", cert.NotAfter.Unix()),
		NotAfter:   cert.NotAfter,
	}, nil
}

// List returns all cached secret names (for SotW SDS).
func (s *SDS) List() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.certs))
	for k := range s.certs {
		out = append(out, k)
	}
	return out
}
