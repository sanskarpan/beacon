// Package mesh provides SPIFFE identity, a simple CA, SDS, and intentions.
//
// Identity over IP: IPs are recycled within seconds in a container fleet, so
// IP-based authorization is authorizing "whatever is at 10.0.3.17 right now".
//
// Short-lived leaf certs (default 24h, rotated at 50%): a compromised cert
// expires before a CRL would propagate, so revocation is mostly unnecessary.
package mesh

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/url"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
)

// Identity is a SPIFFE workload identity.
// Format: spiffe://beacon.local/ns/<namespace>/sa/<account>
type Identity struct {
	TrustDomain    string
	Namespace      string
	ServiceAccount string
}

// URI returns the SPIFFE ID.
func (i Identity) URI() string {
	td := i.TrustDomain
	if td == "" {
		td = "beacon.local"
	}
	return fmt.Sprintf("spiffe://%s/ns/%s/sa/%s", td, i.Namespace, i.ServiceAccount)
}

// Certificate is an issued leaf.
type Certificate struct {
	Identity  Identity
	CertPEM   []byte
	ChainPEM  []byte
	KeyPEM    []byte
	NotBefore time.Time
	NotAfter  time.Time
}

// CA is an internal certificate authority.
type CA struct {
	mu               sync.Mutex
	clk              clock.Clock
	key              *ecdsa.PrivateKey
	cert             *x509.Certificate
	certPEM          []byte
	leafTTL          time.Duration
	serial           *big.Int
	entitlements     map[string]map[string]bool // node/workload → allowed SPIFFE URIs
	parent           *CA
	isIntermediate   bool
	insecureAllowAll bool // dev mode: allow any workload→SPIFFE when no entitlements (default false, secure)
}

// NewCA creates a root CA.
func NewCA(clk clock.Clock) (*CA, error) {
	if clk == nil {
		clk = clock.New()
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "beacon-ca"},
		NotBefore:             clk.Now().Add(-time.Hour),
		NotAfter:              clk.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{
		clk:              clk,
		key:              key,
		cert:             cert,
		certPEM:          pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		leafTTL:          24 * time.Hour,
		serial:           big.NewInt(1),
		entitlements:     make(map[string]map[string]bool),
		insecureAllowAll: true, // dev-mode default for backward compat; production must use NewCAProduction (fail-closed)
	}, nil
}

// NewCAProduction creates a root CA with fail-closed entitlement enforcement:
// Sign denies any workload→SPIFFE pair that was not explicitly entitled via
// Entitle. This is the constructor production deployments must use.
// NewCA is retained for dev/test backward compatibility.
func NewCAProduction(clk clock.Clock) (*CA, error) {
	ca, err := NewCA(clk)
	if err != nil {
		return nil, err
	}
	ca.SetInsecureAllowAll(false)
	return ca, nil
}

// SetInsecureAllowAll enables dev-mode where any workload may request any SPIFFE when no entitlements configured.
func (c *CA) SetInsecureAllowAll(v bool) {
	c.mu.Lock()
	c.insecureAllowAll = v
	c.mu.Unlock()
}

// Bundle returns the trust bundle PEM.
func (c *CA) Bundle() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	// For intermediate, bundle includes intermediate + root
	if c.parent != nil {
		// parent bundle already contains root chain
		p := c.parent.Bundle()
		out := make([]byte, 0, len(c.certPEM)+len(p))
		out = append(out, c.certPEM...)
		out = append(out, p...)
		return out
	}
	return append([]byte(nil), c.certPEM...)
}

// NewIntermediateCA creates an intermediate CA signed by this root.
func (c *CA) NewIntermediateCA(clk clock.Clock) (*CA, error) {
	if clk == nil {
		clk = c.clk
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "beacon-intermediate"},
		NotBefore:             clk.Now().Add(-time.Hour),
		NotAfter:              clk.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	c.mu.Lock()
	parentCert := c.cert
	parentKey := c.key
	c.mu.Unlock()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parentCert, &key.PublicKey, parentKey)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	// deep-copy entitlements to avoid shared map across two Mutexes (M21)
	entCopy := make(map[string]map[string]bool, len(c.entitlements))
	c.mu.Lock()
	for k, v := range c.entitlements {
		m := make(map[string]bool, len(v))
		for kk, vv := range v {
			m[kk] = vv
		}
		entCopy[k] = m
	}
	allowAll := c.insecureAllowAll
	c.mu.Unlock()
	return &CA{
		clk:              clk,
		key:              key,
		cert:             cert,
		certPEM:          pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		leafTTL:          c.leafTTL,
		serial:           big.NewInt(1),
		entitlements:     entCopy,
		parent:           c,
		isIntermediate:   true,
		insecureAllowAll: allowAll,
	}, nil
}

// Entitle allows a workload key to request a SPIFFE URI.
func (c *CA) Entitle(workload, spiffeURI string) {
	c.mu.Lock()
	if c.entitlements[workload] == nil {
		c.entitlements[workload] = make(map[string]bool)
	}
	c.entitlements[workload][spiffeURI] = true
	// if this is an intermediate, also entitle on parent so root sees it (and vice versa)
	// to keep entitlements in sync without sharing map (M21)
	parent := c.parent
	isInter := c.isIntermediate
	c.mu.Unlock()
	if isInter && parent != nil {
		parent.Entitle(workload, spiffeURI)
	}
	// if this is root, propagate to intermediates is not tracked (they pull on Sign)
}

// Sign issues a leaf cert with the SPIFFE URI as a SAN.
// Rejects if the workload is not entitled to the identity.
func (c *CA) Sign(workload string, id Identity) (*Certificate, error) {
	uri := id.URI()
	// For intermediates, check parent entitlements as well (without sharing map, M21)
	if c.isIntermediate && c.parent != nil {
		c.parent.mu.Lock()
		parentAllowed := c.parent.entitlements[workload]
		parentHas := parentAllowed != nil && parentAllowed[uri]
		parentCount := len(c.parent.entitlements)
		parentInsecure := c.parent.insecureAllowAll
		c.parent.mu.Unlock()
		if parentHas {
			// entitled via parent, proceed
		} else {
			c.mu.Lock()
			defer c.mu.Unlock()
			if allowed := c.entitlements[workload]; allowed != nil && allowed[uri] {
				// entitled via local
			} else {
				if parentCount == 0 && len(c.entitlements) == 0 && (parentInsecure || c.insecureAllowAll) {
					// dev-mode allow
				} else {
					return nil, fmt.Errorf("workload %q not entitled to %s", workload, uri)
				}
			}
			goto sign
		}
		// fall through to sign with local lock
		c.mu.Lock()
		defer c.mu.Unlock()
		goto sign
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if allowed := c.entitlements[workload]; allowed != nil && allowed[uri] {
		// entitled
	} else {
		// not entitled: allow only in explicit dev-mode with no entitlements at all
		if len(c.entitlements) == 0 && c.insecureAllowAll {
			// dev-mode allow
		} else {
			if len(c.entitlements) == 0 {
				return nil, fmt.Errorf("workload %q not entitled to %s (no entitlements configured, enable InsecureAllowAll for dev)", workload, uri)
			}
			return nil, fmt.Errorf("workload %q not entitled to %s", workload, uri)
		}
	}
sign:

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	c.serial.Add(c.serial, big.NewInt(1))
	now := c.clk.Now()
	u, _ := url.Parse(uri)
	tmpl := &x509.Certificate{
		SerialNumber: new(big.Int).Set(c.serial),
		Subject:      pkix.Name{CommonName: id.ServiceAccount},
		NotBefore:    now,
		NotAfter:     now.Add(c.leafTTL),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{u},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &leafKey.PublicKey, c.key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	chainPEM := certPEM
	if c.isIntermediate {
		// leaf chain includes intermediate cert
		chainPEM = append(append([]byte(nil), certPEM...), c.certPEM...)
	}
	return &Certificate{
		Identity:  id,
		CertPEM:   certPEM,
		ChainPEM:  chainPEM,
		KeyPEM:    pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		NotBefore: tmpl.NotBefore,
		NotAfter:  tmpl.NotAfter,
	}, nil
}

// ShouldRotate reports whether the cert is past 50% of its lifetime.
func ShouldRotate(cert *Certificate, now time.Time) bool {
	life := cert.NotAfter.Sub(cert.NotBefore)
	return !now.Before(cert.NotBefore.Add(life / 2))
}

// Action is Allow or Deny.
type Action string

const (
	Allow Action = "allow"
	Deny  Action = "deny"
)

// Intention is L4 authorization by identity.
type Intention struct {
	Source      string // "web" or "*"
	Destination string
	Action      Action
	Precedence  int // higher / more specific wins
}

// IntentionStore holds intentions.
type IntentionStore struct {
	mu   sync.RWMutex
	list []Intention
}

// NewIntentionStore creates an empty store.
func NewIntentionStore() *IntentionStore {
	return &IntentionStore{}
}

// Upsert adds or replaces an intention.
func (s *IntentionStore) Upsert(i Intention) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for idx, existing := range s.list {
		if existing.Source == i.Source && existing.Destination == i.Destination {
			s.list[idx] = i
			return
		}
	}
	s.list = append(s.list, i)
}

// Delete removes an intention.
func (s *IntentionStore) Delete(source, dest string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := s.list[:0]
	for _, i := range s.list {
		if i.Source != source || i.Destination != dest {
			keep = append(keep, i)
		}
	}
	s.list = keep
}

// List returns all intentions.
func (s *IntentionStore) List() []Intention {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Intention(nil), s.list...)
}

// Decide evaluates source → destination. More specific (higher precedence) wins.
// Default deny if no match.
func (s *IntentionStore) Decide(source, dest string) Action {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bestPrec := -1
	best := Deny
	for _, i := range s.list {
		if !match(i.Source, source) || !match(i.Destination, dest) {
			continue
		}
		prec := i.Precedence
		if prec == 0 {
			// auto: specific > wildcard
			prec = specificity(i.Source) + specificity(i.Destination)
		}
		if prec > bestPrec {
			bestPrec = prec
			best = i.Action
		}
	}
	return best
}

func match(pattern, value string) bool {
	return pattern == "*" || pattern == value
}

func specificity(s string) int {
	if s == "*" {
		return 0
	}
	return 10
}

// IdentityFromURI parses a SPIFFE URI into Identity.
func IdentityFromURI(uri string) (Identity, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return Identity{}, err
	}
	if u.Scheme != "spiffe" {
		return Identity{}, fmt.Errorf("not spiffe uri: %s", uri)
	}
	td := u.Host
	// path: /ns/<ns>/sa/<sa>
	parts := u.Path
	// expect /ns/<ns>/sa/<sa>
	var ns, sa string
	// split by /
	segs := []string{}
	for _, p := range splitPath(parts) {
		if p != "" {
			segs = append(segs, p)
		}
	}
	for i := 0; i < len(segs)-1; i += 2 {
		switch segs[i] {
		case "ns":
			ns = segs[i+1]
		case "sa":
			sa = segs[i+1]
		}
	}
	if ns == "" || sa == "" {
		return Identity{}, fmt.Errorf("invalid spiffe uri: %s", uri)
	}
	return Identity{TrustDomain: td, Namespace: ns, ServiceAccount: sa}, nil
}

func splitPath(p string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(p); i++ {
		if i == len(p) || p[i] == '/' {
			out = append(out, p[start:i])
			start = i + 1
		}
	}
	return out
}

// Root returns the CA itself for Bundle compat.
func (c *CA) Root() *CA { return c }
