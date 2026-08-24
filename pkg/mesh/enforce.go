// Enforcement: intentions are L4 authorization rules enforced on the live data
// plane by checking the peer's SPIFFE identity (from the mTLS cert) against the
// IntentionStore at connection time. This is the same decision the Envoy RBAC
// filter encodes (pkg/xds RBACFilter), just evaluated in-process so tests and
// non-Envoy workloads enforce it directly.
package mesh

import (
	"crypto/x509"
	"fmt"
	"net"
	"strings"
)

// PeerIdentity extracts the SPIFFE URI from a peer certificate.
// Returns empty string if the cert has no URI SAN.
func PeerIdentity(cert *x509.Certificate) string {
	for _, u := range cert.URIs {
		if u != nil && strings.HasPrefix(u.String(), "spiffe://") {
			return u.String()
		}
	}
	return ""
}

// VerifyPeerAuthorization returns a tls.Config.VerifyPeerCertificate callback
// that rejects connections whose peer SPIFFE identity is denied by intentions
// for the given destination service (TODO-034: denied intention blocks the
// connection; specific rule beats wildcard). The first raw cert is the peer leaf.
//
// dest is the service name this server represents (e.g. "api"). id is the
// expected service account used to build the source label for matching
// (intentions match on service accounts like "web" → "api").
func VerifyPeerAuthorization(intentions *IntentionStore, destService, srcAccountField string) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return net.ErrClosed
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		spiffeID := PeerIdentity(leaf)
		if spiffeID == "" {
			return fmt.Errorf("mesh: peer cert has no SPIFFE identity")
		}
		source := ServiceAccountFromURI(spiffeID)
		if source == "" {
			return fmt.Errorf("mesh: cannot derive source from %s", spiffeID)
		}
		if intentions.Decide(source, destService) != Allow {
			return fmt.Errorf("mesh: intention denied: %s -> %s", source, destService)
		}
		return nil
	}
}

// ServiceAccountFromURI returns the sa segment of a SPIFFE URI.
func ServiceAccountFromURI(uri string) string {
	id, err := IdentityFromURI(uri)
	if err != nil {
		return ""
	}
	return id.ServiceAccount
}
