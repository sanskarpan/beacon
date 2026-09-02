package mesh

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
)

// ServerTLSConfig builds an optional mTLS *tls.Config for the control plane
// (HTTP/gRPC server APIs, TODO-035). It serves the CA-issued server identity
// cert and REQUIRES a client cert from the same trust domain. When
// allowedClients is non-empty, the peer SPIFFE service account must be one of
// them (agent→server authn).
//
// Callers pass this to http.Server.TLSConfig / grpc.Creds, then serve TLS.
func ServerTLSConfig(ca *CA, serverID Identity, serverWorkload string, allowedClients []string) (*tls.Config, error) {
	cert, err := ca.Sign(serverWorkload, serverID)
	if err != nil {
		return nil, err
	}
	kc, err := tls.X509KeyPair(cert.CertPEM, cert.KeyPEM)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.Bundle())

	allowed := map[string]bool{}
	for _, a := range allowedClients {
		allowed[a] = true
	}

	verify := func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("mesh: no client cert presented")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		sa := ServiceAccountFromURI(PeerIdentity(leaf))
		if sa == "" {
			return fmt.Errorf("mesh: client cert has no SPIFFE identity")
		}
		if len(allowed) > 0 && !allowed[sa] {
			return fmt.Errorf("mesh: service account %q not allowed to call control plane", sa)
		}
		return nil
	}

	return &tls.Config{
		Certificates:          []tls.Certificate{kc},
		ClientCAs:             pool,
		ClientAuth:            tls.RequireAndVerifyClientCert,
		MinVersion:            tls.VersionTLS12,
		VerifyPeerCertificate: verify,
	}, nil
}

// ClientTLSConfig builds the client half of control-plane mTLS (agent / CLI).
func ClientTLSConfig(ca *CA, clientID Identity, clientWorkload string) (*tls.Config, error) {
	cert, err := ca.Sign(clientWorkload, clientID)
	if err != nil {
		return nil, err
	}
	kc, err := tls.X509KeyPair(cert.CertPEM, cert.KeyPEM)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca.Bundle())

	verifyPeer := func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("mesh: no server cert presented")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		// Verify chain against our CA bundle
		opts := x509.VerifyOptions{
			Roots:     pool,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageAny},
		}
		if _, err := leaf.Verify(opts); err != nil {
			return fmt.Errorf("mesh: server cert not trusted: %w", err)
		}
		if PeerIdentity(leaf) == "" {
			return fmt.Errorf("mesh: server cert has no SPIFFE identity")
		}
		return nil
	}

	return &tls.Config{
		Certificates: []tls.Certificate{kc},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
		// SPIFFE URI SANs are not DNS names; use custom verification
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: verifyPeer,
	}, nil
}

// PeerIdentityFromConn returns the SPIFFE service account of the mTLS peer on
// an established *tls.Conn (empty if none) — usable by HTTP handlers for
// identity-based routing after the handshake.
func PeerIdentityFromConn(conn *tls.Conn) string {
	if conn == nil {
		return ""
	}
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return ""
	}
	return ServiceAccountFromURI(PeerIdentity(state.PeerCertificates[0]))
}
