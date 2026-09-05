package mesh

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ServerTLSFromFiles loads an operator-provided server certificate. When
// clientCAFile is set, mutual TLS is required and clients must chain to it.
func ServerTLSFromFiles(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("mesh: both TLS certificate and key are required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("mesh: load TLS key pair: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	if clientCAFile == "" {
		return cfg, nil
	}
	// The CA path is explicitly supplied by the operator at startup.
	//nolint:gosec // G304: runtime TLS configuration intentionally reads this operator-provided path.
	pem, err := os.ReadFile(clientCAFile)
	if err != nil {
		return nil, fmt.Errorf("mesh: read client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("mesh: client CA contains no certificates")
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.RequireAndVerifyClientCert
	return cfg, nil
}
