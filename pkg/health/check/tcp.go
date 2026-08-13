package check

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
)

// TCPCheck dials an address and immediately closes.
type TCPCheck struct {
	Addr    string
	Timeout time.Duration
}

// Run dials TCP.
func (c *TCPCheck) Run(ctx context.Context) (catalog.HealthStatus, string, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", c.Addr)
	if err != nil {
		return catalog.HealthCritical, err.Error(), nil
	}
	_ = conn.Close()
	return catalog.HealthPassing, fmt.Sprintf("tcp ok %s", c.Addr), nil
}
