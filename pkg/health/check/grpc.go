package check

import (
	"context"
	"fmt"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// GRPCCheck calls grpc.health.v1.Health/Check.
type GRPCCheck struct {
	Target      string
	ServiceName string
	Timeout     time.Duration
	// Dial is optional override for tests.
	Dial func(ctx context.Context, target string) (*grpc.ClientConn, error)
}

// Run probes gRPC health.
func (c *GRPCCheck) Run(ctx context.Context) (catalog.HealthStatus, string, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dial := c.Dial
	if dial == nil {
		dial = func(ctx context.Context, target string) (*grpc.ClientConn, error) {
			// WithBlock is load-bearing here: the probe must block until
			// connected or ctx timeout; NewClient would always succeed and
			// mask down backends.
			return grpc.DialContext(ctx, target, //nolint:staticcheck
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(), //nolint:staticcheck
			)
		}
	}
	conn, err := dial(ctx, c.Target)
	if err != nil {
		return catalog.HealthCritical, err.Error(), nil
	}
	defer func() { _ = conn.Close() }()

	cli := healthpb.NewHealthClient(conn)
	resp, err := cli.Check(ctx, &healthpb.HealthCheckRequest{Service: c.ServiceName})
	if err != nil {
		return catalog.HealthCritical, err.Error(), nil
	}
	switch resp.Status {
	case healthpb.HealthCheckResponse_SERVING:
		return catalog.HealthPassing, "SERVING", nil
	case healthpb.HealthCheckResponse_NOT_SERVING:
		return catalog.HealthCritical, "NOT_SERVING", nil
	default:
		return catalog.HealthCritical, fmt.Sprintf("status=%v", resp.Status), nil
	}
}
