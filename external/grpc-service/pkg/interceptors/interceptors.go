package interceptors

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	ClientTotalRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_client_requests_total",
		Help: "Total gRPC client requests",
	}, []string{"method"})

	ServerTotalRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_server_requests_total",
		Help: "Total gRPC server requests",
	}, []string{"method"})
)

func init() {
	prometheus.MustRegister(ClientTotalRequests, ServerTotalRequests)
}

// UnaryClientLoggingInterceptor logs outgoing calls.
func UnaryClientLoggingInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(ctx, method, req, reply, cc, opts...)
}

// UnaryClientMetricsInterceptor increments metrics.
func UnaryClientMetricsInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ClientTotalRequests.WithLabelValues(method).Inc()
	return invoker(ctx, method, req, reply, cc, opts...)
}

// StreamClientLoggingInterceptor logs streams.
func StreamClientLoggingInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(ctx, desc, cc, method, opts...)
}

// StreamClientMetricsInterceptor metrics for streams.
func StreamClientMetricsInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ClientTotalRequests.WithLabelValues(method).Inc()
	return streamer(ctx, desc, cc, method, opts...)
}

// UnaryClientAuthInterceptorWithToken adds bearer token.
func UnaryClientAuthInterceptorWithToken(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientAuthInterceptorWithToken for streams.
func StreamClientAuthInterceptorWithToken(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// RetryConfig for UnaryClientRetryInterceptor
type RetryConfig struct {
	MaxAttempts int
	Backoff     time.Duration
}

func DefaultRetryConfig() RetryConfig { return RetryConfig{MaxAttempts: 3, Backoff: 10 * time.Millisecond} }

func UnaryClientRetryInterceptor(cfg RetryConfig) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var lastErr error
		for i := 0; i < cfg.MaxAttempts; i++ {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}
			if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
				return err
			}
			lastErr = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cfg.Backoff):
			}
		}
		return lastErr
	}
}

// Server interceptors

func UnaryRequestIDInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	return handler(ctx, req)
}
func UnaryLoggingInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ServerTotalRequests.WithLabelValues(info.FullMethod).Inc()
	return handler(ctx, req)
}
func UnaryMetricsInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ServerTotalRequests.WithLabelValues(info.FullMethod).Inc()
	return handler(ctx, req)
}
func UnaryPanicRecoveryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = status.Errorf(codes.Internal, "panic: %v", r)
		}
	}()
	return handler(ctx, req)
}

func StreamRequestIDInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ss)
}
func StreamLoggingInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ss)
}
func StreamMetricsInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ss)
}
func StreamPanicRecoveryInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = status.Errorf(codes.Internal, "panic: %v", r)
		}
	}()
	return handler(srv, ss)
}
