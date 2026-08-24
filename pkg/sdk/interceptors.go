package sdk

import (
	"context"

	"github.com/example/grpc-service/pkg/interceptors"
	"google.golang.org/grpc"
)

// ExternalChain wraps the gRPC-Service-with-Interceptors project chain
// (logging, metrics, panic recovery, optional auth/retry) for the client.
//
// beacon appends OutcomeReporter after this chain so passive health sees
// the same RPC outcomes as the application.
type ExternalChain struct {
	token string // optional bearer token for client auth interceptor
}

// NewExternalChain builds a chain from the interceptors project.
// token may be empty (auth interceptor becomes a no-op pass-through when empty
// via UnaryClientAuthInterceptorWithToken("")).
func NewExternalChain(token string) *ExternalChain {
	return &ExternalChain{token: token}
}

// Unary returns the client unary interceptor chain from the interceptors project.
func (e *ExternalChain) Unary() grpc.UnaryClientInterceptor {
	parts := []grpc.UnaryClientInterceptor{
		interceptors.UnaryClientLoggingInterceptor,
		interceptors.UnaryClientMetricsInterceptor,
	}
	if e.token != "" {
		parts = append(parts, interceptors.UnaryClientAuthInterceptorWithToken(e.token))
	}
	parts = append(parts, interceptors.UnaryClientRetryInterceptor(interceptors.DefaultRetryConfig()))
	return ChainFrom(parts...)
}

// Stream returns the client stream interceptor chain.
func (e *ExternalChain) Stream() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
		method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Compose stream interceptors: logging → metrics → optional auth.
		chain := streamer
		if e.token != "" {
			auth := interceptors.StreamClientAuthInterceptorWithToken(e.token)
			next := chain
			chain = func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
				method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return auth(ctx, desc, cc, method, next, opts...)
			}
		}
		metrics := interceptors.StreamClientMetricsInterceptor
		next := chain
		chain = func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
			method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return metrics(ctx, desc, cc, method, next, opts...)
		}
		logging := interceptors.StreamClientLoggingInterceptor
		return logging(ctx, desc, cc, method, chain, opts...)
	}
}

// Ensure ExternalChain implements InterceptorChain.
var _ InterceptorChain = (*ExternalChain)(nil)

// ClientDialOptions returns dial options: external interceptor chain + OutcomeReporter.
func (c *Client) ClientDialOptions(chain InterceptorChain) []grpc.DialOption {
	var unary []grpc.UnaryClientInterceptor
	if chain != nil {
		unary = append(unary, chain.Unary())
	}
	unary = append(unary, c.OutcomeReporter())
	opts := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(unary...),
	}
	if chain != nil {
		opts = append(opts, grpc.WithChainStreamInterceptor(chain.Stream()))
	}
	return opts
}

// DefaultServerUnaryInterceptors returns the server-side chain from the
// interceptors project: request-id → logging → metrics → panic recovery.
// Auth is omitted so local/dev discovery stays open; enable via ServerAuth.
func DefaultServerUnaryInterceptors() []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		interceptors.UnaryRequestIDInterceptor,
		interceptors.UnaryLoggingInterceptor,
		interceptors.UnaryMetricsInterceptor,
		interceptors.UnaryPanicRecoveryInterceptor,
	}
}

// DefaultServerStreamInterceptors mirrors the unary stack for streams.
func DefaultServerStreamInterceptors() []grpc.StreamServerInterceptor {
	return []grpc.StreamServerInterceptor{
		interceptors.StreamRequestIDInterceptor,
		interceptors.StreamLoggingInterceptor,
		interceptors.StreamMetricsInterceptor,
		interceptors.StreamPanicRecoveryInterceptor,
	}
}
