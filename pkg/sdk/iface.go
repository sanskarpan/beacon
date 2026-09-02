package sdk

import (
	"context"

	"google.golang.org/grpc"
)

// InterceptorChain is the contract with the gRPC-interceptors project.
// Its chain is reused; beacon adds OutcomeReporter for passive health.
type InterceptorChain interface {
	Unary() grpc.UnaryClientInterceptor
	Stream() grpc.StreamClientInterceptor
}

// ChainFrom builds a unary chain from individual interceptors.
func ChainFrom(interceptors ...grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		next := invoker
		for i := len(interceptors) - 1; i >= 0; i-- {
			next = func(idx int, nxt grpc.UnaryInvoker) grpc.UnaryInvoker {
				return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
					return interceptors[idx](ctx, method, req, reply, cc, nxt, opts...)
				}
			}(i, next)
		}
		return next(ctx, method, req, reply, cc, opts...)
	}
}
