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
		chain := invoker
		for i := len(interceptors) - 1; i >= 0; i-- {
			ic := interceptors[i]
			next := chain
			chain = func(ctx context.Context, method string, req, reply any,
				cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				return ic(ctx, method, req, reply, cc, next, opts...)
			}
		}
		return chain(ctx, method, req, reply, cc, opts...)
	}
}
