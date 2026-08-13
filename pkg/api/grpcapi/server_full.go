package grpcapi

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/watch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Server is a live gRPC Discovery server with keepalive and graceful drain.
type Server struct {
	UnimplementedDiscoveryServer
	inner   *DiscoveryServer
	gs      *grpc.Server
	draining atomic.Bool
	streams  atomic.Int64
	bus      *events.Bus
}

// UnimplementedDiscoveryServer embeds for forward compat.
type UnimplementedDiscoveryServer struct{}

// RegisterServer wires unary + streaming handlers via grpc.ServiceRegistrar
// using a custom service description (hand-rolled without protoc).
func RegisterServer(gs *grpc.Server, srv *Server) {
	gs.RegisterService(&Discovery_ServiceDesc, srv)
}

// Discovery_ServiceDesc is a hand-written gRPC service descriptor.
var Discovery_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "beacon.v1.Discovery",
	HandlerType: (*discoveryHandler)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Register", Handler: _Discovery_Register_Handler},
		{MethodName: "Deregister", Handler: _Discovery_Deregister_Handler},
		{MethodName: "Resolve", Handler: _Discovery_Resolve_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "Watch", Handler: _Discovery_Watch_Handler, ServerStreams: true},
		{StreamName: "WatchMulti", Handler: _Discovery_WatchMulti_Handler, ServerStreams: true, ClientStreams: true},
	},
	Metadata: "beacon.proto",
}

type discoveryHandler interface {
	Register(context.Context, *RegisterRequest) (*RegisterResponse, error)
	Deregister(context.Context, *DeregisterRequest) (*RegisterResponse, error)
	Resolve(context.Context, *ResolveRequest) (*ResolveResponse, error)
	Watch(*WatchRequest, grpc.ServerStreamingServer[WatchEvent]) error
	WatchMulti(grpc.BidiStreamingServer[WatchMultiRequest, WatchEvent]) error
}

// NewServer builds a Discovery gRPC server with keepalive and interceptors.
func NewServer(st store.CatalogStore, w *watch.Registry, bus *events.Bus, unary []grpc.UnaryServerInterceptor) *Server {
	s := &Server{
		inner: New(st, w, bus),
		bus:   bus,
	}
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     5 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 10 * time.Second,
			Time:                  30 * time.Second,
			Timeout:               10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	if len(unary) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(unary...))
	}
	// stream interceptor for lifecycle + drain
	opts = append(opts, grpc.StreamInterceptor(s.streamInterceptor))
	s.gs = grpc.NewServer(opts...)
	RegisterServer(s.gs, s)
	return s
}

func (s *Server) streamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if s.draining.Load() {
		return status.Error(codes.Unavailable, "server draining")
	}
	s.streams.Add(1)
	defer s.streams.Add(-1)
	if s.bus != nil {
		s.bus.Publish(events.Event{Kind: events.EvWatchOpened, Detail: info.FullMethod})
	}
	err := handler(srv, ss)
	if s.bus != nil {
		s.bus.Publish(events.Event{Kind: events.EvWatchNotified, Detail: "stream closed " + info.FullMethod})
	}
	return err
}

// Serve starts serving on lis.
func (s *Server) Serve(lis net.Listener) error {
	return s.gs.Serve(lis)
}

// GracefulStop drains streams then stops.
func (s *Server) GracefulStop() {
	s.draining.Store(true)
	// wait briefly for streams to notice
	deadline := time.Now().Add(2 * time.Second)
	for s.streams.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	s.gs.GracefulStop()
}

// Stop hard-stops.
func (s *Server) Stop() { s.gs.Stop() }

// GRPC returns the underlying server.
func (s *Server) GRPC() *grpc.Server { return s.gs }

// --- service methods ---

func (s *Server) Register(ctx context.Context, req *RegisterRequest) (*RegisterResponse, error) {
	if s.draining.Load() {
		return nil, status.Error(codes.Unavailable, "draining")
	}
	return s.inner.Register(ctx, req)
}
func (s *Server) Deregister(ctx context.Context, req *DeregisterRequest) (*RegisterResponse, error) {
	return s.inner.Deregister(ctx, req)
}
func (s *Server) Resolve(ctx context.Context, req *ResolveRequest) (*ResolveResponse, error) {
	return s.inner.Resolve(ctx, req)
}

// Watch is server-streaming.
func (s *Server) Watch(req *WatchRequest, stream grpc.ServerStreamingServer[WatchEvent]) error {
	return s.inner.WatchStream(req, func(ev *WatchEvent) error {
		return stream.Send(ev)
	}, stream.Context())
}

// WatchMulti is bidirectional.
func (s *Server) WatchMulti(stream grpc.BidiStreamingServer[WatchMultiRequest, WatchEvent]) error {
	return s.inner.WatchMultiStream(
		func() (*WatchMultiRequest, error) { return stream.Recv() },
		func(ev *WatchEvent) error { return stream.Send(ev) },
		stream.Context(),
	)
}

// Handlers for ServiceDesc (codec uses proto or json — we use generic codec via
// grpc's default which expects proto.Message; for tests we use the in-process
// DiscoveryServer methods directly. These handlers use encoding that works with
// generated-style messages if available; otherwise in-process tests use Server methods.

func _Discovery_Register_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(RegisterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(discoveryHandler).Register(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/beacon.v1.Discovery/Register"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(discoveryHandler).Register(ctx, req.(*RegisterRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_Deregister_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(DeregisterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(discoveryHandler).Deregister(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/beacon.v1.Discovery/Deregister"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(discoveryHandler).Deregister(ctx, req.(*DeregisterRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_Resolve_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ResolveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(discoveryHandler).Resolve(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/beacon.v1.Discovery/Resolve"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(discoveryHandler).Resolve(ctx, req.(*ResolveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_Watch_Handler(srv any, stream grpc.ServerStream) error {
	m := new(WatchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(discoveryHandler).Watch(m, &watchServerStream{ServerStream: stream})
}

func _Discovery_WatchMulti_Handler(srv any, stream grpc.ServerStream) error {
	return srv.(discoveryHandler).WatchMulti(&watchMultiStream{ServerStream: stream})
}

type watchServerStream struct {
	grpc.ServerStream
}

func (w *watchServerStream) Send(m *WatchEvent) error { return w.ServerStream.SendMsg(m) }
func (w *watchServerStream) Context() context.Context  { return w.ServerStream.Context() }

// Ensure interface satisfaction for generics-style aliases used above.
// Go 1.23+ has grpc.ServerStreamingServer; for older we use the wrapper.

type watchMultiStream struct {
	grpc.ServerStream
}

func (w *watchMultiStream) Send(m *WatchEvent) error   { return w.ServerStream.SendMsg(m) }
func (w *watchMultiStream) Recv() (*WatchMultiRequest, error) {
	m := new(WatchMultiRequest)
	if err := w.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
func (w *watchMultiStream) Context() context.Context { return w.ServerStream.Context() }

// InterceptorOrder is a test helper recording interceptor fire order.
func InterceptorOrder(name string, order *[]string, mu *sync.Mutex) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		mu.Lock()
		*order = append(*order, name)
		mu.Unlock()
		return handler(ctx, req)
	}
}

// Ensure catalog import used if needed later
var _ = catalog.HealthPassing
