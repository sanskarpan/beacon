package grpcapi

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/sanskar/beacon/pkg/api/pb"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/watch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// ProtoServer implements the generated pb.DiscoveryServer over real protobuf wire.
// This is the codegen path (TODO-018); the hand-rolled Server remains for legacy tests.
type ProtoServer struct {
	pb.UnimplementedDiscoveryServer
	inner    *DiscoveryServer
	gs       *grpc.Server
	draining atomic.Bool
	streams  atomic.Int64
	bus      *events.Bus
}

// NewProtoServer builds a Discovery gRPC server registered with generated stubs.
func NewProtoServer(st store.CatalogStore, w *watch.Registry, bus *events.Bus, unary []grpc.UnaryServerInterceptor) *ProtoServer {
	s := &ProtoServer{
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
	opts = append(opts, grpc.StreamInterceptor(s.streamInterceptor))
	s.gs = grpc.NewServer(opts...)
	pb.RegisterDiscoveryServer(s.gs, s)
	return s
}

func (s *ProtoServer) streamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
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
func (s *ProtoServer) Serve(lis net.Listener) error { return s.gs.Serve(lis) }

// GracefulStop drains then stops.
func (s *ProtoServer) GracefulStop() {
	s.draining.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for s.streams.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	s.gs.GracefulStop()
}

// Stop hard-stops.
func (s *ProtoServer) Stop() { s.gs.Stop() }

// GRPC returns the underlying server.
func (s *ProtoServer) GRPC() *grpc.Server { return s.gs }

// Register implements pb.DiscoveryServer.
func (s *ProtoServer) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if s.draining.Load() {
		return nil, status.Error(codes.Unavailable, "draining")
	}
	if req.GetInstance() == nil {
		return nil, status.Error(codes.InvalidArgument, "instance required")
	}
	idx, err := s.inner.store.Register(ctx, fromWire(req.Instance))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.RegisterResponse{Index: idx}, nil
}

// Deregister implements pb.DiscoveryServer.
func (s *ProtoServer) Deregister(ctx context.Context, req *pb.DeregisterRequest) (*pb.RegisterResponse, error) {
	idx, err := s.inner.store.Deregister(ctx, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &pb.RegisterResponse{Index: idx}, nil
}

// Resolve implements pb.DiscoveryServer.
func (s *ProtoServer) Resolve(ctx context.Context, req *pb.ResolveRequest) (*pb.ResolveResponse, error) {
	_ = ctx
	res := s.inner.store.GetNow(req.GetService(), catalog.QueryOptions{Passing: req.GetPassing()})
	out := make([]*pb.Instance, 0, len(res.Instances))
	for _, in := range res.Instances {
		out = append(out, toWire(in))
	}
	return &pb.ResolveResponse{Instances: out, Index: res.Index}, nil
}

// Watch implements pb.DiscoveryServer (server streaming).
func (s *ProtoServer) Watch(req *pb.WatchRequest, stream pb.Discovery_WatchServer) error {
	return s.inner.WatchStream(&WatchRequest{
		Service:   req.GetService(),
		FromIndex: req.GetFromIndex(),
		Passing:   req.GetPassing(),
	}, func(ev *WatchEvent) error {
		return stream.Send(wireEvent(ev))
	}, stream.Context())
}

// WatchMulti implements pb.DiscoveryServer (bidi streaming).
func (s *ProtoServer) WatchMulti(stream pb.Discovery_WatchMultiServer) error {
	return s.inner.WatchMultiStream(
		func() (*WatchMultiRequest, error) {
			m, err := stream.Recv()
			if err != nil {
				return nil, err
			}
			return &WatchMultiRequest{
				Op:        m.GetOp(),
				Service:   m.GetService(),
				FromIndex: m.GetFromIndex(),
			}, nil
		},
		func(ev *WatchEvent) error {
			return stream.Send(wireEvent(ev))
		},
		stream.Context(),
	)
}

func toWire(in *catalog.Instance) *pb.Instance {
	if in == nil {
		return nil
	}
	return &pb.Instance{
		Id: in.ID, Service: in.Service, Node: in.Node,
		Address: in.Address, Port: int32(in.Port),
		Tags: in.Tags, Meta: in.Meta, Weight: int32(in.Weight),
		Health: string(in.Health), Region: in.Locality.Region, Zone: in.Locality.Zone,
	}
}

func fromWire(p *pb.Instance) *catalog.Instance {
	return &catalog.Instance{
		ID: p.GetId(), Service: p.GetService(), Node: p.GetNode(),
		Address: p.GetAddress(), Port: int(p.GetPort()),
		Tags: p.GetTags(), Meta: p.GetMeta(), Weight: int(p.GetWeight()),
		Health:   catalog.HealthStatus(p.GetHealth()),
		Locality: catalog.Locality{Region: p.GetRegion(), Zone: p.GetZone()},
	}
}

func wireEvent(ev *WatchEvent) *pb.WatchEvent {
	out := &pb.WatchEvent{
		Kind:    ev.Kind,
		Service: ev.Service,
		Index:   ev.Index,
	}
	for _, in := range ev.Instances {
		// hand-rolled PBInstance → wire via catalog conversion
		out.Instances = append(out.Instances, &pb.Instance{
			Id: in.Id, Service: in.Service, Node: in.Node,
			Address: in.Address, Port: in.Port,
			Tags: in.Tags, Meta: in.Meta, Weight: in.Weight,
			Health: in.Health, Region: in.Region, Zone: in.Zone,
		})
	}
	return out
}

// DialDiscovery connects a generated DiscoveryClient to addr (host:port or passthrough).
func DialDiscovery(ctx context.Context, target string, opts ...grpc.DialOption) (pb.DiscoveryClient, *grpc.ClientConn, error) {
	base := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	base = append(base, opts...)
	conn, err := grpc.NewClient(target, base...)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewDiscoveryClient(conn), conn, nil
}

// WatchOnce is a small helper used by tests: open Watch, collect first snapshot, cancel.
func WatchOnce(ctx context.Context, c pb.DiscoveryClient, service string) (*pb.WatchEvent, error) {
	stream, err := c.Watch(ctx, &pb.WatchRequest{Service: service, Passing: true})
	if err != nil {
		return nil, err
	}
	ev, err := stream.Recv()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return ev, nil
}
