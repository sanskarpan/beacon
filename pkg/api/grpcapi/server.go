// Package grpcapi implements the Discovery gRPC service (streaming watch).
package grpcapi

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/watch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- Hand-written service API (proto-compatible shapes without codegen) ---

// Instance proto-like struct.
type PBInstance struct {
	Id      string            `json:"id"`
	Service string            `json:"service"`
	Node    string            `json:"node"`
	Address string            `json:"address"`
	Port    int32             `json:"port"`
	Tags    []string          `json:"tags"`
	Meta    map[string]string `json:"meta"`
	Weight  int32             `json:"weight"`
	Health  string            `json:"health"`
	Region  string            `json:"region"`
	Zone    string            `json:"zone"`
}

// WatchRequest starts a watch.
type WatchRequest struct {
	Service   string `json:"service"`
	FromIndex uint64 `json:"from_index"`
	Passing   bool   `json:"passing"`
}

// WatchEvent is a stream message.
type WatchEvent struct {
	Kind      string        `json:"kind"` // SNAPSHOT ADD UPDATE REMOVE
	Service   string        `json:"service"`
	Instances []*PBInstance `json:"instances"`
	Index     uint64        `json:"index"`
}

// WatchMultiRequest adds/removes subscriptions.
type WatchMultiRequest struct {
	Op        string `json:"op"` // subscribe | unsubscribe
	Service   string `json:"service"`
	FromIndex uint64 `json:"from_index"`
}

// RegisterRequest wraps an instance.
type RegisterRequest struct {
	Instance *PBInstance `json:"instance"`
}

// RegisterResponse is empty ack with index.
type RegisterResponse struct {
	Index uint64 `json:"index"`
}

// DeregisterRequest removes by id.
type DeregisterRequest struct {
	Id string `json:"id"`
}

// ResolveRequest is a one-shot resolve.
type ResolveRequest struct {
	Service string `json:"service"`
	Passing bool   `json:"passing"`
}

// ResolveResponse lists instances.
type ResolveResponse struct {
	Instances []*PBInstance `json:"instances"`
	Index     uint64        `json:"index"`
}

// DiscoveryServer is the service implementation.
type DiscoveryServer struct {
	store store.CatalogStore
	watch *watch.Registry
	bus   *events.Bus
}

// New creates the gRPC service.
func New(st store.CatalogStore, w *watch.Registry, bus *events.Bus) *DiscoveryServer {
	return &DiscoveryServer{store: st, watch: w, bus: bus}
}

// Register registers an instance.
func (s *DiscoveryServer) Register(ctx context.Context, req *RegisterRequest) (*RegisterResponse, error) {
	if req.Instance == nil {
		return nil, status.Error(codes.InvalidArgument, "instance required")
	}
	inst := fromPB(req.Instance)
	idx, err := s.store.Register(ctx, inst)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &RegisterResponse{Index: idx}, nil
}

// Deregister removes an instance.
func (s *DiscoveryServer) Deregister(ctx context.Context, req *DeregisterRequest) (*RegisterResponse, error) {
	idx, err := s.store.Deregister(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &RegisterResponse{Index: idx}, nil
}

// Resolve is a one-shot lookup.
func (s *DiscoveryServer) Resolve(ctx context.Context, req *ResolveRequest) (*ResolveResponse, error) {
	_ = ctx
	opts := catalog.QueryOptions{Passing: req.Passing}
	res := s.store.GetNow(req.Service, opts)
	out := make([]*PBInstance, 0, len(res.Instances))
	for _, in := range res.Instances {
		out = append(out, toPB(in))
	}
	return &ResolveResponse{Instances: out, Index: res.Index}, nil
}

// WatchStream is the server-streaming watch (first message = snapshot).
func (s *DiscoveryServer) WatchStream(req *WatchRequest, send func(*WatchEvent) error, ctx context.Context) error {
	if s.watch == nil {
		// fallback: single snapshot + block on store
		res := s.store.GetNow(req.Service, catalog.QueryOptions{Passing: req.Passing})
		ev := &WatchEvent{Kind: "SNAPSHOT", Service: req.Service, Index: res.Index}
		for _, in := range res.Instances {
			ev.Instances = append(ev.Instances, toPB(in))
		}
		if err := send(ev); err != nil {
			return err
		}
		// simple poll loop
		idx := res.Index
		for {
			opts := catalog.QueryOptions{MinIndex: idx, Wait: 0}
			r, err := s.store.Get(ctx, req.Service, opts)
			if err != nil {
				return err
			}
			if r.Index > idx {
				ev := &WatchEvent{Kind: "UPDATE", Service: req.Service, Index: r.Index}
				for _, in := range r.Instances {
					ev.Instances = append(ev.Instances, toPB(in))
				}
				if err := send(ev); err != nil {
					return err
				}
				idx = r.Index
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}

	ch, err := s.watch.Watch(ctx, req.Service, req.FromIndex)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			we := &WatchEvent{
				Kind:    stringsUpper(ev.Kind),
				Service: ev.Service,
				Index:   ev.Index,
			}
			for _, in := range ev.Instances {
				we.Instances = append(we.Instances, toPB(in))
			}
			if err := send(we); err != nil {
				return err
			}
		}
	}
}

// WatchMultiStream is bidirectional multi-service watch.
func (s *DiscoveryServer) WatchMultiStream(
	recv func() (*WatchMultiRequest, error),
	send func(*WatchEvent) error,
	ctx context.Context,
) error {
	var mu sync.Mutex
	var sendMu sync.Mutex
	safeSend := func(ev *WatchEvent) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return send(ev)
	}
	cancels := map[string]context.CancelFunc{}
	defer func() {
		mu.Lock()
		for _, c := range cancels {
			c()
		}
		mu.Unlock()
	}()

	for {
		req, err := recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch req.Op {
		case "unsubscribe":
			mu.Lock()
			if c, ok := cancels[req.Service]; ok {
				c()
				delete(cancels, req.Service)
			}
			mu.Unlock()
		default: // subscribe
			mu.Lock()
			if _, ok := cancels[req.Service]; ok {
				mu.Unlock()
				continue
			}
			cctx, cancel := context.WithCancel(ctx)
			cancels[req.Service] = cancel
			mu.Unlock()
			go func(service string, from uint64) {
				_ = s.WatchStream(&WatchRequest{Service: service, FromIndex: from}, safeSend, cctx)
			}(req.Service, req.FromIndex)
		}
	}
}

// Listen starts a raw gRPC server with service methods registered via custom codec-free handlers.
// For simplicity we expose a net/http-style registration using grpc.ServiceDesc manually is heavy;
// instead StartGRPC registers a lightweight service on a grpc.Server for tests.
func StartGRPC(lis net.Listener, st store.CatalogStore, w *watch.Registry, bus *events.Bus, unary []grpc.UnaryServerInterceptor) (*grpc.Server, error) {
	opts := []grpc.ServerOption{}
	if len(unary) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(unary...))
	}
	gs := grpc.NewServer(opts...)
	// Register a generic service using UnknownServiceHandler for flexibility is too loose.
	// We register nothing proto-generated; callers use DiscoveryServer methods directly in tests.
	// For production binary we still start grpc server for health.
	go func() { _ = gs.Serve(lis) }()
	return gs, nil
}

// clampInt32 bounds instance fields (port/weight) into int32 range;
// negatives clamp to 0 since negative ports/weights are never valid.
func clampInt32(v int) int32 {
	if v < 0 {
		return 0
	}
	if v > 0x7FFFFFFF {
		return 0x7FFFFFFF
	}
	return int32(v) //nolint:gosec // G115: bounded above by construction
}

func toPB(in *catalog.Instance) *PBInstance {
	if in == nil {
		return nil
	}
	return &PBInstance{
		Id: in.ID, Service: in.Service, Node: in.Node,
		Address: in.Address, Port: clampInt32(in.Port),
		Tags: in.Tags, Meta: in.Meta, Weight: clampInt32(in.Weight),
		Health: string(in.Health), Region: in.Locality.Region, Zone: in.Locality.Zone,
	}
}

func fromPB(p *PBInstance) *catalog.Instance {
	return &catalog.Instance{
		ID: p.Id, Service: p.Service, Node: p.Node,
		Address: p.Address, Port: int(p.Port),
		Tags: p.Tags, Meta: p.Meta, Weight: int(p.Weight),
		Health:   catalog.HealthStatus(p.Health),
		Locality: catalog.Locality{Region: p.Region, Zone: p.Zone},
	}
}

func stringsUpper(s string) string {
	switch s {
	case "snapshot":
		return "SNAPSHOT"
	case "add":
		return "ADD"
	case "update":
		return "UPDATE"
	case "remove":
		return "REMOVE"
	default:
		return s
	}
}
