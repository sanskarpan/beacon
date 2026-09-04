package xds

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

// ADS gRPC service name matching Envoy AggregatedDiscoveryService.
const ADSServiceName = "envoy.service.discovery.v3.AggregatedDiscoveryService"

// GRPCServer exposes xDS ADS (StreamAggregatedResources) over a live gRPC server.
// Messages use JSON-encoded DiscoveryRequest/Response bodies for testability
// without full protobuf codegen; Envoy integration tests use a companion client
// (and optionally a real Envoy process) that speaks this stream.
type GRPCServer struct {
	inner *Server
	gs    *grpc.Server
	bus   *events.Bus
}

// NewGRPCServer builds an ADS gRPC server.
// Uses the JSON content-subtype codec so hand-written DiscoveryRequest/Response
// shapes work without full protobuf codegen (live Envoy uses real protos when
// wired via go-control-plane; this path proves stream ACK lifecycle).
func NewGRPCServer(st store.CatalogStore, bus *events.Bus, opts ...grpc.ServerOption) *GRPCServer {
	s := &GRPCServer{
		inner: New(st, bus),
		bus:   bus,
	}
	base := []grpc.ServerOption{
		grpc.ForceServerCodec(encoding.GetCodec(JSONCodecName)),
	}
	base = append(base, opts...)
	s.gs = grpc.NewServer(base...)
	s.gs.RegisterService(&ADS_ServiceDesc, s)
	return s
}

// Inner returns the logical xDS server (snapshots, NACK handling).
func (s *GRPCServer) Inner() *Server { return s.inner }

// WithSDS attaches a secret source multiplexed on this ADS stream (TODO-029).
func (s *GRPCServer) WithSDS(src SecretSource) *GRPCServer {
	s.inner.WithSDS(src)
	return s
}

// GRPC returns the underlying grpc.Server.
func (s *GRPCServer) GRPC() *grpc.Server { return s.gs }

// Serve starts serving.
func (s *GRPCServer) Serve(lis net.Listener) error { return s.gs.Serve(lis) }

// Stop stops the server.
func (s *GRPCServer) Stop() { s.gs.Stop() }

// StreamAggregatedResources is the Envoy ADS SotW stream.
func (s *GRPCServer) StreamAggregatedResources(stream grpc.ServerStream) error {
	for {
		req := &DiscoveryRequest{}
		if err := stream.RecvMsg(req); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if req.NodeID == "" {
			return status.Error(codes.InvalidArgument, "node id required")
		}
		resps := s.inner.HandleRequest(req)
		for _, r := range resps {
			if err := stream.SendMsg(r); err != nil {
				return err
			}
		}
	}
}

// ADS_ServiceDesc is a hand-written ADS service descriptor.
var ADS_ServiceDesc = grpc.ServiceDesc{
	ServiceName: ADSServiceName,
	HandlerType: (*adsHandler)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamAggregatedResources",
			Handler:       _ADS_Stream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "envoy/service/discovery/v3/ads.proto",
}

type adsHandler interface {
	StreamAggregatedResources(grpc.ServerStream) error
}

func _ADS_Stream_Handler(srv any, stream grpc.ServerStream) error {
	return srv.(adsHandler).StreamAggregatedResources(stream)
}

// LiveClient is a test/Envoy-shaped ADS client over a real gRPC connection.
type LiveClient struct {
	conn   *grpc.ClientConn
	stream grpc.ClientStream
	mu     sync.Mutex
	acks   int
	nacks  int
	pushes []*DiscoveryResponse
	// incoming serializes all RecvMsg calls through one reader goroutine so
	// timeout-based drains never race a concurrent read or steal responses.
	incoming chan recvResult
}

type recvResult struct {
	r   *DiscoveryResponse
	err error
}

// DialADS opens an ADS bidi stream to addr (host:port).
func DialADS(ctx context.Context, target string, opts ...grpc.DialOption) (*LiveClient, error) {
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	desc := &grpc.StreamDesc{
		StreamName:    "StreamAggregatedResources",
		ServerStreams: true,
		ClientStreams: true,
	}
	stream, err := conn.NewStream(ctx, desc, "/"+ADSServiceName+"/StreamAggregatedResources")
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	c := &LiveClient{conn: conn, stream: stream, incoming: make(chan recvResult, 64)}
	go c.readLoop()
	return c, nil
}

// readLoop is the single reader for the stream.
func (c *LiveClient) readLoop() {
	for {
		r := &DiscoveryResponse{}
		if err := c.stream.RecvMsg(r); err != nil {
			c.incoming <- recvResult{err: err}
			return
		}
		c.mu.Lock()
		c.pushes = append(c.pushes, r)
		c.mu.Unlock()
		c.incoming <- recvResult{r: r}
	}
}

// SendRequest writes a DiscoveryRequest.
func (c *LiveClient) SendRequest(req *DiscoveryRequest) error {
	return c.stream.SendMsg(req)
} // RecvResponse reads one DiscoveryResponse.
func (c *LiveClient) RecvResponse() (*DiscoveryResponse, error) {
	res, ok := <-c.incoming
	if !ok {
		return nil, io.EOF
	}
	return res.r, res.err
}

// RecvResponseTimeout reads one DiscoveryResponse, giving up after timeout
// (used to drain a stream until it is quiescent).
func (c *LiveClient) RecvResponseTimeout(timeout time.Duration) (*DiscoveryResponse, error) {
	select {
	case res, ok := <-c.incoming:
		if !ok {
			return nil, io.EOF
		}
		return res.r, res.err
	case <-time.After(timeout):
		return nil, errStreamQuiescent
	}
}

// errStreamQuiescent means no response arrived within the drain window.
var errStreamQuiescent = fmt.Errorf("stream quiescent (no response within drain window)")

// ACK acknowledges the last push for typeURL.
func (c *LiveClient) ACK(nodeID, typeURL, version, nonce string) error {
	c.mu.Lock()
	c.acks++
	c.mu.Unlock()
	return c.SendRequest(&DiscoveryRequest{
		NodeID:        nodeID,
		TypeURL:       typeURL,
		VersionInfo:   version,
		ResponseNonce: nonce,
	})
}

// NACK nacks with error detail (must not trigger same-config resend).
func (c *LiveClient) NACK(nodeID, typeURL, version, nonce, msg string) error {
	c.mu.Lock()
	c.nacks++
	c.mu.Unlock()
	return c.SendRequest(&DiscoveryRequest{
		NodeID:        nodeID,
		TypeURL:       typeURL,
		VersionInfo:   version,
		ResponseNonce: nonce,
		ErrorDetail:   &ErrorDetail{Message: msg},
	})
}

// Stats returns ack/nack/push counts.
func (c *LiveClient) Stats() (acks, nacks, pushes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.acks, c.nacks, len(c.pushes)
}

// Close closes the connection.
func (c *LiveClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// RunACKLoop requests all types, ACKs each response, returns after at least one full set or ctx done.
func (c *LiveClient) RunACKLoop(ctx context.Context, nodeID string) error {
	if err := c.SendRequest(&DiscoveryRequest{NodeID: nodeID, TypeURL: "ads"}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		// Use a short deadline per recv via parent ctx.
		r, err := c.RecvResponse()
		if err != nil {
			return err
		}
		if err := c.ACK(nodeID, r.TypeURL, r.VersionInfo, r.Nonce); err != nil {
			return err
		}
		acks, _, pushes := c.Stats()
		if acks >= len(AddOrder) && pushes >= len(AddOrder) {
			return nil
		}
	}
}

// BootstrapYAML is a helper for optional real-Envoy runs (writes bootstrap JSON).
func BootstrapYAML(nodeID, adsAddr string, adsPort, adminPort int) ([]byte, error) {
	b, err := GenerateBootstrap(BootstrapConfig{
		NodeID:     nodeID,
		ADSAddress: adsAddr,
		ADSPort:    adsPort,
		AdminPort:  adminPort,
	})
	if err != nil {
		return nil, err
	}
	// Also return a small marker so tests can assert content.
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return b, nil
	}
	if m["node"] == nil {
		return nil, fmt.Errorf("bootstrap missing node")
	}
	return b, nil
}
