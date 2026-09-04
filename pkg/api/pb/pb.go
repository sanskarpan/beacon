package pb

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Instance mirrors proto beacon.v1.Instance
type Instance struct {
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

func (m *Instance) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}
func (m *Instance) GetService() string {
	if m != nil {
		return m.Service
	}
	return ""
}
func (m *Instance) GetNode() string {
	if m != nil {
		return m.Node
	}
	return ""
}
func (m *Instance) GetAddress() string {
	if m != nil {
		return m.Address
	}
	return ""
}
func (m *Instance) GetPort() int32 {
	if m != nil {
		return m.Port
	}
	return 0
}
func (m *Instance) GetTags() []string {
	if m != nil {
		return m.Tags
	}
	return nil
}
func (m *Instance) GetMeta() map[string]string {
	if m != nil {
		return m.Meta
	}
	return nil
}
func (m *Instance) GetWeight() int32 {
	if m != nil {
		return m.Weight
	}
	return 0
}
func (m *Instance) GetHealth() string {
	if m != nil {
		return m.Health
	}
	return ""
}
func (m *Instance) GetRegion() string {
	if m != nil {
		return m.Region
	}
	return ""
}
func (m *Instance) GetZone() string {
	if m != nil {
		return m.Zone
	}
	return ""
}

type RegisterRequest struct {
	Instance *Instance `json:"instance"`
}

func (m *RegisterRequest) GetInstance() *Instance {
	if m != nil {
		return m.Instance
	}
	return nil
}

type RegisterResponse struct {
	Index uint64 `json:"index"`
}

func (m *RegisterResponse) GetIndex() uint64 {
	if m != nil {
		return m.Index
	}
	return 0
}

type DeregisterRequest struct {
	Id string `json:"id"`
}

func (m *DeregisterRequest) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

type ResolveRequest struct {
	Service string `json:"service"`
	Passing bool   `json:"passing"`
}

func (m *ResolveRequest) GetService() string {
	if m != nil {
		return m.Service
	}
	return ""
}
func (m *ResolveRequest) GetPassing() bool {
	if m != nil {
		return m.Passing
	}
	return false
}

type ResolveResponse struct {
	Instances []*Instance `json:"instances"`
	Index     uint64      `json:"index"`
}

func (m *ResolveResponse) GetInstances() []*Instance {
	if m != nil {
		return m.Instances
	}
	return nil
}
func (m *ResolveResponse) GetIndex() uint64 {
	if m != nil {
		return m.Index
	}
	return 0
}

type WatchRequest struct {
	Service   string `json:"service"`
	FromIndex uint64 `json:"from_index"`
	Passing   bool   `json:"passing"`
}

func (m *WatchRequest) GetService() string {
	if m != nil {
		return m.Service
	}
	return ""
}
func (m *WatchRequest) GetFromIndex() uint64 {
	if m != nil {
		return m.FromIndex
	}
	return 0
}
func (m *WatchRequest) GetPassing() bool {
	if m != nil {
		return m.Passing
	}
	return false
}

type WatchEvent struct {
	Kind      string      `json:"kind"`
	Service   string      `json:"service"`
	Instances []*Instance `json:"instances"`
	Index     uint64      `json:"index"`
}

func (m *WatchEvent) GetKind() string {
	if m != nil {
		return m.Kind
	}
	return ""
}
func (m *WatchEvent) GetService() string {
	if m != nil {
		return m.Service
	}
	return ""
}
func (m *WatchEvent) GetInstances() []*Instance {
	if m != nil {
		return m.Instances
	}
	return nil
}
func (m *WatchEvent) GetIndex() uint64 {
	if m != nil {
		return m.Index
	}
	return 0
}

type WatchMultiRequest struct {
	Op        string `json:"op"`
	Service   string `json:"service"`
	FromIndex uint64 `json:"from_index"`
}

func (m *WatchMultiRequest) GetOp() string {
	if m != nil {
		return m.Op
	}
	return ""
}
func (m *WatchMultiRequest) GetService() string {
	if m != nil {
		return m.Service
	}
	return ""
}
func (m *WatchMultiRequest) GetFromIndex() uint64 {
	if m != nil {
		return m.FromIndex
	}
	return 0
}

// Service interfaces

type DiscoveryServer interface {
	Register(context.Context, *RegisterRequest) (*RegisterResponse, error)
	Deregister(context.Context, *DeregisterRequest) (*RegisterResponse, error)
	Resolve(context.Context, *ResolveRequest) (*ResolveResponse, error)
	Watch(*WatchRequest, Discovery_WatchServer) error
	WatchMulti(Discovery_WatchMultiServer) error
}

type UnimplementedDiscoveryServer struct{}

func (UnimplementedDiscoveryServer) Register(context.Context, *RegisterRequest) (*RegisterResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Register not implemented")
}
func (UnimplementedDiscoveryServer) Deregister(context.Context, *DeregisterRequest) (*RegisterResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Deregister not implemented")
}
func (UnimplementedDiscoveryServer) Resolve(context.Context, *ResolveRequest) (*ResolveResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Resolve not implemented")
}
func (UnimplementedDiscoveryServer) Watch(*WatchRequest, Discovery_WatchServer) error {
	return status.Error(codes.Unimplemented, "Watch not implemented")
}
func (UnimplementedDiscoveryServer) WatchMulti(Discovery_WatchMultiServer) error {
	return status.Error(codes.Unimplemented, "WatchMulti not implemented")
}

// Server stream interfaces
type Discovery_WatchServer interface {
	Send(*WatchEvent) error
	grpc.ServerStream
}

type Discovery_WatchMultiServer interface {
	Send(*WatchEvent) error
	Recv() (*WatchMultiRequest, error)
	grpc.ServerStream
}

// Client interfaces
type DiscoveryClient interface {
	Register(ctx context.Context, in *RegisterRequest, opts ...grpc.CallOption) (*RegisterResponse, error)
	Deregister(ctx context.Context, in *DeregisterRequest, opts ...grpc.CallOption) (*RegisterResponse, error)
	Resolve(ctx context.Context, in *ResolveRequest, opts ...grpc.CallOption) (*ResolveResponse, error)
	Watch(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (Discovery_WatchClient, error)
	WatchMulti(ctx context.Context, opts ...grpc.CallOption) (Discovery_WatchMultiClient, error)
}

type Discovery_WatchClient interface {
	Recv() (*WatchEvent, error)
	grpc.ClientStream
}

type Discovery_WatchMultiClient interface {
	Send(*WatchMultiRequest) error
	Recv() (*WatchEvent, error)
	grpc.ClientStream
}

// Registration
func RegisterDiscoveryServer(s grpc.ServiceRegistrar, srv DiscoveryServer) {
	s.RegisterService(&_Discovery_serviceDesc, srv)
}

var _Discovery_serviceDesc = grpc.ServiceDesc{
	ServiceName: "beacon.v1.Discovery",
	HandlerType: (*DiscoveryServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Register", Handler: _Discovery_Register_Handler},
		{MethodName: "Deregister", Handler: _Discovery_Deregister_Handler},
		{MethodName: "Resolve", Handler: _Discovery_Resolve_Handler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "Watch", Handler: _Discovery_Watch_Handler, ServerStreams: true},
		{StreamName: "WatchMulti", Handler: _Discovery_WatchMulti_Handler, ServerStreams: true, ClientStreams: true},
	},
}

func _Discovery_Register_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).Register(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/beacon.v1.Discovery/Register"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).Register(ctx, req.(*RegisterRequest))
	}
	return interceptor(ctx, in, info, handler)
}
func _Discovery_Deregister_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeregisterRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).Deregister(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/beacon.v1.Discovery/Deregister"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).Deregister(ctx, req.(*DeregisterRequest))
	}
	return interceptor(ctx, in, info, handler)
}
func _Discovery_Resolve_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ResolveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).Resolve(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/beacon.v1.Discovery/Resolve"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).Resolve(ctx, req.(*ResolveRequest))
	}
	return interceptor(ctx, in, info, handler)
}
func _Discovery_Watch_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WatchRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DiscoveryServer).Watch(m, &discoveryWatchServer{ServerStream: stream})
}
func _Discovery_WatchMulti_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(DiscoveryServer).WatchMulti(&discoveryWatchMultiServer{ServerStream: stream})
}

type discoveryWatchServer struct{ grpc.ServerStream }

func (x *discoveryWatchServer) Send(m *WatchEvent) error { return x.SendMsg(m) }

type discoveryWatchMultiServer struct{ grpc.ServerStream }

func (x *discoveryWatchMultiServer) Send(m *WatchEvent) error { return x.SendMsg(m) }
func (x *discoveryWatchMultiServer) Recv() (*WatchMultiRequest, error) {
	m := new(WatchMultiRequest)
	if err := x.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Client implementation
type discoveryClient struct{ cc grpc.ClientConnInterface }

func NewDiscoveryClient(cc grpc.ClientConnInterface) DiscoveryClient { return &discoveryClient{cc} }

func (c *discoveryClient) Register(ctx context.Context, in *RegisterRequest, opts ...grpc.CallOption) (*RegisterResponse, error) {
	out := new(RegisterResponse)
	err := c.cc.Invoke(ctx, "/beacon.v1.Discovery/Register", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
func (c *discoveryClient) Deregister(ctx context.Context, in *DeregisterRequest, opts ...grpc.CallOption) (*RegisterResponse, error) {
	out := new(RegisterResponse)
	err := c.cc.Invoke(ctx, "/beacon.v1.Discovery/Deregister", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
func (c *discoveryClient) Resolve(ctx context.Context, in *ResolveRequest, opts ...grpc.CallOption) (*ResolveResponse, error) {
	out := new(ResolveResponse)
	err := c.cc.Invoke(ctx, "/beacon.v1.Discovery/Resolve", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
func (c *discoveryClient) Watch(ctx context.Context, in *WatchRequest, opts ...grpc.CallOption) (Discovery_WatchClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Discovery_serviceDesc.Streams[0], "/beacon.v1.Discovery/Watch", opts...)
	if err != nil {
		return nil, err
	}
	x := &discoveryWatchClient{ClientStream: stream}
	if err := x.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}
func (c *discoveryClient) WatchMulti(ctx context.Context, opts ...grpc.CallOption) (Discovery_WatchMultiClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Discovery_serviceDesc.Streams[1], "/beacon.v1.Discovery/WatchMulti", opts...)
	if err != nil {
		return nil, err
	}
	return &discoveryWatchMultiClient{ClientStream: stream}, nil
}

type discoveryWatchClient struct{ grpc.ClientStream }

func (x *discoveryWatchClient) Recv() (*WatchEvent, error) {
	m := new(WatchEvent)
	if err := x.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

type discoveryWatchMultiClient struct{ grpc.ClientStream }

func (x *discoveryWatchMultiClient) Send(m *WatchMultiRequest) error {
	return x.SendMsg(m)
}
func (x *discoveryWatchMultiClient) Recv() (*WatchEvent, error) {
	m := new(WatchEvent)
	if err := x.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
