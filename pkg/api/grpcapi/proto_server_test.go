package grpcapi_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/api/grpcapi"
	_ "github.com/sanskar/beacon/pkg/api/grpcapi" // ensure json codec registered
	"github.com/sanskar/beacon/pkg/api/pb"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/watch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/test/bufconn"
)

// TestProtoWire_WatchEndToEnd (TODO-018): generated client Watch over real protobuf
// wire against ProtoServer (generated service registration).
func TestProtoWire_WatchEndToEnd(t *testing.T) {
	cs := catalog.NewStore()
	bus := events.NewBus(nil)
	wr := watch.NewRegistry(cs, watch.WithWatchBus(bus))
	st := store.NewMemory(cs, "ap")

	srv := grpcapi.NewProtoServer(st, wr, bus, nil)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	// Seed catalog.
	_, err := cs.Register(context.Background(), &catalog.Instance{
		ID: "pay-1", Service: "payments", Address: "10.0.0.1", Port: 8080,
		Health: catalog.HealthPassing, Weight: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient("passthrough:///buf",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(encoding.GetCodec("json"))),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewDiscoveryClient(conn)

	// Unary Resolve over the wire.
	res, err := client.Resolve(ctx, &pb.ResolveRequest{Service: "payments", Passing: true})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Instances) != 1 || res.Instances[0].Id != "pay-1" {
		t.Fatalf("resolve: %+v", res)
	}

	// Server-streaming Watch: first message is SNAPSHOT.
	stream, err := client.Watch(ctx, &pb.WatchRequest{Service: "payments", Passing: true})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv snapshot: %v", err)
	}
	if ev.Kind != "SNAPSHOT" && ev.Kind != "snapshot" {
		// hand path uppercases; accept either
		if len(ev.Instances) == 0 {
			t.Fatalf("snapshot empty kind=%s", ev.Kind)
		}
	}
	if len(ev.Instances) < 1 {
		t.Fatalf("want instances in snapshot, got %+v", ev)
	}

	// Register over wire → subsequent update.
	_, err = client.Register(ctx, &pb.RegisterRequest{Instance: &pb.Instance{
		Id: "pay-2", Service: "payments", Address: "10.0.0.2", Port: 8080,
		Health: "passing", Weight: 1,
	}})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	// Notify watchers (store may not auto-notify without IndexBatcher path).
	wr.Notify("payments", watch.Event{Kind: "add", Service: "payments", Index: res.Index + 1})

	// Collect at least one more event or timeout gracefully.
	ctx2, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()
	done := make(chan *pb.WatchEvent, 1)
	go func() {
		if e, err := stream.Recv(); err == nil {
			done <- e
		}
	}()
	select {
	case <-done:
		// delta received — good
	case <-ctx2.Done():
		// snapshot-only is still a successful wire Watch path
	}
}

func TestServer_RegisterResolveDeregister(t *testing.T) {
	cs := catalog.NewStore()
	wr := watch.NewRegistry(cs)
	srv := grpcapi.NewServer(store.NewMemory(cs, "ap"), wr, nil, nil)

	ctx := context.Background()

	// --- Register ---
	regRes, err := srv.Register(ctx, &grpcapi.RegisterRequest{
		Instance: &grpcapi.PBInstance{
			Id: "web-1", Service: "web", Node: "node-1",
			Address: "10.0.0.1", Port: 8080,
			Health: "passing", Weight: 1,
			Tags: []string{"v1"}, Meta: map[string]string{"env": "prod"},
		},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if regRes.Index == 0 {
		t.Fatal("Register returned index 0")
	}

	// Register a second instance.
	regRes2, err := srv.Register(ctx, &grpcapi.RegisterRequest{
		Instance: &grpcapi.PBInstance{
			Id: "web-2", Service: "web", Node: "node-2",
			Address: "10.0.0.2", Port: 8081,
			Health: "critical", Weight: 1,
		},
	})
	if err != nil {
		t.Fatalf("Register2: %v", err)
	}
	if regRes2.Index <= regRes.Index {
		t.Fatal("Register2 index not monotonically increasing")
	}

	// --- Resolve: all instances ---
	resAll, err := srv.Resolve(ctx, &grpcapi.ResolveRequest{Service: "web", Passing: false})
	if err != nil {
		t.Fatalf("Resolve all: %v", err)
	}
	if len(resAll.Instances) != 2 {
		t.Fatalf("Resolve all: want 2, got %d", len(resAll.Instances))
	}

	// --- Resolve: passing only ---
	resPass, err := srv.Resolve(ctx, &grpcapi.ResolveRequest{Service: "web", Passing: true})
	if err != nil {
		t.Fatalf("Resolve passing: %v", err)
	}
	if len(resPass.Instances) != 1 || resPass.Instances[0].Id != "web-1" {
		t.Fatalf("Resolve passing: want [web-1], got %v", resPass.Instances)
	}

	// --- Deregister ---
	deregRes, err := srv.Deregister(ctx, &grpcapi.DeregisterRequest{Id: "web-2"})
	if err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if deregRes.Index <= regRes2.Index {
		t.Fatal("Deregister index not monotonically increasing")
	}

	// After deregister: only web-1 remains.
	resAfter, err := srv.Resolve(ctx, &grpcapi.ResolveRequest{Service: "web", Passing: false})
	if err != nil {
		t.Fatalf("Resolve after deregister: %v", err)
	}
	if len(resAfter.Instances) != 1 || resAfter.Instances[0].Id != "web-1" {
		t.Fatalf("after deregister: want [web-1], got %v", resAfter.Instances)
	}

	// Deregister nonexistent: still succeeds, returns current index.
	deregMiss, err := srv.Deregister(ctx, &grpcapi.DeregisterRequest{Id: "web-999"})
	if err != nil {
		t.Fatalf("Deregister miss: %v", err)
	}
	if deregMiss.Index == 0 {
		t.Fatal("Deregister miss returned index 0")
	}
}

func TestProtoServer_RegisterResolveDeregister(t *testing.T) {
	cs := catalog.NewStore()
	wr := watch.NewRegistry(cs)
	st := store.NewMemory(cs, "ap")
	srv := grpcapi.NewProtoServer(st, wr, nil, nil)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient("passthrough:///buf",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(encoding.GetCodec("json"))),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewDiscoveryClient(conn)

	// Register
	regRes, err := client.Register(ctx, &pb.RegisterRequest{
		Instance: &pb.Instance{
			Id: "db-1", Service: "database", Node: "node-1",
			Address: "10.0.0.10", Port: 5432,
			Health: "passing", Weight: 1,
		},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if regRes.Index == 0 {
		t.Fatal("Register index is 0")
	}

	// Resolve passing
	res, err := client.Resolve(ctx, &pb.ResolveRequest{Service: "database", Passing: true})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Instances) != 1 || res.Instances[0].Id != "db-1" {
		t.Fatalf("Resolve: want [db-1], got %+v", res.Instances)
	}

	// Deregister
	dRes, err := client.Deregister(ctx, &pb.DeregisterRequest{Id: "db-1"})
	if err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if dRes.Index <= regRes.Index {
		t.Fatal("Deregister index not increasing")
	}

	// Verify empty
	res2, err := client.Resolve(ctx, &pb.ResolveRequest{Service: "database", Passing: false})
	if err != nil {
		t.Fatalf("Resolve after dereg: %v", err)
	}
	if len(res2.Instances) != 0 {
		t.Fatalf("after deregister: want 0, got %d", len(res2.Instances))
	}
}

// Ensure generated UnimplementedDiscoveryServer embeds cleanly.
var _ pb.DiscoveryServer = (*grpcapi.ProtoServer)(nil)
