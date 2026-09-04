package sdk_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/sdk"
	"github.com/sanskar/beacon/pkg/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const balancerSvc = "test.Balancer"

// testJSONCodec lets the test send arbitrary structs over real gRPC without
// protobuf codegen (same trick as pkg/xds's json codec).
type testJSONCodec struct{}

func (testJSONCodec) Name() string                       { return "testjson" }
func (testJSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (testJSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func init() { encoding.RegisterCodec(testJSONCodec{}) }

// jsonCodec must be resolved lazily: package-level vars initialize before init().
func jsonCodec() encoding.Codec { return encoding.GetCodec("testjson") }

type balancerBackend struct {
	id    string
	hits  atomic.Int64
	sleep time.Duration
	gs    *grpc.Server
	lis   net.Listener
}

func (b *balancerBackend) serve() error {
	b.gs = grpc.NewServer()
	b.gs.RegisterService(&grpc.ServiceDesc{
		ServiceName: balancerSvc,
		HandlerType: (*interface{})(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Ping",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					if b.sleep > 0 {
						select {
						case <-time.After(b.sleep):
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
					b.hits.Add(1)
					// decode (or skip) the JSON body
					var req map[string]any
					_ = dec(&req)
					if interceptor != nil {
						return interceptor(ctx, req, &grpc.UnaryServerInfo{FullMethod: "/" + balancerSvc + "/Ping"},
							func(context.Context, any) (any, error) {
								return map[string]string{"id": b.id}, nil
							})
					}
					return map[string]string{"id": b.id}, nil
				},
			},
		},
		Metadata: "test",
	}, b)
	// Real gRPC health service: required by base.Config{HealthCheck:true} for
	// SubConns to become READY.
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(b.gs, hs)
	return b.gs.Serve(b.lis)
}

func (b *balancerBackend) stop() {
	if b.gs != nil {
		b.gs.Stop()
	}
	if b.lis != nil {
		_ = b.lis.Close()
	}
}

func startBalancerBackend(t *testing.T, id string, sleep time.Duration) *balancerBackend {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &balancerBackend{id: id, lis: lis, sleep: sleep}
	go func() { _ = b.serve() }()
	t.Cleanup(b.stop)
	return b
}

func registerBalancerInst(t *testing.T, cs *catalog.Store, id string, lis net.Listener, weight int) {
	t.Helper()
	host, portStr, _ := net.SplitHostPort(lis.Addr().String())
	var port int
	_, _ = fmt.Sscanf(portStr, "%d", &port)
	if _, err := cs.Register(context.Background(), &catalog.Instance{
		ID:      id,
		Service: "echo",
		Address: host,
		Port:    port,
		Health:  catalog.HealthPassing,
		Weight:  weight,
		Node:    "local",
	}); err != nil {
		t.Fatal(err)
	}
}

// dialBeacon dials beacon:///echo with the beacon resolver wired per-connection
// (avoiding global resolver registration conflicts between tests).
func dialBeacon(t *testing.T, client *sdk.Client, policy string) *grpc.ClientConn {
	t.Helper()
	sdk.RegisterBalancers()
	name := "beacon_" + policy
	if policy == "round_robin" {
		name = "round_robin"
	}
	sc := fmt.Sprintf(`{"loadBalancingConfig": [{%q: {}}]}`, name)
	conn, err := grpc.NewClient("beacon:///echo",
		grpc.WithResolvers(sdk.NewBuilder(client)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(sc),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// pingOnce performs a real RPC through the ClientConn (SubConn) and returns the
// backend id that handled it.
func pingOnce(t *testing.T, conn *grpc.ClientConn, timeout time.Duration) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out map[string]string
	err := conn.Invoke(ctx, "/"+balancerSvc+"/Ping", struct{}{}, &out, grpc.ForceCodec(jsonCodec()))
	if err != nil {
		return "", err
	}
	return out["id"], nil
}

// waitReady waits until the ClientConn reaches READY.
// grpc.NewClient starts in Idle; Connect() kicks off the lazy connection.
func waitReady(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()
	conn.Connect()
	deadline := time.Now().Add(10 * time.Second)
	state := conn.GetState()
	for state != connectivity.Ready && time.Now().Before(deadline) {
		if !conn.WaitForStateChange(context.Background(), state) {
			return
		}
		state = conn.GetState()
	}
	if state != connectivity.Ready {
		t.Fatalf("connection never became READY (state=%v)", state)
	}
}

// TestBalancer_SubConnLifecycle_RealRPCs (TODO-026):
// a real grpc.ClientConn with the beacon resolver + beacon_p2c balancer; real
// SubConns per backend; membership changes push new SubConns in / drop them out.
func TestBalancer_SubConnLifecycle_RealRPCs(t *testing.T) {
	clk := clock.New()
	bus := events.NewBus(clk)
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	st := store.NewMemory(cs, "ap")
	client := sdk.New(sdk.Config{Registry: sdk.StoreAdapter{S: st}, Clock: clk, Bus: bus})

	b0 := startBalancerBackend(t, "b0", 0)
	b1 := startBalancerBackend(t, "b1", 0)
	registerBalancerInst(t, cs, "b0", b0.lis, 1)
	registerBalancerInst(t, cs, "b1", b1.lis, 1)

	conn := dialBeacon(t, client, "p2c")
	waitReady(t, conn)

	// Real RPCs must reach both SubConns.
	seen := map[string]bool{}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		id, err := pingOnce(t, conn, 500*time.Millisecond)
		if err == nil {
			seen[id] = true
		}
		if len(seen) >= 2 {
			break
		}
	}
	if !seen["b0"] || !seen["b1"] {
		t.Fatalf("expected real SubConns for both backends, got %v", seen)
	}

	// Membership change: add b2 → resolver must push it → new SubConn joins pool.
	b2 := startBalancerBackend(t, "b2", 0)
	registerBalancerInst(t, cs, "b2", b2.lis, 1)
	seen2 := map[string]bool{}
	deadline2 := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline2) {
		id, err := pingOnce(t, conn, 500*time.Millisecond)
		if err == nil {
			seen2[id] = true
		}
		if seen2["b2"] {
			break
		}
	}
	if !seen2["b2"] {
		t.Fatalf("new backend b2 never received traffic via SubConn lifecycle, got %v", seen2)
	}

	// Membership change: mark b0 critical → resolver drops it → no more traffic.
	_, _ = cs.UpdateHealth(context.Background(), "b0", catalog.HealthCritical)
	// allow resolver+balancer to react and in-flight RPCs to drain
	time.Sleep(500 * time.Millisecond)
	before := b0.hits.Load()
	deadline3 := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline3) {
		_, _ = pingOnce(t, conn, 500*time.Millisecond)
	}
	if got := b0.hits.Load(); got != before {
		t.Fatalf("critical backend still receiving traffic: before=%d after=%d", before, got)
	}
}

// TestBalancer_P2CSlowInstance (TODO-026): P2C with real RPCs under a slow
// instance must prefer the fast one (outcome also feeds passive health via Done).
func TestBalancer_P2CSlowInstance(t *testing.T) {
	clk := clock.New()
	cs := catalog.NewStore(catalog.WithClock(clk))
	st := store.NewMemory(cs, "ap")
	client := sdk.New(sdk.Config{Registry: sdk.StoreAdapter{S: st}, Clock: clk})

	fast := startBalancerBackend(t, "fast", 0)
	slow := startBalancerBackend(t, "slow", 200*time.Millisecond)
	registerBalancerInst(t, cs, "fast", fast.lis, 1)
	registerBalancerInst(t, cs, "slow", slow.lis, 1)

	conn := dialBeacon(t, client, "p2c")
	waitReady(t, conn)

	// Each worker runs SEQUENTIAL RPCs so later picks observe the slow backend's
	// accumulated inflight. P2C picks 2 at random and chooses the least loaded;
	// once slow accumulates inflight, most picks should land on fast.
	const workers = 12
	const rounds = 8
	hits := make(chan string, workers*rounds)
	errs := 0
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				var out map[string]string
				err := conn.Invoke(ctx, "/"+balancerSvc+"/Ping", struct{}{}, &out, grpc.ForceCodec(jsonCodec()))
				cancel()
				if err != nil {
					errs++
					continue
				}
				hits <- out["id"]
			}
		}()
	}
	wg.Wait()
	close(hits)
	f, s := 0, 0
	for id := range hits {
		if id == "fast" {
			f++
		} else {
			s++
		}
	}
	if f <= s {
		t.Fatalf("P2C should prefer fast under load: fast=%d slow=%d errs=%d", f, s, errs)
	}
	if errs > workers*rounds/3 {
		t.Fatalf("too many RPC errors: %d/%d", errs, workers*rounds)
	}
	t.Logf("P2C under slow instance: fast=%d slow=%d errs=%d", f, s, errs)
}

// TestBalancer_WeightedEndToEnd: resolver weight attributes must reach the
// balancer (TODO-026 weighted end-to-end).
func TestBalancer_WeightedEndToEnd(t *testing.T) {
	clk := clock.New()
	cs := catalog.NewStore(catalog.WithClock(clk))
	st := store.NewMemory(cs, "ap")
	client := sdk.New(sdk.Config{Registry: sdk.StoreAdapter{S: st}, Clock: clk})

	heavy := startBalancerBackend(t, "heavy", 0)
	light := startBalancerBackend(t, "light", 0)
	registerBalancerInst(t, cs, "heavy", heavy.lis, 3)
	registerBalancerInst(t, cs, "light", light.lis, 1)

	conn := dialBeacon(t, client, "wrr")
	waitReady(t, conn)

	// Smooth WRR with 3:1 should give ~75% to heavy.
	const n = 400
	counts := map[string]int{}
	for i := 0; i < n; i++ {
		id, err := pingOnce(t, conn, 500*time.Millisecond)
		if err != nil {
			continue
		}
		counts[id]++
	}
	h := counts["heavy"]
	if h < n/3 || h > 4*n/5 {
		t.Fatalf("weighted distribution off: heavy=%d light=%d of %d", h, counts["light"], n)
	}
	t.Logf("WRR 3:1 → heavy=%d (%.0f%%), light=%d", h, 100*float64(h)/n, counts["light"])
}
