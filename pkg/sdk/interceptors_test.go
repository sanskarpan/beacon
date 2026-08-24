package sdk_test

import (
	"context"
	"sync"
	"testing"

	"github.com/example/grpc-service/pkg/interceptors"
	"github.com/example/grpc-service/pkg/logging"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/sdk"
	"github.com/sanskar/beacon/pkg/store"
	"google.golang.org/grpc"
)

func init() {
	logging.Init("error", "console")
}

// TestInterceptors_OrderAndMetrics verifies the interceptors project chain is
// imported, ordered correctly, emits metrics, and OutcomeReporter is appended.
func TestInterceptors_OrderAndMetrics(t *testing.T) {
	var order []string
	var mu sync.Mutex
	record := func(name string) grpc.UnaryClientInterceptor {
		return func(ctx context.Context, method string, req, reply any,
			cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}

	// External chain pieces (same order as ExternalChain.Unary without retry noise)
	external := sdk.ChainFrom(
		record("logging"),
		interceptors.UnaryClientMetricsInterceptor,
		record("metrics-done"),
	)

	clk := clock.New()
	bus := events.NewBus(clk)
	cs := catalog.NewStore(catalog.WithClock(clk), catalog.WithBus(bus))
	st := store.NewMemory(cs, "ap")
	client := sdk.New(sdk.Config{Registry: sdk.StoreAdapter{S: st}, Clock: clk, Bus: bus})

	// Full beacon dial chain: external then OutcomeReporter
	full := sdk.ChainFrom(external, client.OutcomeReporter(), record("outcome-done"))

	method := "/test.Service/Ping"
	before := getCounter(interceptors.ClientTotalRequests.WithLabelValues(method))

	err := full(context.Background(), method, "req", "reply", nil,
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			mu.Lock()
			order = append(order, "invoker")
			mu.Unlock()
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}

	after := getCounter(interceptors.ClientTotalRequests.WithLabelValues(method))
	if after <= before {
		t.Fatalf("expected UnaryClientMetricsInterceptor to increment counter: before=%v after=%v", before, after)
	}

	mu.Lock()
	defer mu.Unlock()
	// logging → metrics (inside interceptor) → metrics-done → outcome reporter → outcome-done → invoker
	// OutcomeReporter wraps invoker, so order is: logging, metrics-done (after metrics ic calls next), ...
	// ChainFrom wraps last-to-first: first interceptor is outermost.
	// external = logging outer, metrics middle, metrics-done inner → then OutcomeReporter → outcome-done
	// Call order: logging, metrics, metrics-done, OutcomeReporter, outcome-done, invoker
	if len(order) < 4 {
		t.Fatalf("order too short: %v", order)
	}
	if order[0] != "logging" {
		t.Fatalf("outermost should be logging, got %v", order)
	}
	if order[len(order)-1] != "invoker" {
		t.Fatalf("innermost should be invoker, got %v", order)
	}
	// outcome-done must appear before invoker (reporter wraps it)
	foundOutcome := false
	for _, n := range order {
		if n == "outcome-done" {
			foundOutcome = true
		}
	}
	if !foundOutcome {
		t.Fatalf("OutcomeReporter chain missing: %v", order)
	}

	// Server-side defaults import interceptors project.
	su := sdk.DefaultServerUnaryInterceptors()
	if len(su) < 4 {
		t.Fatalf("server unary chain too short: %d", len(su))
	}
	ss := sdk.DefaultServerStreamInterceptors()
	if len(ss) < 4 {
		t.Fatalf("server stream chain too short: %d", len(ss))
	}

	// ExternalChain implements InterceptorChain
	var _ sdk.InterceptorChain = sdk.NewExternalChain("tok")
}

func getCounter(c prometheus.Counter) float64 {
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		return 0
	}
	return m.GetCounter().GetValue()
}
