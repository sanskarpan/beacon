package telemetry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sanskar/beacon/pkg/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TODO-006: OTel tracing wiring — verify Init + span chain.
func TestOTel_InitAndSpanChain(t *testing.T) {
	// Init with stdout exporter (no BEACON_OTEL_ENDPOINT set).
	telemetry.Init("test-node-1", "0.0.1")

	ctx := context.Background()

	// Start a parent span.
	ctx, parent := telemetry.StartSpan(ctx, "register")
	defer parent.End()

	// Record an event.
	telemetry.Event(ctx, "catalog.write")

	// Start a child span.
	ctx2, child := telemetry.StartSpan(ctx, "gossip.push")
	telemetry.SetAttributes(ctx2, attribute.String("node", "node-0"))
	telemetry.Event(ctx2, "piggyback")
	child.End()

	// Verify trace ID is propagated.
	traceID := telemetry.TraceIDFromContext(ctx)
	if traceID == "" {
		t.Fatal("expected non-empty trace ID in context")
	}

	t.Logf("trace chain: parent=%s child=%s traceID=%s",
		parent.SpanContext().SpanID(), child.SpanContext().SpanID(), traceID)
}

// TestOTel_NoInit_NoPanic verifies that using telemetry before Init doesn't crash.
func TestOTel_NoInit_NoPanic(t *testing.T) {
	ctx := context.Background()
	ctx, span := telemetry.StartSpan(ctx, "before-init")
	defer span.End()

	telemetry.Event(ctx, "test-event")
	telemetry.SetAttributes(ctx, attribute.String("key", "value"))

	traceID := telemetry.TraceIDFromContext(ctx)
	t.Logf("traceID before init: %q (may be empty)", traceID)
}

func TestOTel_HTTPMiddlewarePropagatesIncomingTrace(t *testing.T) {
	telemetry.Init("http-test-node", "0.0.1")

	parentCtx, parent := telemetry.StartSpan(context.Background(), "caller")
	defer parent.End()
	header := http.Header{}
	otel.GetTextMapPropagator().Inject(parentCtx, propagation.HeaderCarrier(header))

	var received trace.SpanContext
	handler := telemetry.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = trace.SpanFromContext(r.Context()).SpanContext()
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "http://beacon.test/v1/catalog/services", nil)
	req.Header = header
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d want %d", response.Code, http.StatusNoContent)
	}
	if !received.IsValid() {
		t.Fatal("expected server span in handler context")
	}
	if received.TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("trace id=%s want %s", received.TraceID(), parent.SpanContext().TraceID())
	}
	if received.SpanID() == parent.SpanContext().SpanID() {
		t.Fatal("expected HTTP middleware to create a child span")
	}
}
