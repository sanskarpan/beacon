package telemetry_test

import (
	"context"
	"testing"

	"github.com/sanskar/beacon/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
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
