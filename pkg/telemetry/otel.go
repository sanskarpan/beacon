// Package telemetry provides observability primitives for beacon.
//
// TODO-006: OpenTelemetry end-to-end tracing.
// This file wires OTel spans at each hop with shared TraceID / trace context.
package telemetry

import (
	"context"
	"os"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

var (
	globalTracer trace.Tracer
	initOnce     sync.Once
	mu           sync.Mutex
)

// ResetForTest clears the once guard so tests can call Init with different ids.
// Not for production use.
func ResetForTest() {
	mu.Lock()
	defer mu.Unlock()
	initOnce = sync.Once{}
	globalTracer = nil
	otel.SetTracerProvider(tracenoop.NewTracerProvider())
}

// Init sets up the global OTel provider with a simple stdout exporter.
// Call this once at server startup. id and serviceVersion identify the node.
func Init(id, serviceVersion string) {
	mu.Lock()
	defer mu.Unlock()
	// allow re-init if ResetForTest was called (initOnce zero)
	initOnce.Do(func() {
		res, _ := resource.Merge(
			resource.Default(),
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("beacon"),
				semconv.ServiceInstanceIDKey.String(id),
				semconv.ServiceVersionKey.String(serviceVersion),
			),
		)

		// Use OTLP exporter if BEACON_OTEL_ENDPOINT is set, else stdout.
		var exp sdktrace.SpanExporter
		if ep := os.Getenv("BEACON_OTEL_ENDPOINT"); ep != "" {
			exp = newOTLPExporter(ep)
		} else {
			exp = newStdoutExporter()
		}

		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		globalTracer = otel.Tracer("beacon")
	})
}

// Tracer returns the global beacon tracer. Panics if Init was not called.
func Tracer() trace.Tracer {
	if globalTracer == nil {
		// Fallback: no-op tracer if Init was not called.
		return otel.Tracer("beacon")
	}
	return globalTracer
}

// SpanFromContext extracts a span from context (returns a no-op span if none).
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// StartSpan is a convenience wrapper around Tracer().Start().
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return Tracer().Start(ctx, name, opts...)
}

// Event records a named event on the current span.
func Event(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.AddEvent(name, trace.WithAttributes(attrs...))
	}
}

// Error records an error on the current span.
func Error(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// SetAttributes sets attributes on the current span.
func SetAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		span.SetAttributes(attrs...)
	}
}
