package telemetry

import (
	"context"
	"fmt"
	"log"
	"sync"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// newStdoutExporter returns a SpanExporter that logs to stdout (for dev/demo).
func newStdoutExporter() sdktrace.SpanExporter {
	return &stdoutExporter{}
}

type stdoutExporter struct{}

func (e *stdoutExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, s := range spans {
		dur := s.EndTime().Sub(s.StartTime())
		log.Printf("[otel] %s  dur=%s  name=%s", s.SpanContext().SpanID(), dur, s.Name())
	}
	return nil
}

func (e *stdoutExporter) Shutdown(_ context.Context) error { return nil }

// newOTLPExporter creates a real OTLP gRPC exporter to the given endpoint.
// The exporter is wrapped in otlpFallback so a mid-run OTLP outage degrades
// to stdout logging instead of dropping spans silently.
func newOTLPExporter(endpoint string) sdktrace.SpanExporter {
	exp, err := newOTLPGRPCExporter(endpoint)
	if err != nil {
		log.Printf("[otel] OTLP exporter unavailable (%v), falling back to stdout", err)
		return newStdoutExporter()
	}
	return &otlpFallback{inner: exp}
}

func newOTLPGRPCExporter(endpoint string) (sdktrace.SpanExporter, error) {
	ctx := context.Background()
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp grpc: %w", err)
	}
	return exp, nil
}

// otlpFallback wraps an inner exporter with stdout fallback.
type otlpFallback struct {
	inner sdktrace.SpanExporter
	mu    sync.Mutex
}

func (o *otlpFallback) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.inner != nil {
		return o.inner.ExportSpans(ctx, spans)
	}
	for _, s := range spans {
		dur := s.EndTime().Sub(s.StartTime())
		fmt.Printf("[otel:fallback] %s  dur=%s  name=%s\n", s.SpanContext().SpanID(), dur, s.Name())
	}
	return nil
}

func (o *otlpFallback) Shutdown(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.inner != nil {
		return o.inner.Shutdown(ctx)
	}
	return nil
}

// TraceIDFromContext is a helper that returns the trace ID from context.
func TraceIDFromContext(ctx context.Context) string {
	sp := trace.SpanFromContext(ctx)
	if sp == nil {
		return ""
	}
	return sp.SpanContext().TraceID().String()
}
