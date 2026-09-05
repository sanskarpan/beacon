package telemetry

import (
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HTTPMiddleware creates a server span for each request and extracts the
// incoming W3C trace context before invoking the wrapped handler.
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := StartSpan(ctx, "HTTP "+r.Method, trace.WithSpanKind(trace.SpanKindServer))
		traced := &httpResponseWriter{ResponseWriter: w}

		defer func() {
			route := r.Pattern
			if route == "" {
				route = "unknown"
			}
			status := traced.status
			if status == 0 {
				status = http.StatusOK
			}
			span.SetName(fmt.Sprintf("HTTP %s %s", r.Method, route))
			span.SetAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("http.route", route),
				attribute.Int("http.response.status_code", status),
			)
			if status >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, http.StatusText(status))
			}
			span.End()
		}()

		next.ServeHTTP(traced, r.WithContext(ctx))
	})
}

type httpResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *httpResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *httpResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

// Flush preserves streaming handlers such as the event SSE endpoint.
func (w *httpResponseWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap allows http.NewResponseController to reach optional interfaces on
// the underlying writer.
func (w *httpResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
