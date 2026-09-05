package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sanskar/beacon/pkg/gossip"
)

type serverMetrics struct {
	registry         *prometheus.Registry
	requests         *prometheus.CounterVec
	duration         *prometheus.HistogramVec
	catalogServices  prometheus.Gauge
	catalogInstances prometheus.Gauge
	catalogIndex     prometheus.Gauge
	watchers         prometheus.Gauge
	watchCacheSize   prometheus.Gauge
	gossipMembers    *prometheus.GaugeVec
}

func newServerMetrics() *serverMetrics {
	m := &serverMetrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "beacon",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total number of HTTP requests handled by the Beacon API.",
		}, []string{"method", "route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "beacon",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request latency in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),
		catalogServices: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "beacon", Subsystem: "catalog", Name: "services",
			Help: "Current number of services in the catalog.",
		}),
		catalogInstances: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "beacon", Subsystem: "catalog", Name: "instances",
			Help: "Current number of instances in the catalog.",
		}),
		catalogIndex: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "beacon", Subsystem: "catalog", Name: "index",
			Help: "Current catalog modification index.",
		}),
		watchers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "beacon", Subsystem: "watch", Name: "watchers",
			Help: "Current number of open catalog watchers.",
		}),
		watchCacheSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "beacon", Subsystem: "watch", Name: "cache_events",
			Help: "Current number of events retained by the watch cache.",
		}),
		gossipMembers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "beacon", Subsystem: "gossip", Name: "members",
			Help: "Current gossip membership count by status.",
		}, []string{"status"}),
	}
	for _, collector := range []prometheus.Collector{
		m.requests, m.duration, m.catalogServices, m.catalogInstances,
		m.catalogIndex, m.watchers, m.watchCacheSize, m.gossipMembers,
	} {
		m.registry.MustRegister(collector)
	}
	return m
}

func (m *serverMetrics) handler(s *Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.refresh(s)
		promhttp.HandlerFor(prometheus.Gatherers{
			prometheus.DefaultGatherer,
			m.registry,
		}, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	})
}

func (m *serverMetrics) refresh(s *Server) {
	if s.store != nil {
		m.catalogServices.Set(float64(len(s.store.ListServices())))
		if snapshot := s.store.Snapshot(); snapshot != nil {
			m.catalogInstances.Set(float64(len(snapshot.Instances)))
			m.catalogIndex.Set(float64(snapshot.Index))
		}
	}

	m.watchers.Set(0)
	m.watchCacheSize.Set(0)
	if s.watch != nil {
		stats := s.watch.Stats()
		if total, ok := stats["total_watchers"].(int); ok {
			m.watchers.Set(float64(total))
		}
		if cache, ok := stats["cache"].(map[string]any); ok {
			if size, ok := cache["size"].(int); ok {
				m.watchCacheSize.Set(float64(size))
			}
		}
	}

	for _, status := range []gossip.MemberStatus{
		gossip.StatusAlive, gossip.StatusSuspect, gossip.StatusFailed, gossip.StatusLeft,
	} {
		m.gossipMembers.WithLabelValues(string(status)).Set(0)
	}
	if s.membership != nil {
		counts := make(map[string]int)
		for _, member := range s.membership.Members() {
			counts[string(member.Status)]++
		}
		for status, count := range counts {
			m.gossipMembers.WithLabelValues(status).Set(float64(count))
		}
	}
}

func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		observed := &metricsResponseWriter{ResponseWriter: w}
		defer func() {
			status := observed.status
			if status == 0 {
				status = http.StatusOK
			}
			route := requestRoute(r)
			s.metrics.requests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
			s.metrics.duration.WithLabelValues(r.Method, route).Observe(time.Since(started).Seconds())
		}()
		next.ServeHTTP(observed, r)
	})
}

func requestRoute(r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	path := r.URL.Path
	for _, prefix := range []string{
		"/v1/agent/service/node/",
		"/v1/agent/service/deregister/",
		"/v1/agent/check/pass/",
		"/v1/agent/check/warn/",
		"/v1/agent/check/fail/",
		"/v1/agent/maintenance/",
		"/v1/catalog/service/",
		"/v1/health/service/",
		"/v1/query/",
		"/v1/connect/intentions/",
		"/v1/lab/consistency/",
	} {
		if strings.HasPrefix(path, prefix) {
			return prefix
		}
	}
	for _, route := range []string{
		"/v1/agent/service/register", "/v1/agent/members", "/v1/catalog/services",
		"/v1/query", "/v1/connect/intentions", "/v1/xds/status", "/v1/telemetry/calls",
		"/v1/telemetry/calls/record", "/v1/watch/stats", "/v1/lab/consistency",
		"/v1/bench/gossip-contrast", "/v1/events", "/metrics", "/health", "/ready",
	} {
		if path == route {
			return route
		}
	}
	return "unknown"
}

type metricsResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *metricsResponseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *metricsResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

func (w *metricsResponseWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *metricsResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
