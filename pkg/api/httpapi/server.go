// Package httpapi serves the Consul-style HTTP API on :8500.
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sanskar/beacon/pkg/agent"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	"github.com/sanskar/beacon/pkg/lab"
	"github.com/sanskar/beacon/pkg/mesh"
	"github.com/sanskar/beacon/pkg/query"
	"github.com/sanskar/beacon/pkg/sim"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/telemetry"
	"github.com/sanskar/beacon/pkg/watch"
	"github.com/sanskar/beacon/pkg/xds"
	"golang.org/x/time/rate"
)

// Server is the HTTP control/data plane.
type Server struct {
	store        store.CatalogStore
	agent        *agent.Agent
	bus          *events.Bus
	clk          clock.Clock
	watch        *watch.Registry
	membership   gossip.Membership
	onRegister   func(inst *catalog.Instance, idx uint64)
	onDeregister func(id, service string, idx uint64)
	mux          *http.ServeMux
	// rate limit per remote addr
	limiters sync.Map // string -> *httpLimiterEntry
	rps      rate.Limit
	burst    int
	queries    *query.Store
	intentions *mesh.IntentionStore
	xds        *xds.Server
	calls      *telemetry.CallGraph
	lab        *lab.ConsistencyLab
}

type httpLimiterEntry struct {
	lim  *rate.Limiter
	last time.Time
	mu   sync.Mutex
}

// Config for the HTTP server.
type Config struct {
	Store        store.CatalogStore
	Agent        *agent.Agent
	Bus          *events.Bus
	Clock        clock.Clock
	Watch        *watch.Registry
	Membership   gossip.Membership
	OnRegister   func(inst *catalog.Instance, idx uint64)
	OnDeregister func(id, service string, idx uint64)
	RPS          float64
	Burst        int
	Queries      *query.Store
	Intentions   *mesh.IntentionStore
	XDS          *xds.Server
	Calls        *telemetry.CallGraph
	Lab          *lab.ConsistencyLab
}

// New creates the HTTP API.
func New(cfg Config) *Server {
	if cfg.Clock == nil {
		cfg.Clock = clock.New()
	}
	if cfg.RPS <= 0 {
		cfg.RPS = 1000
	}
	if cfg.Burst <= 0 {
		cfg.Burst = 100
	}
	calls := cfg.Calls
	if calls == nil {
		calls = telemetry.NewCallGraph(cfg.Clock, cfg.Bus, 0)
	}
	intentions := cfg.Intentions
	if intentions == nil {
		intentions = mesh.NewIntentionStore()
	}
	labInst := cfg.Lab
	if labInst == nil {
		labInst = lab.NewConsistencyLab(cfg.Clock, cfg.Bus)
	}
	s := &Server{
		store:        cfg.Store,
		agent:        cfg.Agent,
		bus:          cfg.Bus,
		clk:          cfg.Clock,
		watch:        cfg.Watch,
		membership:   cfg.Membership,
		onRegister:   cfg.OnRegister,
		onDeregister: cfg.OnDeregister,
		mux:          http.NewServeMux(),
		rps:          rate.Limit(cfg.RPS),
		burst:        cfg.Burst,
		queries:      cfg.Queries,
		intentions:   intentions,
		xds:          cfg.XDS,
		calls:        calls,
		lab:          labInst,
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/v1/agent/service/register", s.wrap(s.register))
	s.mux.HandleFunc("/v1/agent/service/deregister/", s.wrap(s.deregister))
	s.mux.HandleFunc("/v1/agent/check/pass/", s.wrap(s.checkPass))
	s.mux.HandleFunc("/v1/agent/check/warn/", s.wrap(s.checkWarn))
	s.mux.HandleFunc("/v1/agent/check/fail/", s.wrap(s.checkFail))
	s.mux.HandleFunc("/v1/agent/maintenance/", s.wrap(s.maintenance))
	s.mux.HandleFunc("/v1/agent/members", s.wrap(s.members))
	s.mux.HandleFunc("/v1/catalog/services", s.wrap(s.catalogServices))
	s.mux.HandleFunc("/v1/catalog/service/", s.wrap(s.catalogService))
	s.mux.HandleFunc("/v1/health/service/", s.wrap(s.healthService))
	s.mux.HandleFunc("/v1/query", s.wrap(s.preparedQueryRoot))
	s.mux.HandleFunc("/v1/query/", s.wrap(s.preparedQueryItem))
	s.mux.HandleFunc("/v1/connect/intentions", s.wrap(s.intentionsRoot))
	s.mux.HandleFunc("/v1/connect/intentions/", s.wrap(s.intentionsItem))
	s.mux.HandleFunc("/v1/xds/status", s.wrap(s.xdsStatus))
	s.mux.HandleFunc("/v1/telemetry/calls", s.wrap(s.telemetryCalls))
	s.mux.HandleFunc("/v1/telemetry/calls/record", s.wrap(s.telemetryRecord))
	s.mux.HandleFunc("/v1/watch/stats", s.wrap(s.watchStats))
	s.mux.HandleFunc("/v1/lab/consistency", s.wrap(s.labConsistency))
	s.mux.HandleFunc("/v1/lab/consistency/", s.wrap(s.labConsistencyAction))
	s.mux.HandleFunc("/v1/bench/gossip-contrast", s.wrap(s.gossipContrast))
	s.mux.HandleFunc("/v1/events", s.sse)
	s.mux.Handle("/metrics", promhttp.Handler())
	s.mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	s.mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
}

// Handler returns the root handler.
func (s *Server) Handler() http.Handler { return s.mux }

// ListenAndServe starts the server.
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s.mux)
}

func (s *Server) wrap(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.allow(r) {
			w.Header().Set("Retry-After", "1")
			writeErr(w, http.StatusTooManyRequests, "rate_limited", "too many requests")
			return
		}
		next(w, r)
	}
}

func (s *Server) allow(r *http.Request) bool {
	key := r.RemoteAddr
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		key = host
	}
	if key == "" {
		key = "unknown"
	}
	now := s.clk.Now()
	// opportunistic GC when map grows
	var count int
	s.limiters.Range(func(k, v any) bool { count++; return count <= 512 })
	if count > 512 {
		s.limiters.Range(func(k, v any) bool {
			e := v.(*httpLimiterEntry)
			e.mu.Lock()
			idle := now.Sub(e.last)
			e.mu.Unlock()
			if idle > 10*time.Minute {
				s.limiters.Delete(k)
			}
			return true
		})
	}
	val, _ := s.limiters.LoadOrStore(key, &httpLimiterEntry{lim: rate.NewLimiter(s.rps, s.burst), last: now})
	e := val.(*httpLimiterEntry)
	e.mu.Lock()
	e.last = now
	e.mu.Unlock()
	return e.lim.Allow()
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method", "PUT required")
		return
	}
	var inst catalog.Instance
	if err := json.NewDecoder(r.Body).Decode(&inst); err != nil {
		writeErr(w, http.StatusBadRequest, "decode", err.Error())
		return
	}
	ctx := r.Context()
	var idx uint64
	var err error
	if s.agent != nil {
		if err = s.agent.Register(ctx, &inst); err != nil {
			writeErr(w, http.StatusInternalServerError, "register", err.Error())
			return
		}
		idx = s.store.Index()
	} else {
		idx, err = s.store.Register(ctx, &inst)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "register", err.Error())
			return
		}
	}
	if s.onRegister != nil {
		s.onRegister(&inst, idx)
	}
	w.Header().Set("X-Beacon-Index", strconv.FormatUint(idx, 10))
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"id": inst.ID, "index": idx})
}

func (s *Server) deregister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method", "PUT required")
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/agent/service/deregister/")
	ctx := r.Context()
	var service string
	if inst, ok := s.store.GetInstance(id); ok {
		service = inst.Service
	}
	var idx uint64
	if s.agent != nil {
		_ = s.agent.Deregister(ctx, id)
		idx = s.store.Index()
	} else {
		idx, _ = s.store.Deregister(ctx, id)
	}
	if s.onDeregister != nil {
		s.onDeregister(id, service, idx)
	}
	w.Header().Set("X-Beacon-Index", strconv.FormatUint(idx, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) checkPass(w http.ResponseWriter, r *http.Request) {
	s.ttlPush(w, r, "/v1/agent/check/pass/", catalog.HealthPassing)
}
func (s *Server) checkWarn(w http.ResponseWriter, r *http.Request) {
	s.ttlPush(w, r, "/v1/agent/check/warn/", catalog.HealthWarning)
}
func (s *Server) checkFail(w http.ResponseWriter, r *http.Request) {
	s.ttlPush(w, r, "/v1/agent/check/fail/", catalog.HealthCritical)
}

func (s *Server) ttlPush(w http.ResponseWriter, r *http.Request, prefix string, status catalog.HealthStatus) {
	id := strings.TrimPrefix(r.URL.Path, prefix)
	// id format: checkID or instanceID/checkID
	parts := strings.SplitN(id, "/", 2)
	if s.agent != nil && len(parts) == 2 {
		_ = s.agent.TTLPass(parts[0], parts[1], status, "")
	} else if len(parts) == 1 {
		_, _ = s.store.UpdateHealth(r.Context(), parts[0], status)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) maintenance(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/agent/maintenance/")
	enable := r.URL.Query().Get("enable") == "true"
	status := catalog.HealthPassing
	if enable {
		status = catalog.HealthMaint
	}
	_, _ = s.store.UpdateHealth(r.Context(), id, status)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) members(w http.ResponseWriter, r *http.Request) {
	_ = r
	if s.membership == nil {
		_ = json.NewEncoder(w).Encode([]any{})
		return
	}
	members := s.membership.Members()
	out := make([]map[string]any, 0, len(members))
	for _, m := range members {
		out = append(out, map[string]any{
			"id":          string(m.ID),
			"name":        m.Name,
			"addr":        m.Addr,
			"port":        m.Port,
			"status":      string(m.Status),
			"incarnation": m.Incarnation,
			"meta":        m.Meta,
		})
	}
	_ = json.NewEncoder(w).Encode(out)
}

func (s *Server) catalogServices(w http.ResponseWriter, r *http.Request) {
	_ = r
	_ = json.NewEncoder(w).Encode(s.store.ListServices())
}

func (s *Server) catalogService(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/catalog/service/")
	s.blockingRead(w, r, name, false)
}

func (s *Server) healthService(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/health/service/")
	s.blockingRead(w, r, name, true)
}

func (s *Server) blockingRead(w http.ResponseWriter, r *http.Request, service string, healthPath bool) {
	opts := catalog.QueryOptions{}
	q := r.URL.Query()
	if v := q.Get("index"); v != "" {
		opts.MinIndex, _ = strconv.ParseUint(v, 10, 64)
	}
	if v := q.Get("wait"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			opts.Wait = d
		}
	}
	// cap wait to avoid holding handlers forever (M11)
	if opts.Wait > 5*time.Minute {
		opts.Wait = 5 * time.Minute
	}
	if q.Get("passing") == "true" || healthPath && q.Get("passing") != "false" {
		// health path defaults to all; passing=true filters
		if q.Get("passing") == "true" {
			opts.Passing = true
		}
	}
	if t := q.Get("tag"); t != "" {
		opts.Tags = []string{t}
	}
	if f := q.Get("filter"); f != "" {
		opts.Filter = f
	}
	opts.Consistent = q.Get("consistent") == "true"
	opts.Stale = q.Get("stale") == "true"

	var res *catalog.Result
	var err error
	if opts.MinIndex > 0 || opts.Wait > 0 {
		res, err = watch.BlockingQuery(r.Context(), asCatalog(s.store), service, opts, s.clk, nil)
	} else {
		res = s.store.GetNow(service, opts)
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "query", err.Error())
		return
	}
	w.Header().Set("X-Beacon-Index", strconv.FormatUint(res.Index, 10))
	if res.Stale || opts.Stale {
		w.Header().Set("X-Beacon-Stale", "true")
		w.Header().Set("X-Beacon-Last-Contact", res.LastContact.String())
	}
	_ = json.NewEncoder(w).Encode(res.Instances)
}

func asCatalog(s store.CatalogStore) *catalog.Store {
	// Prefer underlying catalog when available
	type hasLocal interface{ Local() *catalog.Store }
	if g, ok := s.(hasLocal); ok {
		return g.Local()
	}
	type hasFSM interface{ FSM() *catalog.Store }
	if r, ok := s.(hasFSM); ok {
		return r.FSM()
	}
	if m, ok := s.(*store.MemoryStore); ok {
		return m.Store
	}
	// fallback: wrap via snapshot round-trip store
	cs := catalog.NewStore()
	_ = cs.Restore(s.Snapshot())
	return cs
}

func (s *Server) sse(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "sse unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	if s.bus == nil {
		return
	}
	ch, unsub := s.bus.Subscribe(256)
	defer unsub()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiError{Code: code, Message: msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// Ensure sim import used (gossip contrast)
var _ = sim.MeasureGossipContrast

// Ensure context used
var _ = context.Background
