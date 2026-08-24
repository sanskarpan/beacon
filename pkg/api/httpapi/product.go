package httpapi

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sanskar/beacon/pkg/mesh"
	"github.com/sanskar/beacon/pkg/query"
	"github.com/sanskar/beacon/pkg/sim"
)

// --- Prepared queries (TODO-019) ---

// GET /v1/query  — list
// PUT /v1/query  — create/upsert body
func (s *Server) preparedQueryRoot(w http.ResponseWriter, r *http.Request) {
	if s.queries == nil {
		writeErr(w, http.StatusServiceUnavailable, "queries", "prepared queries not configured")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.queries.List())
	case http.MethodPut, http.MethodPost:
		var q query.PreparedQuery
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			writeErr(w, http.StatusBadRequest, "decode", err.Error())
			return
		}
		if err := s.queries.Create(&q); err != nil {
			writeErr(w, http.StatusBadRequest, "create", err.Error())
			return
		}
		writeJSON(w, q)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method", "GET or PUT")
	}
}

// GET    /v1/query/{id}           — get
// GET    /v1/query/{id}/execute   — execute with failover
// DELETE /v1/query/{id}           — delete
func (s *Server) preparedQueryItem(w http.ResponseWriter, r *http.Request) {
	if s.queries == nil {
		writeErr(w, http.StatusServiceUnavailable, "queries", "prepared queries not configured")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/query/")
	path = strings.Trim(path, "/")
	if path == "" {
		s.preparedQueryRoot(w, r)
		return
	}
	parts := strings.Split(path, "/")
	id := parts[0]
	if len(parts) > 1 && parts[1] == "execute" {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method", "GET or POST")
			return
		}
		res, err := s.queries.Execute(r.Context(), id)
		if err != nil {
			writeErr(w, http.StatusNotFound, "execute", err.Error())
			return
		}
		writeJSON(w, res)
		return
	}
	switch r.Method {
	case http.MethodGet:
		q, ok := s.queries.Get(id)
		if !ok {
			writeErr(w, http.StatusNotFound, "not_found", "prepared query not found")
			return
		}
		writeJSON(w, q)
	case http.MethodDelete:
		s.queries.Delete(id)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method", "GET or DELETE")
	}
}

// --- Intentions (TODO-020) ---

// GET /v1/connect/intentions — list
// PUT /v1/connect/intentions — create/upsert
func (s *Server) intentionsRoot(w http.ResponseWriter, r *http.Request) {
	if s.intentions == nil {
		writeErr(w, http.StatusServiceUnavailable, "intentions", "not configured")
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.intentions.List())
	case http.MethodPut, http.MethodPost:
		var i mesh.Intention
		if err := json.NewDecoder(r.Body).Decode(&i); err != nil {
			writeErr(w, http.StatusBadRequest, "decode", err.Error())
			return
		}
		if i.Source == "" || i.Destination == "" {
			writeErr(w, http.StatusBadRequest, "invalid", "source and destination required")
			return
		}
		if i.Action == "" {
			i.Action = mesh.Allow
		}
		s.intentions.Upsert(i)
		writeJSON(w, i)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method", "GET or PUT")
	}
}

// DELETE /v1/connect/intentions/{source}/{dest}
func (s *Server) intentionsItem(w http.ResponseWriter, r *http.Request) {
	if s.intentions == nil {
		writeErr(w, http.StatusServiceUnavailable, "intentions", "not configured")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/connect/intentions/")
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		writeErr(w, http.StatusBadRequest, "path", "want /v1/connect/intentions/{source}/{dest}")
		return
	}
	src, dest := parts[0], parts[1]
	if r.Method != http.MethodDelete {
		writeErr(w, http.StatusMethodNotAllowed, "method", "DELETE")
		return
	}
	s.intentions.Delete(src, dest)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// --- xDS status (TODO-020) ---

// GET /v1/xds/status[?node=…]
func (s *Server) xdsStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method", "GET")
		return
	}
	if s.xds == nil {
		writeJSON(w, map[string]any{
			"configured": false,
			"nodes":      []any{},
			"detail":     "xDS control plane not attached",
		})
		return
	}
	node := r.URL.Query().Get("node")
	writeJSON(w, s.xds.Status(node))
}

// --- Telemetry call graph (TODO-047) ---

// GET /v1/telemetry/calls
func (s *Server) telemetryCalls(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method", "GET")
		return
	}
	if s.calls == nil {
		writeJSON(w, []any{})
		return
	}
	writeJSON(w, s.calls.Edges())
}

// POST /v1/telemetry/calls/record  {source, target, error?: bool}
func (s *Server) telemetryRecord(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		writeErr(w, http.StatusMethodNotAllowed, "method", "POST")
		return
	}
	if s.calls == nil {
		writeErr(w, http.StatusServiceUnavailable, "calls", "not configured")
		return
	}
	var body struct {
		Source string `json:"source"`
		Target string `json:"target"`
		Error  bool   `json:"error"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "decode", err.Error())
		return
	}
	var err error
	if body.Error {
		err = errCallFailed
	}
	s.calls.Record(body.Source, body.Target, err)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}

type callFailed string

func (e callFailed) Error() string { return string(e) }

const errCallFailed = callFailed("rpc failed")

// --- Watch stats (TODO-049) ---

// GET /v1/watch/stats
func (s *Server) watchStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method", "GET")
		return
	}
	if s.watch == nil {
		writeJSON(w, map[string]any{"total_watchers": 0, "watchers": []any{}, "cache": map[string]any{}})
		return
	}
	writeJSON(w, s.watch.Stats())
}

// --- Consistency lab (TODO-050) ---

// GET /v1/lab/consistency
func (s *Server) labConsistency(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method", "GET")
		return
	}
	if s.lab == nil {
		writeErr(w, http.StatusServiceUnavailable, "lab", "not configured")
		return
	}
	writeJSON(w, s.lab.Snapshot())
}

// POST /v1/lab/consistency/{partition|heal|write-ap|write-cp}
func (s *Server) labConsistencyAction(w http.ResponseWriter, r *http.Request) {
	if s.lab == nil {
		writeErr(w, http.StatusServiceUnavailable, "lab", "not configured")
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/v1/lab/consistency/")
	action = strings.Trim(action, "/")
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		writeErr(w, http.StatusMethodNotAllowed, "method", "POST")
		return
	}
	switch action {
	case "partition":
		s.lab.Partition()
		writeJSON(w, s.lab.Snapshot())
	case "heal":
		s.lab.Heal()
		writeJSON(w, s.lab.Snapshot())
	case "write-ap":
		side := r.URL.Query().Get("side")
		if side == "" {
			side = "a"
		}
		id, err := s.lab.WriteAP(side)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "write", err.Error())
			return
		}
		st := s.lab.Snapshot()
		writeJSON(w, map[string]any{"id": id, "status": st})
	case "write-cp":
		minority := r.URL.Query().Get("minority") == "true"
		id, err := s.lab.WriteCP(minority)
		if err != nil {
			// still return status so console can show rejection
			writeJSON(w, map[string]any{
				"id": id, "error": err.Error(), "status": s.lab.Snapshot(),
			})
			return
		}
		writeJSON(w, map[string]any{"id": id, "status": s.lab.Snapshot()})
	default:
		writeErr(w, http.StatusNotFound, "action", "unknown lab action")
	}
}

// GET /v1/bench/gossip-contrast — returns cached artifact or computes contrast (TODO-012).
func (s *Server) gossipContrast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method", "GET")
		return
	}
	// Prefer on-disk artifact from `beacon bench contrast` when present.
	if b, err := os.ReadFile("tmp/sim/gossip_contrast.json"); err == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
		return
	}
	c := sim.MeasureGossipContrast(10, 5, 30*time.Second)
	_ = sim.WriteContrastJSON("tmp/sim", c)
	writeJSON(w, c)
}
