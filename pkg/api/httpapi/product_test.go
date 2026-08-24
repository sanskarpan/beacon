package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sanskar/beacon/pkg/api/httpapi"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/query"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/watch"
	"github.com/sanskar/beacon/pkg/xds"
)

func TestPreparedQueryHTTP_CRUDAndExecute(t *testing.T) {
	cs := catalog.NewStore()
	st := store.NewMemory(cs, "ap")
	// Local empty; remote DC has the instance for failover.
	remoteCS := catalog.NewStore()
	_, _ = remoteCS.Register(context.Background(), &catalog.Instance{
		ID: "r1", Service: "payments", Address: "10.0.0.9", Port: 8080,
		Health: catalog.HealthPassing,
	})
	pq := query.New(st)
	pq.RegisterDC("dc2", store.NewMemory(remoteCS, "ap"))

	srv := httpapi.New(httpapi.Config{Store: st, Queries: pq, RPS: 1e6, Burst: 1e6})
	h := srv.Handler()

	// Create
	body := `{"id":"pay-q","name":"pay-q","service":"payments","passing_only":true,"failover":{"datacenters":["dc2"]}}`
	req := httptest.NewRequest(http.MethodPut, "/v1/query", bytes.NewReader([]byte(body)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}

	// List
	req = httptest.NewRequest(http.MethodGet, "/v1/query", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var list []query.PreparedQuery
	_ = json.NewDecoder(rec.Body).Decode(&list)
	if len(list) != 1 {
		t.Fatalf("list: %+v", list)
	}

	// Execute with failover to dc2
	req = httptest.NewRequest(http.MethodGet, "/v1/query/pay-q/execute", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("execute: %d %s", rec.Code, rec.Body.String())
	}
	var res catalog.Result
	_ = json.NewDecoder(rec.Body).Decode(&res)
	if len(res.Instances) != 1 || res.Instances[0].ID != "r1" {
		t.Fatalf("failover result: %+v", res)
	}

	// Delete
	req = httptest.NewRequest(http.MethodDelete, "/v1/query/pay-q", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("delete: %d", rec.Code)
	}
}

func TestConsoleObservatoryHTTP(t *testing.T) {
	cs := catalog.NewStore()
	st := store.NewMemory(cs, "ap")
	wr := watch.NewRegistry(cs)
	// Open a watcher so stats are non-empty.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := wr.Watch(ctx, "payments", 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = ch

	srv := httpapi.New(httpapi.Config{Store: st, Watch: wr, RPS: 1e6, Burst: 1e6})
	h := srv.Handler()

	// Seed call graph
	req := httptest.NewRequest(http.MethodPost, "/v1/telemetry/calls/record",
		bytes.NewReader([]byte(`{"source":"web","target":"payments","error":false}`)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("record: %d %s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/telemetry/calls", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}

	// Watch stats
	req = httptest.NewRequest(http.MethodGet, "/v1/watch/stats", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
	var stats map[string]any
	_ = json.NewDecoder(rec.Body).Decode(&stats)
	if stats["total_watchers"].(float64) < 1 {
		t.Fatalf("watchers: %+v", stats)
	}

	// Lab partition / write / heal
	req = httptest.NewRequest(http.MethodPost, "/v1/lab/consistency/partition", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("partition: %s", rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/lab/consistency/write-ap?side=a", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	req = httptest.NewRequest(http.MethodPost, "/v1/lab/consistency/write-ap?side=b", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	req = httptest.NewRequest(http.MethodGet, "/v1/lab/consistency", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var lab map[string]any
	_ = json.NewDecoder(rec.Body).Decode(&lab)
	if lab["partitioned"] != true {
		t.Fatalf("lab: %+v", lab)
	}
	req = httptest.NewRequest(http.MethodPost, "/v1/lab/consistency/heal", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}
}

func TestIntentionsAndXDSStatusHTTP(t *testing.T) {
	cs := catalog.NewStore()
	st := store.NewMemory(cs, "ap")
	x := xds.New(st, nil)
	// Seed an ADS stream so status is non-empty.
	_ = x.HandleRequest(&xds.DiscoveryRequest{NodeID: "envoy-1", TypeURL: "ads"})

	srv := httpapi.New(httpapi.Config{Store: st, XDS: x, RPS: 1e6, Burst: 1e6})
	h := srv.Handler()

	// Create intention
	body := `{"source":"web","destination":"api","action":"allow","precedence":100}`
	req := httptest.NewRequest(http.MethodPut, "/v1/connect/intentions", bytes.NewReader([]byte(body)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("intention create: %d %s", rec.Code, rec.Body.String())
	}

	// List
	req = httptest.NewRequest(http.MethodGet, "/v1/connect/intentions", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatal(rec.Body.String())
	}

	// Delete
	req = httptest.NewRequest(http.MethodDelete, "/v1/connect/intentions/web/api", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("delete: %d", rec.Code)
	}

	// xDS status
	req = httptest.NewRequest(http.MethodGet, "/v1/xds/status?node=envoy-1", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("xds status: %d %s", rec.Code, rec.Body.String())
	}
	var stMap map[string]any
	_ = json.NewDecoder(rec.Body).Decode(&stMap)
	if stMap["configured"] != true {
		t.Fatalf("status: %+v", stMap)
	}
}
