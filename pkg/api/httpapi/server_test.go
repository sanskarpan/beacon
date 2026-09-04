package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sanskar/beacon/pkg/api/httpapi"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/store"
)

func TestHTTPLifecycle(t *testing.T) {
	cs := catalog.NewStore()
	s := httpapi.New(httpapi.Config{Store: store.NewMemory(cs, "ap")})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	body, _ := json.Marshal(map[string]any{
		"id": "h1", "service": "api", "address": "127.0.0.1", "port": 9090,
		"health": "passing", "weight": 1, "node": "n1",
	})
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/v1/agent/service/register", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("register: %v %v", err, resp)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/v1/health/service/api")
	if err != nil {
		t.Fatal(err)
	}
	var insts []catalog.Instance
	_ = json.NewDecoder(resp.Body).Decode(&insts)
	resp.Body.Close()
	if len(insts) != 1 {
		t.Fatalf("want 1 got %d", len(insts))
	}
	// index header is set on the blocking path; empty here is fine for non-blocking reads.
	_ = resp.Header.Get("X-Beacon-Index")

	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/v1/agent/service/deregister/h1", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
}
