package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sanskar/beacon/pkg/api/httpapi"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/gossip"
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

func TestMetricsExposeApplicationSignals(t *testing.T) {
	cs := catalog.NewStore()
	membership := gossip.NewMemory(gossip.NewCluster(nil), "node-1", "127.0.0.1", 7946)
	s := httpapi.New(httpapi.Config{
		Store:      store.NewMemory(cs, "ap"),
		Membership: membership,
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	body := bytes.NewBufferString(`{"id":"m1","service":"api","address":"127.0.0.1","port":9090}`)
	resp, err := http.Post(ts.URL+"/v1/agent/service/register", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	resp, err = http.Get(ts.URL + "/v1/catalog/services")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	resp, err = http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	exposition := string(data)
	for _, metric := range []string{
		`beacon_http_requests_total{method="GET",route="/v1/catalog/services",status="200"}`,
		`beacon_http_request_duration_seconds_count{method="GET",route="/v1/catalog/services"}`,
		`beacon_catalog_services 1`,
		`beacon_catalog_instances 1`,
		`beacon_watch_watchers 0`,
		`beacon_gossip_members{status="alive"} 1`,
	} {
		if !strings.Contains(exposition, metric) {
			t.Errorf("metrics missing %q", metric)
		}
	}
}
