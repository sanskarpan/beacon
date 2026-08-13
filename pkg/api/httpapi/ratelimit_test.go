package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sanskar/beacon/pkg/api/httpapi"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/store"
)

func TestRateLimit429(t *testing.T) {
	cs := catalog.NewStore()
	s := httpapi.New(httpapi.Config{
		Store: store.NewMemory(cs, "ap"),
		RPS:   1,
		Burst: 1,
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// Exhaust burst + rate: fire many requests quickly from same client.
	var saw429 bool
	var retryAfter string
	for i := 0; i < 50; i++ {
		resp, err := http.Get(ts.URL + "/v1/catalog/services")
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			saw429 = true
			retryAfter = resp.Header.Get("Retry-After")
			resp.Body.Close()
			break
		}
		resp.Body.Close()
	}
	if !saw429 {
		t.Fatal("want at least one 429 under tight rate limit")
	}
	if retryAfter == "" {
		t.Fatal("missing Retry-After")
	}
}
