package check_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/health/check"
)

func TestHTTPCheck(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(200)
		case "/rate":
			w.WriteHeader(429)
		default:
			w.WriteHeader(500)
		}
	}))
	defer ts.Close()

	st, _, _ := (&check.HTTPCheck{URL: ts.URL + "/ok"}).Run(context.Background())
	if st != catalog.HealthPassing {
		t.Fatal(st)
	}
	st, _, _ = (&check.HTTPCheck{URL: ts.URL + "/rate"}).Run(context.Background())
	if st != catalog.HealthWarning {
		t.Fatalf("429 should be warning, got %s", st)
	}
	st, _, _ = (&check.HTTPCheck{URL: ts.URL + "/bad"}).Run(context.Background())
	if st != catalog.HealthCritical {
		t.Fatal(st)
	}
}
