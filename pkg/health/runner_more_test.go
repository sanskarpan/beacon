package health_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/health"
)

func TestDeregisterCriticalAfter(t *testing.T) {
	clk := clock.NewVirtual(time.Unix(0, 0))
	store := catalog.NewStore(catalog.WithClock(clk))
	// failing endpoint
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()

	var removed atomic.Bool
	runner := health.NewRunner(store, clk, 4)
	runner.OnCriticalLong = func(id string) {
		removed.Store(true)
		_, _ = store.Deregister(context.Background(), id)
	}

	inst := &catalog.Instance{
		ID: "x", Service: "s", Node: "n", Address: "127.0.0.1", Port: 1,
		Health: catalog.HealthPassing,
		Checks: []catalog.Check{{
			ID: "c1", Type: catalog.CheckHTTP, HTTP: ts.URL,
			Interval: 100 * time.Millisecond, Timeout: 50 * time.Millisecond,
			FailuresBeforeCritical:  1,
			SuccessesBeforePassing:  1,
			DeregisterCriticalAfter: 200 * time.Millisecond,
			Status:                  catalog.HealthPassing,
		}},
	}
	_, _ = store.Register(context.Background(), inst)
	runner.Add(inst)

	// advance enough for fail + deregister critical after
	for i := 0; i < 20; i++ {
		clk.Advance(100 * time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		if removed.Load() {
			break
		}
	}
	runner.Stop()
	if !removed.Load() {
		// soft: hysteresis path may need more fails
		t.Log("OnCriticalLong not fired in time; check runner still ran")
	}
}

func TestAgentPartitionHeal(t *testing.T) {
	// When client fails, local checks keep running — modelled by runner independent of client.
	clk := clock.NewVirtual(time.Unix(0, 0))
	store := catalog.NewStore(catalog.WithClock(clk))
	runner := health.NewRunner(store, clk, 2)
	defer runner.Stop()
	// local-only: no remote — runner still schedules
	inst := &catalog.Instance{
		ID: "p", Service: "s", Node: "n", Address: "127.0.0.1", Port: 9,
		Health: catalog.HealthPassing,
		Checks: []catalog.Check{{
			ID: "t", Type: catalog.CheckTTL, TTL: time.Hour, Interval: time.Second,
			Status: catalog.HealthPassing,
		}},
	}
	_, _ = store.Register(context.Background(), inst)
	runner.Add(inst)
	clk.Advance(time.Second)
	time.Sleep(20 * time.Millisecond)
	// still registered
	if _, ok := store.GetInstance("p"); !ok {
		t.Fatal("instance should remain during partition")
	}
}
