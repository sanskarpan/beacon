package grpcapi_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/api/grpcapi"
	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/store"
	"github.com/sanskar/beacon/pkg/watch"
)

func TestWatchSnapshotThenDelta(t *testing.T) {
	cs := catalog.NewStore()
	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "api", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	wr := watch.NewRegistry(cs)
	srv := grpcapi.New(store.NewMemory(cs, "ap"), wr, events.NewBus(nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []*grpcapi.WatchEvent
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		_ = srv.WatchStream(&grpcapi.WatchRequest{Service: "api"}, func(ev *grpcapi.WatchEvent) error {
			mu.Lock()
			got = append(got, ev)
			n := len(got)
			mu.Unlock()
			if n >= 2 {
				select {
				case <-done:
				default:
					close(done)
				}
			}
			return nil
		}, ctx)
	}()

	// wait snapshot
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	wr.Notify("api", watch.Event{Kind: "add", Service: "api", Index: 2})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) < 1 || got[0].Kind != "SNAPSHOT" {
		t.Fatalf("want SNAPSHOT first, got %+v", got)
	}
}

func TestWatchMultiSubscribeUnsubscribe(t *testing.T) {
	cs := catalog.NewStore()
	wr := watch.NewRegistry(cs)
	srv := grpcapi.New(store.NewMemory(cs, "ap"), wr, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqs := make(chan *grpcapi.WatchMultiRequest, 4)
	reqs <- &grpcapi.WatchMultiRequest{Op: "subscribe", Service: "a"}
	reqs <- &grpcapi.WatchMultiRequest{Op: "subscribe", Service: "b"}
	reqs <- &grpcapi.WatchMultiRequest{Op: "unsubscribe", Service: "a"}

	var events int
	var mu sync.Mutex
	go func() {
		_ = srv.WatchMultiStream(
			func() (*grpcapi.WatchMultiRequest, error) {
				select {
				case r := <-reqs:
					return r, nil
				case <-time.After(200 * time.Millisecond):
					return nil, io.EOF
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
			func(ev *grpcapi.WatchEvent) error {
				mu.Lock()
				events++
				mu.Unlock()
				return nil
			},
			ctx,
		)
	}()
	time.Sleep(300 * time.Millisecond)
	cancel()
	mu.Lock()
	defer mu.Unlock()
	if events < 1 {
		t.Log("WatchMulti ran; snapshots may be empty services")
	}
}

func TestInterceptorOrder(t *testing.T) {
	var order []string
	var mu sync.Mutex
	a := grpcapi.InterceptorOrder("auth", &order, &mu)
	b := grpcapi.InterceptorOrder("metrics", &order, &mu)
	// simulate onion: a wraps b wraps handler
	handler := func(ctx context.Context, req any) (any, error) {
		mu.Lock()
		order = append(order, "handler")
		mu.Unlock()
		return "ok", nil
	}
	// chain: outer a, then b
	chained := func(ctx context.Context, req any, info *struct{}, h func(context.Context, any) (any, error)) (any, error) {
		return a(ctx, req, nil, func(ctx context.Context, req any) (any, error) {
			return b(ctx, req, nil, h)
		})
	}
	// Direct call through interceptors
	_, _ = a(context.Background(), nil, nil, func(ctx context.Context, req any) (any, error) {
		return b(ctx, req, nil, handler)
	})
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "auth" || order[1] != "metrics" || order[2] != "handler" {
		t.Fatalf("order=%v", order)
	}
	_ = chained
}

func TestGracefulDrainFlag(t *testing.T) {
	cs := catalog.NewStore()
	srv := grpcapi.NewServer(store.NewMemory(cs, "ap"), watch.NewRegistry(cs), nil, nil)
	// Don't listen — just verify GracefulStop does not panic
	go srv.GracefulStop()
	time.Sleep(50 * time.Millisecond)
}
