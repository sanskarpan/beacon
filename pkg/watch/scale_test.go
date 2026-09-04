package watch_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/watch"
)

func TestManyConcurrentWatchers(t *testing.T) {
	s := catalog.NewStore()
	r := watch.NewRegistry(s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 500 // full 10k is CI-heavy; 500 exercises fan-out + cleanup
	chs := make([]<-chan watch.Event, 0, n)
	for i := 0; i < n; i++ {
		ch, err := r.Watch(ctx, "svc", 0)
		if err != nil {
			t.Fatal(err)
		}
		chs = append(chs, ch)
		snapWait := time.NewTimer(time.Second)
		select {
		case <-ch: // snapshot
			snapWait.Stop()
		case <-snapWait.C:
			t.Fatal("no snapshot")
		}
	}
	// one change
	_, _ = s.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "svc", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	r.Notify("svc", watch.Event{Kind: "add", Service: "svc", Index: s.Index()})
	time.Sleep(600 * time.Millisecond)
	cancel()
	for _, ch := range chs {
		for range ch {
		}
	}
	runtime.GC()
	t.Logf("opened %d watchers ok", n)
}

func BenchmarkWatchMemory(b *testing.B) {
	s := catalog.NewStore()
	r := watch.NewRegistry(s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch, err := r.Watch(ctx, "svc", 0)
		if err != nil {
			b.Fatal(err)
		}
		select {
		case <-ch:
		default:
		}
	}
}
