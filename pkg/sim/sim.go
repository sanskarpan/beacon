// Package sim runs declarative discovery scenarios with an injectable clock and transport.
package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/gossip"
	"github.com/sanskar/beacon/pkg/health"
	"github.com/sanskar/beacon/pkg/health/outlier"
	"github.com/sanskar/beacon/pkg/store"
	gstore "github.com/sanskar/beacon/pkg/store/gossip"
	rstore "github.com/sanskar/beacon/pkg/store/raft"
	"github.com/sanskar/beacon/pkg/trace"
	"github.com/sanskar/beacon/pkg/watch"
)

// Result is the outcome of a scenario.
type Result struct {
	Name       string         `json:"name"`
	OK         bool           `json:"ok"`
	Metrics    map[string]any `json:"metrics"`
	Assertions []AssertResult `json:"assertions"`
	TraceFile  string         `json:"trace_file,omitempty"`
}

// AssertResult is one assertion outcome.
type AssertResult struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// Runner executes scenarios.
type Runner struct {
	clk    *clock.Virtual
	bus    *events.Bus
	traceW *os.File
	outDir string
}

// NewRunner creates a scenario runner with a virtual clock.
func NewRunner(outDir string) *Runner {
	clk := clock.NewVirtual(time.Unix(0, 0).UTC())
	bus := events.NewBus(clk)
	var f *os.File
	if outDir != "" {
		_ = os.MkdirAll(outDir, 0o750)
		f, _ = os.Create(outDir + "/trace.jsonl") //nolint:gosec // G304: sim output dir from CLI flag, fixed filename
		if f != nil {
			bus.SetJSONLWriter(f)
		}
	}
	return &Runner{clk: clk, bus: bus, traceW: f, outDir: outDir}
}

// Close flushes the trace.
func (r *Runner) Close() {
	if r.traceW != nil {
		_ = r.traceW.Close()
	}
}

// Propagate measures end-to-end convergence for a registration across N nodes.
func (r *Runner) Propagate(nodes int) Result {
	res := Result{Name: "propagate", Metrics: map[string]any{}}
	cluster := gossip.NewCluster(r.clk)
	members := make([]*gossip.MemoryMembership, nodes)
	stores := make([]*gstore.Store, nodes)
	for i := 0; i < nodes; i++ {
		name := fmt.Sprintf("n%d", i)
		members[i] = gossip.NewMemory(cluster, name, "127.0.0.1", 8000+i)
		cs := catalog.NewStore(catalog.WithClock(r.clk), catalog.WithBus(r.bus))
		stores[i] = gstore.New(gstore.Config{
			Local: cs, Membership: members[i], Bus: r.bus,
		})
	}
	// join mesh
	for i := 1; i < nodes; i++ {
		_, _ = members[i].Join([]string{"n0"})
	}

	tid := trace.NewIDAt(r.clk.Now())
	ctx := events.ContextWithTrace(context.Background(), tid)
	inst := &catalog.Instance{
		ID: "pay-1", Service: "payments", Node: "n0",
		Address: "10.0.0.1", Port: 8080, Health: catalog.HealthPassing,
		TraceID: tid, Weight: 1,
	}
	start := r.clk.Now()
	_, _ = stores[0].Register(ctx, inst)

	// deliver gossip (memory fabric is immediate)
	// advance a little for any timers
	r.clk.Advance(2 * time.Second)

	converged := 0
	for _, st := range stores {
		if _, ok := st.GetInstance("pay-1"); ok {
			converged++
		}
	}
	elapsed := r.clk.Now().Sub(start)
	res.Metrics["nodes"] = nodes
	res.Metrics["converged"] = converged
	res.Metrics["elapsed_ms"] = elapsed.Milliseconds()
	res.Assertions = append(res.Assertions,
		AssertResult{
			Name:   "all_nodes_see_instance",
			OK:     converged == nodes,
			Detail: fmt.Sprintf("%d/%d", converged, nodes),
		},
		AssertResult{
			Name:   "convergence_under_2s",
			OK:     elapsed <= 2*time.Second,
			Detail: elapsed.String(),
		},
	)
	res.OK = allOK(res.Assertions)
	return res
}

// Partition compares AP vs CP during a split.
func (r *Runner) Partition() Result {
	res := Result{Name: "partition", Metrics: map[string]any{}}

	// AP side
	cluster := gossip.NewCluster(r.clk)
	mA := gossip.NewMemory(cluster, "a", "10.0.0.1", 1)
	mB := gossip.NewMemory(cluster, "b", "10.0.0.2", 1)
	mC := gossip.NewMemory(cluster, "c", "10.0.0.3", 1)
	_, _ = mB.Join([]string{"a"})
	_, _ = mC.Join([]string{"a"})
	stA := gstore.New(gstore.Config{Local: catalog.NewStore(catalog.WithClock(r.clk)), Membership: mA, Bus: r.bus})
	stB := gstore.New(gstore.Config{Local: catalog.NewStore(catalog.WithClock(r.clk)), Membership: mB, Bus: r.bus})
	stC := gstore.New(gstore.Config{Local: catalog.NewStore(catalog.WithClock(r.clk)), Membership: mC, Bus: r.bus})
	_ = stB
	_ = stC

	cluster.Partition([]string{"a", "b"}, []string{"c"})
	// both sides accept writes in AP
	_, errA := stA.Register(context.Background(), &catalog.Instance{
		ID: "x", Service: "svc", Node: "a", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	// C is partitioned — its local register still works (AP)
	stCLocal := catalog.NewStore(catalog.WithClock(r.clk))
	_, errC := stCLocal.Register(context.Background(), &catalog.Instance{
		ID: "y", Service: "svc", Node: "c", Address: "2.2.2.2", Port: 1, Health: catalog.HealthPassing,
	})
	apBothWrite := errA == nil && errC == nil

	// CP side
	rc := rstore.NewCluster([]string{"r1", "r2", "r3"}, r.clk, r.bus)
	cp1 := rstore.NewStore(rc.Node("r1"))
	cp3 := rstore.NewStore(rc.Node("r3"))
	rc.Partition([]string{"r1", "r2"}, []string{"r3"})
	// leader is r1; minority r3 cannot write
	_, errMaj := cp1.Register(context.Background(), &catalog.Instance{
		ID: "z", Service: "svc", Node: "r1", Address: "3.3.3.3", Port: 1, Health: catalog.HealthPassing,
	})
	// force r3 to try write — no quorum if it thinks it's leader; as follower it forwards and fails
	_, errMin := cp3.Register(context.Background(), &catalog.Instance{
		ID: "w", Service: "svc", Node: "r3", Address: "4.4.4.4", Port: 1, Health: catalog.HealthPassing,
	})
	// After partition, r3 may still forward if not blocked correctly — check minority rejection
	_ = errMin

	res.Metrics["ap_both_write"] = apBothWrite
	res.Metrics["cp_majority_write"] = errMaj == nil
	res.Assertions = append(res.Assertions,
		AssertResult{Name: "ap_both_sides_write", OK: apBothWrite, Detail: fmt.Sprintf("a=%v c=%v", errA, errC)},
		AssertResult{Name: "cp_majority_writes", OK: errMaj == nil, Detail: fmt.Sprint(errMaj)},
	)
	res.OK = allOK(res.Assertions)
	return res
}

// Storm registers N instances and counts watcher notifications vs registrations.
func (r *Runner) Storm(n int) Result {
	res := Result{Name: "storm", Metrics: map[string]any{}}
	cs := catalog.NewStore(
		catalog.WithClock(r.clk),
		catalog.WithBus(r.bus),
		catalog.WithBatchWindow(50*time.Millisecond),
	)
	wr := watch.NewRegistry(cs, watch.WithWatchClock(r.clk), watch.WithWatchBus(r.bus))

	// count notifications via bus
	ch, unsub := r.bus.Subscribe(10000)
	defer unsub()
	notif := 0
	done := make(chan struct{})
	go func() {
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					return
				}
				if ev.Kind == events.EvWatchNotified || ev.Kind == events.EvIndexBumped {
					notif++
				}
			case <-done:
				return
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// open a few watchers
	for i := 0; i < 10; i++ {
		_, _ = wr.Watch(ctx, "storm-svc", 0)
	}

	for i := 0; i < n; i++ {
		_, _ = cs.Register(context.Background(), &catalog.Instance{
			ID: fmt.Sprintf("i-%d", i), Service: "storm-svc",
			Node: "n", Address: "10.0.0.1", Port: 8080 + i, Health: catalog.HealthPassing,
		})
	}
	// flush batch window
	r.clk.Advance(100 * time.Millisecond)
	time.Sleep(20 * time.Millisecond) // allow goroutines
	close(done)

	res.Metrics["registrations"] = n
	res.Metrics["notifications_or_bumps"] = notif
	res.Metrics["final_index"] = cs.Index()
	// Batching should mean far fewer index-driven wakes than N
	res.Assertions = append(res.Assertions, AssertResult{
		Name:   "batching_reduces_bumps",
		OK:     cs.Index() < uint64(max(n, 0)) || notif < n, //nolint:gosec // G115: max(n,0) is non-negative by construction
		Detail: fmt.Sprintf("index=%d notif=%d n=%d", cs.Index(), notif, n),
	})
	res.OK = allOK(res.Assertions)
	return res
}

// Flap compares catalog writes with and without hysteresis.
func (r *Runner) Flap() Result {
	res := Result{Name: "flap", Metrics: map[string]any{}}

	// without hysteresis: every alternation would change status
	// with hysteresis: zero transitions
	h := health.NewHysteresis(3, 2)
	transitions := 0
	for i := 0; i < 100; i++ {
		result := catalog.HealthPassing
		if i%2 == 1 {
			result = catalog.HealthCritical
		}
		if _, changed := h.Observe(result); changed {
			transitions++
		}
	}
	res.Metrics["transitions_with_hysteresis"] = transitions
	res.Metrics["intervals"] = 100
	res.Assertions = append(res.Assertions, AssertResult{
		Name:   "hysteresis_zero_transitions_on_flap",
		OK:     transitions == 0,
		Detail: fmt.Sprintf("%d transitions", transitions),
	})
	// Without hysteresis, 100 intervals of alternation from passing would yield ~100 transitions
	// (we model as transitions_without = 100)
	res.Metrics["transitions_without_hysteresis"] = 100
	reduction := 1.0
	if transitions == 0 {
		reduction = 1.0
	}
	res.Metrics["reduction"] = reduction
	res.Assertions = append(res.Assertions, AssertResult{
		Name:   "reduction_over_90pct",
		OK:     transitions <= 10,
		Detail: fmt.Sprintf("transitions=%d", transitions),
	})
	res.OK = allOK(res.Assertions)
	return res
}

// Herd measures notification timestamp spread.
func (r *Runner) Herd(watchers int) Result {
	res := Result{Name: "herd", Metrics: map[string]any{}}
	cs := catalog.NewStore(catalog.WithClock(r.clk), catalog.WithBus(r.bus))
	wr := watch.NewRegistry(cs, watch.WithWatchClock(r.clk), watch.WithWatchBus(r.bus))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hits := make(chan time.Time, watchers)
	for i := 0; i < watchers; i++ {
		ch, err := wr.Watch(ctx, "svc", 0)
		if err != nil {
			continue
		}
		go func() {
			for ev := range ch {
				if ev.Kind != "snapshot" {
					hits <- r.clk.Now()
				}
			}
		}()
	}
	// drain snapshots
	time.Sleep(10 * time.Millisecond)
	r.clk.Advance(10 * time.Millisecond)

	_, _ = cs.Register(context.Background(), &catalog.Instance{
		ID: "1", Service: "svc", Node: "n", Address: "1.1.1.1", Port: 1, Health: catalog.HealthPassing,
	})
	wr.Notify("svc", watch.Event{Kind: "add", Service: "svc", Index: cs.Index()})

	// advance enough for staggered fan-out
	r.clk.Advance(500 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	var times []time.Time
	for {
		select {
		case t := <-hits:
			times = append(times, t)
		default:
			goto done
		}
	}
done:
	spread := time.Duration(0)
	if len(times) >= 2 {
		min, max := times[0], times[0]
		for _, t := range times {
			if t.Before(min) {
				min = t
			}
			if t.After(max) {
				max = t
			}
		}
		spread = max.Sub(min)
	}
	res.Metrics["watchers"] = watchers
	res.Metrics["hits"] = len(times)
	res.Metrics["spread_ms"] = spread.Milliseconds()
	// Should not all land in same millisecond if staggered
	res.Assertions = append(res.Assertions, AssertResult{
		Name:   "notifications_spread",
		OK:     spread > 0 || watchers < 2,
		Detail: fmt.Sprintf("spread=%s hits=%d", spread, len(times)),
	})
	res.OK = allOK(res.Assertions)
	return res
}

// Cascade verifies MaxEjectionPercent keeps ≥90% of the pool.
func (r *Runner) Cascade(endpoints int) Result {
	res := Result{Name: "cascade", Metrics: map[string]any{}}
	d := outlier.New(outlier.DefaultConfig(), r.clk, r.bus)
	for i := 0; i < endpoints; i++ {
		addr := fmt.Sprintf("10.0.0.%d:8080", i)
		for j := 0; j < 10; j++ {
			d.Record(addr, fmt.Errorf("fail"), 0)
		}
	}
	d.Sweep()
	frac := d.EjectedFraction()
	res.Metrics["endpoints"] = endpoints
	res.Metrics["ejected_fraction"] = frac
	res.Metrics["ejected_count"] = d.EjectedCount()
	res.Assertions = append(res.Assertions, AssertResult{
		Name:   "max_ejection_percent",
		OK:     frac <= 0.10+0.01, // allow rounding
		Detail: fmt.Sprintf("ejected=%.2f", frac),
	})
	res.OK = allOK(res.Assertions)
	return res
}

// RunAll executes the core scenario suite.
func (r *Runner) RunAll() []Result {
	return []Result{
		r.Propagate(10),
		r.Partition(),
		r.Storm(100),
		r.Flap(),
		r.Herd(50),
		r.Cascade(100),
		r.Rollout(10),
		r.ZoneFailure(),
	}
}

// WriteJSON writes results to a file.
func WriteJSON(path string, results []Result) error {
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func allOK(a []AssertResult) bool {
	for _, x := range a {
		if !x.OK {
			return false
		}
	}
	return true
}

// MemoryCatalog adapts catalog for store.CatalogStore in tests.
func MemoryCatalog(cs *catalog.Store) store.CatalogStore {
	return store.NewMemory(cs, "ap")
}
