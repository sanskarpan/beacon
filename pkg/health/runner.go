package health

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/catalog"
	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
	"github.com/sanskar/beacon/pkg/health/check"
)

// Runner schedules agent-local health checks with jitter and bounded concurrency.
//
// Health checks run on the AGENT that owns the instance, over loopback — never
// centrally. 10k instances × 1 check/5s = 2k checks/s from a central control
// plane; agent-local is ~10 checks per agent over loopback.
type Runner struct {
	mu          sync.Mutex
	clk         clock.Clock
	bus         *events.Bus
	store       *catalog.Store
	checks      map[string]*managedCheck // key: instanceID/checkID
	concurrency int
	sem         chan struct{}
	rng         *rand.Rand
	stop        chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	// criticalSince tracks how long an instance has been critical for DeregisterCriticalAfter
	criticalSince map[string]time.Time
	// onCriticalLong callback for deregistration
	OnCriticalLong func(instanceID string)
	// OnStatusChange is called after hysteresis changes the aggregate status.
	// The callback runs in the check goroutine and should be non-blocking.
	OnStatusChange func(ctx context.Context, instanceID string, status catalog.HealthStatus, output string)
}

type managedCheck struct {
	instanceID string
	check      catalog.Check
	checker    check.Checker
	hyst       *Hysteresis
	ttl        *check.TTLCheck // if type TTL
	cancel     context.CancelFunc
}

// RunnerOption configures the runner.
type RunnerOption func(*Runner)

// WithRunnerBus attaches events.
func WithRunnerBus(b *events.Bus) RunnerOption {
	return func(r *Runner) { r.bus = b }
}

// NewRunner creates a check runner.
func NewRunner(store *catalog.Store, clk clock.Clock, concurrency int, opts ...RunnerOption) *Runner {
	if clk == nil {
		clk = clock.New()
	}
	if concurrency <= 0 {
		concurrency = 32
	}
	seed := clk.Now().UnixNano()
	if seed == 0 {
		seed = 1
	}
	r := &Runner{
		clk:           clk,
		store:         store,
		checks:        make(map[string]*managedCheck),
		concurrency:   concurrency,
		sem:           make(chan struct{}, concurrency),
		rng:           rand.New(rand.NewSource(seed)),
		stop:          make(chan struct{}),
		criticalSince: make(map[string]time.Time),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Add registers checks for an instance and starts them.
func (r *Runner) Add(inst *catalog.Instance) {
	for i := range inst.Checks {
		c := inst.Checks[i]
		c.Defaults()
		key := string(inst.ID) + "/" + string(c.ID)
		r.mu.Lock()
		if _, exists := r.checks[key]; exists {
			r.mu.Unlock()
			continue
		}
		mc := &managedCheck{
			instanceID: inst.ID,
			check:      c,
			hyst:       NewHysteresis(c.FailuresBeforeCritical, c.SuccessesBeforePassing),
			checker:    r.buildChecker(c, inst),
		}
		if c.Type == catalog.CheckTTL {
			if ttl, ok := mc.checker.(*check.TTLCheck); ok {
				mc.ttl = ttl
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		mc.cancel = cancel
		r.checks[key] = mc
		r.mu.Unlock()

		r.wg.Add(1)
		go r.loop(ctx, mc)
	}
}

// Remove stops checks for an instance.
func (r *Runner) Remove(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, mc := range r.checks {
		if mc.instanceID == instanceID {
			mc.cancel()
			delete(r.checks, key)
		}
	}
	delete(r.criticalSince, instanceID)
}

// TTLPass records a TTL push.
func (r *Runner) TTLPass(instanceID string, checkID catalog.CheckID, status catalog.HealthStatus, output string) error {
	key := instanceID + "/" + string(checkID)
	r.mu.Lock()
	mc, ok := r.checks[key]
	r.mu.Unlock()
	if !ok || mc.ttl == nil {
		return fmt.Errorf("ttl check not found: %s", key)
	}
	mc.ttl.Set(status, output)
	return nil
}

// Stop halts all checks. Safe to call more than once.
func (r *Runner) Stop() {
	r.stopOnce.Do(func() {
		close(r.stop)
	})
	r.mu.Lock()
	for _, mc := range r.checks {
		mc.cancel()
	}
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *Runner) loop(ctx context.Context, mc *managedCheck) {
	defer r.wg.Done()
	interval := mc.check.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	// Initial jitter so 500 checks don't fire on the same tick.
	r.mu.Lock()
	jitter := time.Duration(r.rng.Int63n(int64(interval)))
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return
	case <-r.stop:
		return
	case <-r.clk.After(jitter):
	}

	ticker := r.clk.NewTicker(interval)
	defer ticker.Stop()

	// run immediately after jitter
	r.runOne(ctx, mc)

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stop:
			return
		case <-ticker.C():
			r.runOne(ctx, mc)
		}
	}
}

func (r *Runner) runOne(ctx context.Context, mc *managedCheck) {
	// bounded concurrency
	select {
	case r.sem <- struct{}{}:
		defer func() { <-r.sem }()
	case <-ctx.Done():
		return
	}

	start := r.clk.Now()
	status, output, err := mc.checker.Run(ctx)
	dur := r.clk.Now().Sub(start)
	if err != nil {
		status = catalog.HealthCritical
		output = err.Error()
	}

	if r.bus != nil {
		r.bus.Publish(events.Event{
			Kind:     events.EvCheckExecuted,
			Instance: mc.instanceID,
			Detail:   fmt.Sprintf("%s %s %s", mc.check.ID, status, dur),
		})
	}

	newStatus, changed := mc.hyst.Observe(status)
	if !changed {
		return
	}

	// Flapping: many transitions
	if mc.hyst.Transitions() > 10 {
		if r.bus != nil {
			r.bus.Publish(events.Event{
				Kind:     events.EvFlappingDetected,
				Instance: mc.instanceID,
				Detail:   fmt.Sprintf("%d transitions", mc.hyst.Transitions()),
			})
		}
	}

	_, _ = r.store.UpdateCheckStatus(ctx, mc.instanceID, mc.check.ID, newStatus, output)
	if r.OnStatusChange != nil {
		r.OnStatusChange(ctx, mc.instanceID, newStatus, output)
	}

	// DeregisterCriticalAfter tracking — timer-based, not runOne cadence
	if newStatus == catalog.HealthCritical {
		r.mu.Lock()
		_, existed := r.criticalSince[mc.instanceID]
		if !existed {
			r.criticalSince[mc.instanceID] = r.clk.Now()
		}
		since := r.criticalSince[mc.instanceID]
		after := mc.check.DeregisterCriticalAfter
		r.mu.Unlock()
		if after > 0 {
			if r.clk.Now().After(since.Add(after)) {
				if r.OnCriticalLong != nil {
					r.OnCriticalLong(mc.instanceID)
				}
			} else if !existed {
				// schedule exact deregistration; fires even if next runOne is up to Interval away
				go func(id string, start time.Time, d time.Duration) {
					select {
					case <-r.clk.After(d):
						r.mu.Lock()
						s, ok := r.criticalSince[id]
						r.mu.Unlock()
						if ok && s.Equal(start) && r.OnCriticalLong != nil {
							r.OnCriticalLong(id)
						}
					case <-ctx.Done():
					case <-r.stop:
					}
				}(mc.instanceID, since, after)
			}
		}
	} else {
		r.mu.Lock()
		delete(r.criticalSince, mc.instanceID)
		r.mu.Unlock()
	}
}

func (r *Runner) buildChecker(c catalog.Check, inst *catalog.Instance) check.Checker {
	switch c.Type {
	case catalog.CheckHTTP:
		return &check.HTTPCheck{URL: c.HTTP, Timeout: c.Timeout}
	case catalog.CheckTCP:
		addr := c.TCP
		if addr == "" {
			addr = inst.Addr()
		}
		return &check.TCPCheck{Addr: addr, Timeout: c.Timeout}
	case catalog.CheckGRPC:
		target := c.GRPC
		if target == "" {
			target = inst.Addr()
		}
		return &check.GRPCCheck{Target: target, ServiceName: c.GRPCServiceName, Timeout: c.Timeout}
	case catalog.CheckExec:
		return &check.ExecCheck{Script: c.Exec, Args: c.Args, Timeout: c.Timeout}
	case catalog.CheckTTL:
		return check.NewTTL(r.clk, c.TTL)
	case catalog.CheckAlias:
		return &check.AliasCheck{
			Service: c.AliasService,
			Lookup: func(service string) catalog.HealthStatus {
				res := r.store.GetNow(service, catalog.QueryOptions{})
				if len(res.Instances) == 0 {
					return catalog.HealthCritical
				}
				statuses := make([]catalog.HealthStatus, len(res.Instances))
				for i, in := range res.Instances {
					statuses[i] = in.Health
				}
				return catalog.Aggregate(statuses)
			},
		}
	default:
		return &check.TCPCheck{Addr: inst.Addr(), Timeout: c.Timeout}
	}
}
