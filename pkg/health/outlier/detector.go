// Package outlier implements passive health checking (outlier detection).
//
// MaxEjectionPercent (default 10) is the most important field: if a shared
// dependency is down, every endpoint errors. Without a cap, ejecting all of
// them turns a degradation into a total outage.
package outlier

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/sanskar/beacon/pkg/clock"
	"github.com/sanskar/beacon/pkg/events"
)

// Config holds outlier detection parameters.
type Config struct {
	ConsecutiveErrors       int
	Interval                time.Duration
	BaseEjectionTime        time.Duration
	MaxEjectionPercent      int // default 10 — NEVER eject more than this
	SuccessRateMinimumHosts int
	SuccessRateStdevFactor  float64
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		ConsecutiveErrors:       5,
		Interval:                10 * time.Second,
		BaseEjectionTime:        30 * time.Second,
		MaxEjectionPercent:      10,
		SuccessRateMinimumHosts: 5,
		SuccessRateStdevFactor:  1.9,
	}
}

// Detector tracks per-endpoint outcomes and ejects outliers.
type Detector struct {
	mu           sync.Mutex
	cfg          Config
	clk          clock.Clock
	bus          *events.Bus
	endpoints    map[string]*endpoint
	ejectedCount int
}

type endpoint struct {
	addr           string
	consecutiveErr int
	successes      int
	failures       int
	ejected        bool
	ejectionCount  int
	ejectedUntil   time.Time
	errorRate      float64
	// probation: after re-insert, require successes before full trust
	probation      bool
	probationOK    int
}

// New creates a detector.
func New(cfg Config, clk clock.Clock, bus *events.Bus) *Detector {
	if clk == nil {
		clk = clock.New()
	}
	if cfg.MaxEjectionPercent <= 0 {
		cfg.MaxEjectionPercent = 10
	}
	if cfg.ConsecutiveErrors <= 0 {
		cfg.ConsecutiveErrors = 5
	}
	if cfg.BaseEjectionTime <= 0 {
		cfg.BaseEjectionTime = 30 * time.Second
	}
	return &Detector{
		cfg:       cfg,
		clk:       clk,
		bus:       bus,
		endpoints: make(map[string]*endpoint),
	}
}

// Record feeds an RPC outcome from the client SDK Done callback / interceptor.
func (d *Detector) Record(addr string, err error, latency time.Duration) {
	_ = latency
	d.mu.Lock()
	defer d.mu.Unlock()
	ep := d.ensure(addr)
	if err != nil {
		ep.consecutiveErr++
		ep.failures++
		if ep.probation {
			// fail during probation → re-eject immediately
			ep.probation = false
			ep.probationOK = 0
			if !ep.ejected {
				ep.ejected = true
				ep.ejectionCount++
				ep.ejectedUntil = d.clk.Now().Add(time.Duration(ep.ejectionCount) * d.cfg.BaseEjectionTime)
				d.ejectedCount++
			}
		}
	} else {
		ep.consecutiveErr = 0
		ep.successes++
		if ep.probation {
			ep.probationOK++
			if ep.probationOK >= 2 {
				ep.probation = false
			}
		}
	}
	total := ep.successes + ep.failures
	if total > 0 {
		ep.errorRate = float64(ep.failures) / float64(total)
	}
}

// IsEjected reports whether addr is currently out of the pool.
func (d *Detector) IsEjected(addr string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	ep, ok := d.endpoints[addr]
	if !ok {
		return false
	}
	if ep.ejected && d.clk.Now().After(ep.ejectedUntil) {
		// auto re-insert
		ep.ejected = false
		d.ejectedCount--
		if d.bus != nil {
			d.bus.Publish(events.Event{
				Kind:   events.EvOutlierReturned,
				Detail: addr,
			})
		}
		return false
	}
	return ep.ejected
}

// Sweep evaluates ejection candidates. Call on Interval.
func (d *Detector) Sweep() {
	d.mu.Lock()
	defer d.mu.Unlock()

	// re-admit expired ejections first (into probation)
	now := d.clk.Now()
	for _, ep := range d.endpoints {
		if ep.ejected && now.After(ep.ejectedUntil) {
			ep.ejected = false
			ep.probation = true
			ep.probationOK = 0
			d.ejectedCount--
			if d.bus != nil {
				d.bus.Publish(events.Event{Kind: events.EvOutlierReturned, Detail: ep.addr + " (probation)"})
			}
		}
	}

	total := len(d.endpoints)
	if total == 0 {
		return
	}
	maxEject := total * d.cfg.MaxEjectionPercent / 100
	if maxEject < 1 && total > 0 {
		maxEject = 1
	}

	// Success-rate statistical outliers vs fleet mean (requires min hosts).
	statCandidates := d.successRateOutliersLocked()

	type cand struct {
		ep *endpoint
	}
	var candidates []cand
	seen := map[string]bool{}
	for _, ep := range d.endpoints {
		if ep.ejected {
			continue
		}
		if ep.consecutiveErr >= d.cfg.ConsecutiveErrors {
			candidates = append(candidates, cand{ep: ep})
			seen[ep.addr] = true
		}
	}
	for _, ep := range statCandidates {
		if ep.ejected || seen[ep.addr] {
			continue
		}
		candidates = append(candidates, cand{ep: ep})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ep.errorRate > candidates[j].ep.errorRate
	})

	for _, c := range candidates {
		if d.ejectedCount >= maxEject {
			if d.bus != nil {
				d.bus.Publish(events.Event{
					Kind: events.EvEjectionCapReached,
					Detail: fmt.Sprintf(
						"%d/%d endpoints failing but ejection capped at %d (%d%%) — "+
							"widespread failure suggests a shared dependency, not bad endpoints",
						len(candidates), total, maxEject, d.cfg.MaxEjectionPercent),
				})
			}
			break
		}
		// Ejection duration grows with repeat offences.
		dur := time.Duration(c.ep.ejectionCount+1) * d.cfg.BaseEjectionTime
		c.ep.ejected = true
		c.ep.ejectionCount++
		c.ep.ejectedUntil = now.Add(dur)
		c.ep.probation = false
		d.ejectedCount++
		if d.bus != nil {
			d.bus.Publish(events.Event{
				Kind:   events.EvOutlierEjected,
				Detail: fmt.Sprintf("%s for %s (count=%d)", c.ep.addr, dur, c.ep.ejectionCount),
			})
		}
	}
}

// successRateOutliersLocked returns endpoints whose success rate is more than
// StdevFactor standard deviations below the fleet mean. Requires
// SuccessRateMinimumHosts active (non-ejected) endpoints.
func (d *Detector) successRateOutliersLocked() []*endpoint {
	minHosts := d.cfg.SuccessRateMinimumHosts
	if minHosts <= 0 {
		minHosts = 5
	}
	factor := d.cfg.SuccessRateStdevFactor
	if factor <= 0 {
		factor = 1.9
	}
	var active []*endpoint
	var rates []float64
	for _, ep := range d.endpoints {
		if ep.ejected {
			continue
		}
		total := ep.successes + ep.failures
		if total < 10 {
			continue // not enough samples
		}
		rate := float64(ep.successes) / float64(total)
		active = append(active, ep)
		rates = append(rates, rate)
	}
	if len(active) < minHosts {
		return nil
	}
	mean := 0.0
	for _, r := range rates {
		mean += r
	}
	mean /= float64(len(rates))
	varSum := 0.0
	for _, r := range rates {
		dlt := r - mean
		varSum += dlt * dlt
	}
	stdev := math.Sqrt(varSum / float64(len(rates)))
	if stdev < 1e-9 {
		return nil
	}
	threshold := mean - factor*stdev
	var out []*endpoint
	for i, ep := range active {
		if rates[i] < threshold {
			out = append(out, ep)
		}
	}
	return out
}

// EjectedFraction for tests.
func (d *Detector) EjectedFraction() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.endpoints) == 0 {
		return 0
	}
	return float64(d.ejectedCount) / float64(len(d.endpoints))
}

// ActiveCount is total tracked endpoints.
func (d *Detector) ActiveCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.endpoints)
}

// EjectedCount returns how many are currently ejected.
func (d *Detector) EjectedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ejectedCount
}

func (d *Detector) ensure(addr string) *endpoint {
	ep, ok := d.endpoints[addr]
	if !ok {
		ep = &endpoint{addr: addr}
		d.endpoints[addr] = ep
	}
	return ep
}

// Start launches the periodic sweep loop.
func (d *Detector) Start(stop <-chan struct{}) {
	interval := d.cfg.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	t := d.clk.NewTicker(interval)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C():
				d.Sweep()
			}
		}
	}()
}
