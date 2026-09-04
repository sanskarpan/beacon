// Package clock provides an injectable time source used by every timer in beacon.
//
// Rule: no bare time.After / time.NewTicker / time.NewTimer anywhere outside this
// package and its tests. The discrete-event simulator swaps in a virtual clock
// and fast-forwards; real timers make lease/anti-entropy tests unworkable.
package clock

import (
	"sync"
	"time"
)

// Clock is the injectable time abstraction.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) Ticker
	NewTimer(d time.Duration) Timer
	// AfterFunc schedules f to run after d. Real clocks use time.AfterFunc;
	// virtual clocks schedule against the heap.
	AfterFunc(d time.Duration, f func()) Timer
	Sleep(d time.Duration)
}

// Ticker is a resettable periodic timer.
type Ticker interface {
	C() <-chan time.Time
	Stop()
	Reset(d time.Duration)
}

// Timer is a one-shot timer.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// Real is a Clock backed by the wall clock.
type Real struct{}

// New returns a wall-clock Clock.
func New() Clock { return Real{} }

func (Real) Now() time.Time                         { return time.Now() }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (Real) Sleep(d time.Duration)                  { time.Sleep(d) }

func (Real) NewTicker(d time.Duration) Ticker {
	return &realTicker{t: time.NewTicker(d)}
}

func (Real) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

func (Real) AfterFunc(d time.Duration, f func()) Timer {
	return &realTimer{t: time.AfterFunc(d, f)}
}

type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time   { return r.t.C }
func (r *realTicker) Stop()                 { r.t.Stop() }
func (r *realTicker) Reset(d time.Duration) { r.t.Reset(d) }

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// ---------------------------------------------------------------------------
// Virtual clock for the simulator
// ---------------------------------------------------------------------------

// Virtual is a manually advanced clock with a heap of scheduled wakeups.
type Virtual struct {
	mu     sync.Mutex
	now    time.Time
	seq    uint64
	timers []*vEntry
}

type vEntry struct {
	when    time.Time
	seq     uint64
	ch      chan time.Time
	fn      func()
	period  time.Duration // 0 = one-shot
	stopped bool
}

// NewVirtual starts at the given time (usually a fixed epoch).
func NewVirtual(start time.Time) *Virtual {
	return &Virtual{now: start}
}

func (v *Virtual) Now() time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.now
}

func (v *Virtual) After(d time.Duration) <-chan time.Time {
	t := v.NewTimer(d)
	return t.C()
}

func (v *Virtual) Sleep(d time.Duration) {
	<-v.After(d)
}

func (v *Virtual) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		d = time.Nanosecond
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	e := &vEntry{
		when:   v.now.Add(d),
		seq:    v.nextSeq(),
		ch:     make(chan time.Time, 1),
		period: d,
	}
	v.timers = append(v.timers, e)
	return &vTicker{v: v, e: e}
}

func (v *Virtual) NewTimer(d time.Duration) Timer {
	v.mu.Lock()
	defer v.mu.Unlock()
	e := &vEntry{
		when: v.now.Add(d),
		seq:  v.nextSeq(),
		ch:   make(chan time.Time, 1),
	}
	v.timers = append(v.timers, e)
	return &vTimer{v: v, e: e}
}

func (v *Virtual) AfterFunc(d time.Duration, f func()) Timer {
	v.mu.Lock()
	defer v.mu.Unlock()
	e := &vEntry{
		when: v.now.Add(d),
		seq:  v.nextSeq(),
		ch:   make(chan time.Time, 1),
		fn:   f,
	}
	v.timers = append(v.timers, e)
	return &vTimer{v: v, e: e}
}

// Advance moves the virtual clock forward by d and fires due timers.
func (v *Virtual) Advance(d time.Duration) {
	if d < 0 {
		return
	}
	v.mu.Lock()
	target := v.now.Add(d)
	v.mu.Unlock()
	v.AdvanceTo(target)
}

// AdvanceTo moves the clock to t (or later) firing every timer that is due.
func (v *Virtual) AdvanceTo(t time.Time) {
	for {
		v.mu.Lock()
		if !t.After(v.now) && !hasDue(v.timers, v.now) {
			v.now = t
			v.mu.Unlock()
			return
		}
		// Find next due entry at or before t.
		var next *vEntry
		var nextIdx int
		for i, e := range v.timers {
			if e.stopped {
				continue
			}
			if e.when.After(t) {
				continue
			}
			if next == nil || e.when.Before(next.when) || (e.when.Equal(next.when) && e.seq < next.seq) {
				next = e
				nextIdx = i
			}
		}
		if next == nil {
			v.now = t
			v.mu.Unlock()
			return
		}
		v.now = next.when
		fn := next.fn
		ch := next.ch
		period := next.period
		if period > 0 {
			next.when = v.now.Add(period)
		} else {
			// remove one-shot
			v.timers = append(v.timers[:nextIdx], v.timers[nextIdx+1:]...)
			next.stopped = true
		}
		v.mu.Unlock()

		// Deliver outside the lock.
		if fn != nil {
			fn()
		}
		select {
		case ch <- v.Now():
		default:
		}
	}
}

func hasDue(entries []*vEntry, now time.Time) bool {
	for _, e := range entries {
		if !e.stopped && !e.when.After(now) {
			return true
		}
	}
	return false
}

func (v *Virtual) nextSeq() uint64 {
	v.seq++
	return v.seq
}

type vTicker struct {
	v *Virtual
	e *vEntry
}

func (t *vTicker) C() <-chan time.Time { return t.e.ch }
func (t *vTicker) Stop() {
	t.v.mu.Lock()
	defer t.v.mu.Unlock()
	t.e.stopped = true
}
func (t *vTicker) Reset(d time.Duration) {
	t.v.mu.Lock()
	defer t.v.mu.Unlock()
	t.e.stopped = false
	t.e.period = d
	t.e.when = t.v.now.Add(d)
}

type vTimer struct {
	v *Virtual
	e *vEntry
}

func (t *vTimer) C() <-chan time.Time { return t.e.ch }
func (t *vTimer) Stop() bool {
	t.v.mu.Lock()
	defer t.v.mu.Unlock()
	if t.e.stopped {
		return false
	}
	t.e.stopped = true
	return true
}
func (t *vTimer) Reset(d time.Duration) bool {
	t.v.mu.Lock()
	defer t.v.mu.Unlock()
	active := !t.e.stopped
	t.e.stopped = false
	t.e.when = t.v.now.Add(d)
	// re-insert if removed
	found := false
	for _, e := range t.v.timers {
		if e == t.e {
			found = true
			break
		}
	}
	if !found {
		t.v.timers = append(t.v.timers, t.e)
	}
	return active
}
