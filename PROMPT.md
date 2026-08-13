# CLAUDE CODE PROMPT — `beacon`: A Service Discovery System

## Project Mission

Build a Consul-class service discovery system that **integrates two projects you have already built**:

- **Backend: Go** — catalog with monotonic indexing, TTL leases, agent-local health checking with hysteresis, gossip-driven propagation, watch/notify (blocking queries + gRPC streaming), DNS/HTTP/gRPC interfaces, an xDS control plane, and a client SDK with resolver + balancer
- **Two consistency backends side by side**: AP (gossip) and CP (Raft), so the trade is measured rather than argued
- **Frontend: React + TypeScript + Vite + Tailwind + shadcn/ui + D3 + Recharts** — a mesh topology and, more importantly, a **propagation observatory**

**Read `discovery-SPEC.md` and `discovery-CHECKLIST.md` before writing any code.**

### Four rules that override everything

1. **Do not reimplement the gossip protocol or the gRPC interceptors.** Both exist. Define a narrow interface to each and vendor them. The gossip project becomes the membership and propagation layer; the interceptors project becomes the client SDK's reporting path. Rewriting either is the wrong use of this project's time.

2. **Every timer goes through an injectable `Clock`.** No bare `time.After`, no bare `time.NewTicker`, anywhere. Health check intervals, lease expiry, anti-entropy, watch timeouts, backoff — all of it. Miss one and the simulator cannot control it, and Phase 15 becomes untestable.

3. **Thread a `TraceID` from the first line of code.** Register → agent → catalog → gossip → watch → client. Phase 16's propagation measurement is the headline deliverable, and retrofitting tracing into a finished system is miserable. Do it in Phase 0.

4. **Health checks run on the agent, over loopback. Never centrally.** This is the single decision that separates a system that scales from one that doesn't, and it is also a *semantic* choice: agent-local checks answer "can this instance serve", central checks answer "can the control plane reach it", and those have different answers surprisingly often.

---

## Phase 0 — Integration Seams

Define the boundaries with the existing projects first, so nothing accidentally couples to their internals.

```go
// pkg/gossip/iface.go
//
// The contract with your existing SWIM project. beacon depends on THIS, not on
// that project's internals, so it can be swapped for a test double or a
// different membership implementation.
type Membership interface {
    Members() []Member
    Join(seeds []string) (int, error)
    Leave() error

    // Node-level liveness. beacon subscribes to this and uses it to remove
    // every service instance on a dead node IMMEDIATELY, rather than waiting
    // for each instance's health check to independently time out.
    Subscribe(ch chan<- MemberEvent)

    // Piggyback catalog deltas on the EXISTING gossip stream. Do not start a
    // second gossip protocol alongside the first.
    Broadcast(payload []byte) error
    OnBroadcast(fn func(from NodeID, payload []byte))
}

type MemberEvent struct {
    Type MemberEventType // Join | Leave | Failed | Update
    Node Member
}
```

```go
// pkg/sdk/iface.go
//
// The contract with your gRPC-interceptors project. Its interceptor chain is
// reused verbatim; beacon adds one interceptor that reports per-endpoint RPC
// outcomes into passive health checking.
type InterceptorChain interface {
    Unary() grpc.UnaryClientInterceptor
    Stream() grpc.StreamClientInterceptor
}

// The new interceptor beacon contributes. This is the seam that makes outlier
// detection work without the application knowing anything about it.
func OutcomeReporter(od *outlier.Detector) grpc.UnaryClientInterceptor {
    return func(ctx context.Context, method string, req, reply any,
        cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
        start := time.Now()
        err := invoker(ctx, method, req, reply, cc, opts...)
        od.Record(cc.Target(), err, time.Since(start))
        return err
    }
}
```

```go
// pkg/clock/clock.go
//
// RULE 2. Every timer in beacon goes through this. The simulator swaps in a
// virtual clock and fast-forwards; with real timers, testing a 30-second lease
// expiry takes 30 seconds and testing a 30-minute anti-entropy interval is
// simply not done.
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
    NewTicker(d time.Duration) Ticker
    NewTimer(d time.Duration) Timer
    Sleep(d time.Duration)
}
```

---

## Phase 1 — The Catalog

### The index rule that prevents a self-inflicted DDoS

```go
// pkg/catalog/store.go

// UpdateHealth bumps the index ONLY IF THE STATUS ACTUALLY CHANGED.
//
// This looks like a micro-optimization. It is not.
//
// A check running every 5 seconds on 10,000 instances is 2,000 health updates
// per second. If each one bumps the catalog index, then every watcher of every
// service wakes up 2,000 times a second — for changes that contain no new
// information, because the status was "passing" before and is "passing" now.
//
// With 500 watchers that is a million wakeups per second, produced entirely by
// a healthy, idle cluster. Systems have shipped with this bug.
func (s *Store) UpdateHealth(id string, status HealthStatus) (uint64, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    inst, ok := s.instances[id]
    if !ok {
        return s.index, ErrNotFound
    }
    if inst.Health == status {
        return s.index, nil   // ← no change, no bump, no wakeups
    }

    prev := inst.Health
    inst.Health = status
    idx := s.bump()
    inst.ModifyIndex = idx
    s.services[inst.Service].ModifyIndex = idx

    s.events.Emit(Event{
        Kind: EvHealthChanged, TraceID: traceFrom(ctx),
        Instance: inst.ID, From: prev, To: status, Index: idx,
    })
    return idx, nil
}
```

### Batched index bumps — the registration-storm defence

```go
// pkg/catalog/batcher.go

// A 1,000-instance deploy registers 1,000 times in a few seconds. Naively,
// that's 1,000 index bumps × 500 watchers = 500,000 notifications, each one
// causing a client to re-resolve and rebuild its connection pool.
//
// Coalescing mutations within a 50 ms window turns that into ~20 bumps.
// Correctness is preserved because the index is still monotonic and every
// watcher still converges to the final state — they just don't observe every
// intermediate step, which they never needed to.
type IndexBatcher struct {
    clock   clock.Clock
    window  time.Duration   // default 50ms
    mu      sync.Mutex
    pending map[string]struct{}   // services with unflushed changes
    timer   clock.Timer
    flush   func(services []string, index uint64)
}

func (b *IndexBatcher) Touch(service string) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.pending[service] = struct{}{}
    if b.timer == nil {
        b.timer = b.clock.NewTimer(b.window)
        go func() {
            <-b.timer.C()
            b.doFlush()
        }()
    }
}
```

---

## Phase 3 — Health Checking

### Hysteresis: the difference between a stable system and a self-DDoS

```go
// pkg/health/hysteresis.go

// An instance must fail N times consecutively before being marked critical,
// and pass M times before returning.
//
// WITHOUT THIS, a flapping instance — one that alternates pass/fail every
// interval, which is exactly what a resource-starved or intermittently
// networked host does — generates on EVERY interval:
//     a catalog write
//   + an index bump
//   + a notification to every watcher of that service
//   + a re-resolve and connection-pool rebuild in every client
//
// One sick instance out of ten thousand can generate more control-plane load
// than the entire rest of the fleet. Hysteresis turns that into zero
// transitions, because the instance never reaches the consecutive threshold in
// either direction.
type Hysteresis struct {
    failuresBeforeCritical  int   // default 3
    successesBeforePassing  int   // default 2

    consecutiveFailures  int
    consecutiveSuccesses int
    current              HealthStatus
}

func (h *Hysteresis) Observe(result HealthStatus) (newStatus HealthStatus, changed bool) {
    switch result {
    case HealthPassing:
        h.consecutiveFailures = 0
        h.consecutiveSuccesses++
        if h.current != HealthPassing && h.consecutiveSuccesses >= h.successesBeforePassing {
            h.current = HealthPassing
            return h.current, true
        }
    case HealthCritical:
        h.consecutiveSuccesses = 0
        h.consecutiveFailures++
        if h.current != HealthCritical && h.consecutiveFailures >= h.failuresBeforeCritical {
            h.current = HealthCritical
            return h.current, true
        }
    case HealthWarning:
        // Warning does not reset either counter — it is neither success nor
        // failure. An instance that alternates passing/warning stays passing.
    }
    return h.current, false
}
```

**The test that proves it matters:**

```go
func TestHysteresis_FlappingProducesZeroTransitions(t *testing.T) {
    h := NewHysteresis(3, 2)   // 3 fails to eject, 2 passes to return
    h.current = HealthPassing

    transitions := 0
    // The pathological pattern: perfect alternation, 100 intervals.
    for i := 0; i < 100; i++ {
        result := HealthPassing
        if i%2 == 1 { result = HealthCritical }
        if _, changed := h.Observe(result); changed {
            transitions++
        }
    }

    // Never two consecutive failures, so never ejected. Zero catalog writes,
    // zero index bumps, zero watcher notifications — from an instance that
    // would otherwise generate 100 of each.
    assert.Equal(t, 0, transitions,
        "flapping instance caused %d state transitions; hysteresis is not working",
        transitions)
}
```

### The exec check must kill the process *group*

```go
// pkg/health/check/exec.go

// Killing only the process leaves its children orphaned and running. A health
// check script that spawns `curl` and then times out leaks a curl process on
// every interval — one per 5 seconds, forever, until the agent runs out of PIDs.
func (c *ExecCheck) Run(ctx context.Context) (HealthStatus, string, error) {
    ctx, cancel := context.WithTimeout(ctx, c.timeout)
    defer cancel()

    cmd := exec.Command(c.script, c.args...)
    // Put the child in its own process group so we can signal the whole tree.
    cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

    var out bytes.Buffer
    cmd.Stdout, cmd.Stderr = &out, &out
    if err := cmd.Start(); err != nil {
        return HealthCritical, err.Error(), nil
    }

    done := make(chan error, 1)
    go func() { done <- cmd.Wait() }()

    select {
    case <-ctx.Done():
        // Negative PID = the whole process group.
        _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
        <-done
        return HealthCritical, "timeout", nil
    case err := <-done:
        switch code := exitCode(err); code {
        case 0:  return HealthPassing,  truncate(out.String()), nil
        case 1:  return HealthWarning,  truncate(out.String()), nil
        default: return HealthCritical, truncate(out.String()), nil
        }
    }
}
```

### Outlier detection: the ejection cap is a circuit breaker for the circuit breaker

```go
// pkg/health/outlier/detector.go

// MaxEjectionPercent (default 10) is the most important field here.
//
// THE FAILURE IT PREVENTS: the shared database goes down. Every instance of
// every service starts returning 500s. Outlier detection observes consecutive
// errors on EVERY endpoint and, without a cap, ejects ALL of them.
//
// The pool is now empty. A degradation — where every request was slow or
// erroring but some were succeeding — has become a total outage, caused by the
// resilience mechanism.
//
// The cap says: no matter how bad the evidence looks, keep 90% of the pool.
// If everything is failing, the problem is not the endpoints.
func (d *Detector) sweep() {
    d.mu.Lock()
    defer d.mu.Unlock()

    total := len(d.endpoints)
    maxEject := total * d.cfg.MaxEjectionPercent / 100
    if maxEject < 1 && total > 0 { maxEject = 1 }

    candidates := d.candidatesForEjection()
    // Worst first, so if we can only eject some, we eject the worst ones.
    sort.Slice(candidates, func(i, j int) bool {
        return candidates[i].errorRate > candidates[j].errorRate
    })

    for i, c := range candidates {
        if d.ejectedCount >= maxEject {
            d.events.Emit(Event{
                Kind: EvEjectionCapReached,
                Detail: fmt.Sprintf(
                    "%d/%d endpoints failing but ejection capped at %d (%d%%) — "+
                    "widespread failure suggests a shared dependency, not bad endpoints",
                    len(candidates), total, maxEject, d.cfg.MaxEjectionPercent),
            })
            break
        }
        // Ejection duration grows with repeat offences, so a persistently sick
        // host stops churning in and out of the pool.
        dur := time.Duration(c.ejectionCount+1) * d.cfg.BaseEjectionTime
        d.eject(c.addr, dur)
        _ = i
    }
}
```

---

## Phase 4 — Anti-Entropy

```go
// pkg/agent/antientropy.go

// THE AGENT'S LOCAL STATE IS AUTHORITATIVE for services registered with it.
// The catalog is a replica.
//
// Three consequences that are easy to get backwards:
//
//  1. If the catalog loses data — total server failure, restore from an old
//     snapshot — the agents REPOPULATE it. The system self-heals from complete
//     control-plane data loss, which is a genuinely remarkable property and
//     falls straight out of this one design decision.
//
//  2. If an operator deletes an instance directly from the catalog, the agent
//     PUTS IT BACK. The catalog is not editable for agent-owned services.
//     This surprises people; document it loudly.
//
//  3. Reconciliation is bidirectional: catalog-only entries for this node are
//     removed, agent-only entries are added.
func (a *Agent) sync(ctx context.Context) error {
    local := a.localState.Services()
    remote, err := a.client.NodeServices(ctx, a.nodeName)
    if err != nil { return err }

    var adds, removes, updates int

    // Anything we own that the catalog is missing or has wrong → push.
    for id, svc := range local {
        r, inCatalog := remote[id]
        if !inCatalog {
            if err := a.client.Register(ctx, svc); err != nil { return err }
            adds++
        } else if !svc.Equal(r) {
            if err := a.client.Register(ctx, svc); err != nil { return err }
            updates++
        }
    }

    // Anything the catalog thinks is on this node that we don't own → remove.
    // This is what makes the agent authoritative rather than merely a writer.
    for id := range remote {
        if _, ours := local[id]; !ours {
            if err := a.client.Deregister(ctx, id); err != nil { return err }
            removes++
        }
    }

    a.events.Emit(Event{Kind: EvAntiEntropySync,
        Adds: adds, Removes: removes, Updates: updates})
    return nil
}

// Interval scales with cluster size, and each agent picks a random offset
// within the window. 10,000 agents syncing in lockstep would flatten the
// servers every interval; jitter spreads it into a flat line.
func syncInterval(clusterSize int) time.Duration {
    switch {
    case clusterSize <= 128:  return 1 * time.Minute
    case clusterSize <= 512:  return 5 * time.Minute
    case clusterSize <= 2048: return 10 * time.Minute
    case clusterSize <= 8192: return 20 * time.Minute
    default:                  return 30 * time.Minute
    }
}

func (a *Agent) antiEntropyLoop(ctx context.Context) {
    for {
        base := syncInterval(a.membership.Size())
        jitter := time.Duration(a.rand.Int63n(int64(base / 4)))
        select {
        case <-a.clock.After(base + jitter):
            if err := a.sync(ctx); err != nil {
                a.log.Warn("anti-entropy sync failed", "err", err)
            }
        case <-a.localChanged:
            // Immediate sync on local change, rate-limited so a burst of
            // registrations doesn't become a burst of syncs.
            a.rateLimiter.Wait(ctx)
            _ = a.sync(ctx)
        case <-ctx.Done():
            return
        }
    }
}
```

---

## Phase 5 — Gossip: the highest-value integration

```go
// pkg/store/gossip/failure.go

// ★ THE PAYOFF OF INTEGRATING THE SWIM PROJECT ★
//
// When SWIM declares a node dead, every instance registered on that node must
// leave the serving pool IMMEDIATELY — not after each instance's health check
// independently times out.
//
//   SWIM node-failure detection:  ~2 protocol periods         ≈ 2 s
//   Health check detection:       interval × failures_before  ≈ 15 s
//                                 (5s × 3)
//
// 7× faster for the single most common failure mode in any fleet: a host dying.
// The health checks were never going to tell you anything the membership layer
// didn't already know — they'd just take longer to say it.
func (s *GossipStore) watchMembership(ctx context.Context) {
    ch := make(chan gossip.MemberEvent, 64)
    s.membership.Subscribe(ch)

    for {
        select {
        case ev := <-ch:
            switch ev.Type {
            case gossip.Failed, gossip.Leave:
                instances := s.catalog.InstancesOnNode(ev.Node.Name)
                for _, inst := range instances {
                    // Critical, not deleted: the node may come back, and we want
                    // to keep the registration and its metadata.
                    s.catalog.UpdateHealth(inst.ID, HealthCritical)
                }
                s.events.Emit(Event{
                    Kind: EvNodeFailed, Node: ev.Node.Name,
                    InstancesAffected: len(instances),
                    Detail: "instances marked critical via gossip failure detection",
                })

            case gossip.Join:
                // Node is back. Restore to its last known health, then let the
                // checks re-verify. Do NOT assume passing.
                for _, inst := range s.catalog.InstancesOnNode(ev.Node.Name) {
                    s.catalog.UpdateHealth(inst.ID, inst.LastKnownHealth)
                }
            }
        case <-ctx.Done():
            return
        }
    }
}
```

### Delta conflict resolution

```go
// Reuse SWIM's incarnation-number mechanism rather than inventing a second one.
// Higher incarnation wins; deregistration is monotone at a given incarnation.
func (s *GossipStore) applyDelta(d CatalogDelta) bool {
    s.mu.Lock()
    defer s.mu.Unlock()

    existing, ok := s.instances[d.Instance.ID]
    if ok {
        if d.Incarnation < existing.Incarnation {
            return false   // stale
        }
        // At EQUAL incarnation, deregistration wins over registration.
        // Without this rule, a register delta and a deregister delta at the
        // same incarnation resolve differently depending on arrival order, and
        // the cluster never converges — nodes flip-flop forever.
        if d.Incarnation == existing.Incarnation {
            if existing.Deregistered && d.Type == DeltaRegister {
                return false
            }
        }
    }
    s.apply(d)
    s.membership.Broadcast(encode(d))   // piggyback onward
    return true
}
```

---

## Phase 7 — Watch: three details that bite

```go
// pkg/watch/blocking.go

func (h *Handler) BlockingQuery(w http.ResponseWriter, r *http.Request) {
    service := chi.URLParam(r, "service")
    idx, wait := parseBlockingParams(r)

    // ── DETAIL 1: JITTER THE TIMEOUT ──────────────────────────────────────
    // Without jitter, clients that connected at the same time (a deploy, a
    // control-plane restart) time out at the same instant and re-request
    // together.
    //
    // Worse than a one-off spike: they then converge on the SAME PHASE. Every
    // subsequent cycle is more synchronised than the last, so a herd that
    // starts mild becomes perfectly aligned over time. ±16% jitter breaks the
    // phase lock permanently.
    wait = jitterDuration(wait, 0.16, h.rand)

    // ── DETAIL 2: GUARD AGAINST A FUTURE INDEX ────────────────────────────
    // A client can legitimately hold an index greater than ours: the server
    // restarted, or was restored from a snapshot, or the client talked to a
    // different server that was ahead.
    //
    // Blocking on `> idx` when idx is already in the future means blocking
    // FOREVER. The client hangs and never re-resolves — a silent, permanent
    // failure that looks like the service simply stopped updating.
    if current := h.store.Index(); idx > current {
        idx = 0
    }

    ctx, cancel := context.WithTimeout(r.Context(), wait)
    defer cancel()

    result, err := h.store.Get(ctx, service, catalog.QueryOptions{MinIndex: idx})

    // ── DETAIL 3: TIMEOUT RETURNS STATE, NOT AN ERROR ─────────────────────
    // The client's loop should be identical whether or not anything changed.
    // Returning 5xx on timeout means every client logs an error every `wait`
    // seconds during normal, healthy operation.
    if errors.Is(err, context.DeadlineExceeded) {
        result = h.store.GetNow(service)
        err = nil
    }
    if err != nil {
        writeError(w, err)
        return
    }

    w.Header().Set("X-Beacon-Index", strconv.FormatUint(result.Index, 10))
    json.NewEncoder(w).Encode(result)
}
```

### Watch cache compaction — tell the truth

```go
// pkg/watch/cache.go

var ErrIndexCompacted = errors.New("index too old; client must re-list")

// If a client's index has aged out of the ring buffer, we CANNOT serve it
// correctly. There are two options and only one of them is honest:
//
//   (a) return the current state and pretend nothing was missed
//   (b) return ErrIndexCompacted and make the client re-list
//
// (a) is tempting and wrong. The client's view is now permanently missing
// whatever happened in the gap, and it has no way to know. A deregistered
// instance stays in its address list forever.
//
// Kubernetes returns HTTP 410 Gone here for exactly this reason.
func (c *WatchCache) Since(index uint64) ([]Event, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    if index < c.oldest {
        c.events.Emit(Event{Kind: EvWatchCompacted,
            RequestedIndex: index, OldestAvailable: c.oldest})
        return nil, ErrIndexCompacted
    }
    return c.eventsAfter(index), nil
}
```

### Staggered fan-out

```go
// One change, 1,000 watchers. Firing all 1,000 notifications in the same
// instant means 1,000 clients simultaneously re-resolve, rebuild connection
// pools, and hit the registry again — the notification itself becomes the load
// spike.
//
// Spreading them over a window costs a few hundred milliseconds of propagation
// and removes the spike entirely.
func (r *Registry) notify(service string, ev Event) {
    r.mu.RLock()
    watchers := append([]*watcher(nil), r.watchers[service]...)
    r.mu.RUnlock()

    n := len(watchers)
    if n == 0 { return }

    // Cap the total spread so propagation stays bounded.
    spread := time.Duration(n) * 200 * time.Microsecond
    if spread > 500*time.Millisecond { spread = 500 * time.Millisecond }

    for i, w := range watchers {
        delay := time.Duration(i) * spread / time.Duration(n)
        w := w
        r.clock.AfterFunc(delay, func() {
            select {
            case w.ch <- ev:
            default:
                // Slow consumer. Do not block the fan-out — mark it for a
                // full resync and move on. One slow client must never stall
                // notifications to the other 999.
                w.markResync()
            }
        })
    }
}
```

---

## Phase 12 — The Client SDK

```go
// pkg/sdk/resolver.go

// ★ NEVER PUSH AN EMPTY ADDRESS LIST ★
//
// THE SCENARIO: a bad deploy ships a broken /health endpoint. Every instance
// fails its check within 15 seconds. The catalog now reports zero passing
// instances.
//
// If the resolver pushes an empty list, every client has NOWHERE to send
// traffic and fails 100% of requests — even though the instances are, in fact,
// serving requests perfectly well. The health check was wrong, not the service.
//
// Keeping the last known good set means some requests fail and most succeed.
// Envoy calls this "panic mode" and enters it below 50% healthy, on the
// explicit theory that when the health data says everything is broken, the
// health data is more likely to be wrong than the entire fleet.
func (r *beaconResolver) update(instances []catalog.Instance) {
    addrs := make([]resolver.Address, 0, len(instances))
    for _, inst := range instances {
        if inst.Health != catalog.HealthPassing {
            continue
        }
        addrs = append(addrs, resolver.Address{
            Addr: net.JoinHostPort(inst.Address, strconv.Itoa(inst.Port)),
            // Weight and locality ride along so the picker can use them.
            Attributes: attributes.New(weightKey, inst.Weight).
                WithValue(localityKey, inst.Locality),
        })
    }

    if len(addrs) == 0 {
        if len(r.lastGood) == 0 {
            r.cc.ReportError(errors.New("no instances available and no cached set"))
            return
        }
        addrs = r.lastGood
        r.events.Emit(Event{
            Kind: EvPanicModeEntered,
            Service: r.service,
            Detail: "0 passing instances; serving last known good set of " +
                strconv.Itoa(len(addrs)),
        })
    } else {
        r.lastGood = addrs
        r.persistCache(addrs)   // survives a client restart during an outage
    }

    r.cc.UpdateState(resolver.State{Addresses: addrs})
}

// Reconnect backoff with jitter.
//
// When the control plane restarts, every client's watch stream breaks at the
// same instant. Without jitter they all reconnect together and knock it over
// again — a reconnect storm that turns a 5-second restart into a 5-minute
// outage.
func backoffWithJitter(attempt int, rng *rand.Rand) time.Duration {
    base := time.Duration(math.Min(
        float64(time.Second)*math.Pow(2, float64(attempt)),
        float64(30*time.Second),
    ))
    // Full jitter: uniform in [0, base). Spreads reconnects across the whole
    // window rather than clustering them at the end.
    return time.Duration(rng.Int63n(int64(base)))
}
```

### P2C

```go
// pkg/lb/p2c.go

// Power of two choices: pick two endpoints at random, send to the one with
// fewer outstanding requests.
//
// THE COUNTERINTUITIVE PART: "pick the least loaded" is WORSE than random in a
// distributed setting. Every client independently computes the same answer,
// and they all stampede the same endpoint — so the least-loaded server becomes
// the most-loaded within one round trip, and the system oscillates.
//
// P2C's randomness breaks that synchronisation. It gets you most of the benefit
// of global load awareness with none of the herding, at O(1) cost and zero
// coordination.
func (p *p2cPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
    n := len(p.subConns)
    if n == 0 { return balancer.PickResult{}, balancer.ErrNoSubConnAvailable }
    if n == 1 { return p.result(p.subConns[0]), nil }

    a := p.rand.Intn(n)
    // Pick a distinct second index without a retry loop: choose from n-1 and
    // skip over `a`. A retry loop has unbounded worst-case latency on a hot path.
    b := p.rand.Intn(n - 1)
    if b >= a { b++ }

    sa, sb := p.subConns[a], p.subConns[b]

    // Weighted comparison, so a 2×-capacity host takes 2× the load.
    loadA := float64(sa.inflight.Load()) / float64(sa.weight)
    loadB := float64(sb.inflight.Load()) / float64(sb.weight)
    chosen := sa
    if loadB < loadA { chosen = sb }

    chosen.inflight.Add(1)
    return balancer.PickResult{
        SubConn: chosen.sc,
        Done: func(di balancer.DoneInfo) {
            chosen.inflight.Add(-1)
            // ← The seam with the interceptors project: every RPC outcome
            //   feeds passive health checking, with no application involvement.
            p.outlier.Record(chosen.addr, di.Err)
        },
    }, nil
}
```

---

## Phase 13 — xDS

```go
// pkg/xds/order.go

// ★ ADS ORDERING IS "MAKE BEFORE BREAK" ★
//
// ADS multiplexes all resource types onto one stream specifically so the
// control plane can control ordering. Ordering is not cosmetic:
//
//   Push LDS referencing cluster "payments-v2" before CDS defines it, and
//   Envoy either rejects the listener outright or accepts it and 503s every
//   request routed to the missing cluster.
//
// ADD order is bottom-up:      define it, fill it, then point traffic at it.
// REMOVE order is the reverse: stop pointing traffic at it, then delete it.
//
// Getting this backwards produces exactly the 503 spike during deploys that
// teams usually blame on the application.
var addOrder = []string{
    resource.ClusterType,   // 1. define the cluster
    resource.EndpointType,  // 2. populate it
    resource.ListenerType,  // 3. open the listener
    resource.RouteType,     // 4. route to it
}

var removeOrder = []string{
    resource.RouteType,     // 1. stop routing
    resource.ListenerType,  // 2. close the listener
    resource.EndpointType,  // 3. drain endpoints
    resource.ClusterType,   // 4. delete the cluster
}
```

```go
// pkg/xds/server.go

// NACK SEMANTICS — the part people get wrong.
//
// A NACK does NOT mean "resend". It means "I rejected this and I am still
// running the previous config."
//
// If the control plane retries the same config, the proxy NACKs again, and you
// have a hot loop between control plane and data plane that makes no progress
// and consumes CPU on both. This is a real outage mode, not a theoretical one.
//
// DETECT A NACK BY THE PRESENCE OF error_detail, not by comparing versions.
// Version comparison breaks for clients that dynamically change their
// subscription set — the version can legitimately differ without a rejection.
func (s *Server) handleRequest(req *discovery.DiscoveryRequest, st *streamState) error {
    typeURL := req.TypeUrl

    if req.ErrorDetail != nil {
        s.metrics.NACKs.WithLabelValues(typeURL).Inc()
        s.events.Emit(Event{
            Kind:    EvXDSNack,
            Node:    req.Node.GetId(),
            Type:    typeURL,
            Version: req.VersionInfo,   // the version they're STILL running
            Error:   req.ErrorDetail.Message,
        })
        st.nacked[typeURL] = req.VersionInfo
        // ★ Do NOT resend. Surface it and wait for a new, different config.
        return nil
    }

    // An ACK matching our last nonce confirms the version was applied.
    if req.ResponseNonce != "" && req.ResponseNonce == st.lastNonce[typeURL] {
        st.acked[typeURL] = req.VersionInfo
        delete(st.nacked, typeURL)
        s.events.Emit(Event{Kind: EvXDSAck, Node: req.Node.GetId(),
            Type: typeURL, Version: req.VersionInfo})
    }

    // A request with a stale nonce is a spontaneous subscription change, not an
    // ACK. Update the subscription set but do not treat it as confirmation.
    st.subscribed[typeURL] = req.ResourceNames
    return s.maybePush(st, typeURL)
}
```

---

## Phase 16 — The Measurement ⭐

**This is the headline deliverable. The argument for gossip-driven discovery is a number, and this produces it.**

```go
// pkg/bench/propagation.go

// THE STALE-ENDPOINT WINDOW: an instance dies. How long until the last client
// stops sending it traffic?
//
// Almost nobody measures this, and it is the number that actually determines
// how many requests a host failure costs you.
type PropagationResult struct {
    Config string

    // The four stages. Reporting only the total hides where the time goes.
    DetectionMs    float64  // death → registry knows
    PropagationMs  float64  // registry knows → all servers know
    NotificationMs float64  // servers know → all clients notified
    ClientApplyMs  float64  // notified → address list updated

    TotalMs        float64
    P50, P99, Max  float64
}

func (b *Bench) MeasureStaleWindow(ctx context.Context, cfg Config) PropagationResult {
    traceID := newTraceID()

    // Every client registers an observer keyed by trace ID, so we learn exactly
    // when each one applied the change.
    observers := b.registerObservers(traceID)

    death := b.clock.Now()
    b.killInstance(ctx, cfg.TargetInstance, traceID)

    // Wait for the LAST client, not the first. The p99 client is the one that
    // determines your error budget.
    last := b.waitAllObservers(ctx, observers)

    return PropagationResult{
        Config:         cfg.Name,
        DetectionMs:    ms(b.traceStage(traceID, StageDetected) - death),
        PropagationMs:  ms(b.traceStage(traceID, StageAllServers) -
                           b.traceStage(traceID, StageDetected)),
        NotificationMs: ms(b.traceStage(traceID, StageAllNotified) -
                           b.traceStage(traceID, StageAllServers)),
        ClientApplyMs:  ms(last - b.traceStage(traceID, StageAllNotified)),
        TotalMs:        ms(last - death),
    }
}

// The four configurations that produce the headline table.
var Configs = []Config{
    {Name: "gossip + streaming watch", Gossip: true,  Watch: Streaming},
    {Name: "healthcheck + streaming",  Gossip: false, Watch: Streaming},
    {Name: "healthcheck + blocking",   Gossip: false, Watch: Blocking},
    {Name: "healthcheck + DNS",        Gossip: false, Watch: DNSOnly, DNSTTLSec: 30},
}
```

```go
func TestPropagation_GossipIsDramaticallyFaster(t *testing.T) {
    b := NewBench(t, 100 /* nodes */, 50 /* clients */)
    results := make(map[string]PropagationResult)
    for _, cfg := range Configs {
        results[cfg.Name] = b.MeasureStaleWindow(context.Background(), cfg)
    }

    fast := results["gossip + streaming watch"]
    slow := results["healthcheck + DNS"]

    assert.Less(t, fast.TotalMs, 3000.0,
        "gossip + streaming should converge in under 3s, got %.0fms", fast.TotalMs)
    assert.Greater(t, slow.TotalMs/fast.TotalMs, 10.0,
        "expected the DNS path to be at least 10× slower; got %.1f×",
        slow.TotalMs/fast.TotalMs)

    t.Logf("\n%-32s %10s %10s %10s %10s %10s",
        "config", "detect", "propagate", "notify", "apply", "TOTAL")
    for _, cfg := range Configs {
        r := results[cfg.Name]
        t.Logf("%-32s %9.0fms %9.0fms %9.0fms %9.0fms %9.0fms",
            r.Config, r.DetectionMs, r.PropagationMs, r.NotificationMs,
            r.ClientApplyMs, r.TotalMs)
    }
}
```

---

## Frontend — the view that is the point

```tsx
// console/src/components/propagation/PropagationTimeline.tsx

/// THE FLAGSHIP VIEW.
///
/// Kill an instance and watch the change travel through the system, hop by hop,
/// with a timestamp at every stage. The convergence time is called out in large
/// type because it is the number the whole project exists to produce.
///
/// The comparison mode — running the same experiment with gossip disabled or
/// DNS-only — is what makes it an argument rather than a demo. Seeing 1.2s next
/// to 47s, for the same event on the same cluster, settles the design question
/// in a way no amount of prose does.
export function PropagationTimeline({ trace, comparison }: Props) {
  const hops = useMemo(() => buildHops(trace), [trace]);
  const x = d3.scaleLinear().domain([0, trace.totalMs]).range([0, width]);

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader><CardTitle>Convergence</CardTitle></CardHeader>
        <CardContent>
          <div className="text-5xl font-mono tabular-nums">
            {trace.totalMs.toFixed(0)}<span className="text-2xl ml-1">ms</span>
          </div>
          <div className="text-sm text-muted-foreground mt-1">
            {trace.observerCount} clients converged · {trace.nodeCount} nodes
          </div>
        </CardContent>
      </Card>

      <svg width={width} height={hops.length * 28 + 40}>
        {hops.map((hop, i) => (
          <g key={hop.id} transform={`translate(0,${i * 28})`}>
            <text x={0} y={14} fontSize={11} fill="#94a3b8" className="font-mono">
              {hop.component}
            </text>
            <rect
              x={x(hop.startMs)} y={4}
              width={Math.max(2, x(hop.endMs) - x(hop.startMs))} height={16}
              rx={2} fill={STAGE_COLOR[hop.stage]}
            />
            <text x={x(hop.endMs) + 6} y={16} fontSize={10} fill="#64748b"
                  className="font-mono tabular-nums">
              +{hop.endMs.toFixed(0)}ms
            </text>
          </g>
        ))}
      </svg>

      {/* The comparison is the argument. */}
      {comparison && (
        <div className="grid grid-cols-2 gap-4">
          {comparison.map(c => (
            <Card key={c.config} className={c.totalMs > 10000 ? 'border-red-500/50' : ''}>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-normal">{c.config}</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-3xl font-mono tabular-nums">
                  {(c.totalMs / 1000).toFixed(1)}s
                </div>
                <StageBreakdown result={c} />
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </div>
  );
}
```

---

## Correctness Invariants

1. **Monotonic index** — never decreases; every mutation increases it
2. **No-op writes don't bump** — `UpdateHealth` with an unchanged status changes nothing
3. **Hysteresis** — a perfectly flapping instance produces zero state transitions
4. **Ejection cap** — never eject more than `MaxEjectionPercent`, even at 100% failure
5. **Never-empty resolution** — the resolver retains the last good set and reports panic mode
6. **Watch completeness** — every event after a client's index is delivered, or `ErrIndexCompacted` is returned. Never silently skipped
7. **Watch liveness** — a blocking query always returns within `wait + jitter`
8. **No goroutine leaks** — 10,000 watchers connect and disconnect; the count returns to baseline
9. **Anti-entropy authority** — an entry deleted from the catalog is restored by its agent
10. **Catalog repopulation** — wiping all server state is fully recovered within one sync interval
11. **Gossip convergence** — a delta reaches all live nodes in `O(log N)` rounds
12. **Node failure propagation** — a dead node's instances go critical everywhere within 3 s
13. **Delta determinism** — conflicting deltas resolve identically regardless of arrival order
14. **AP availability** — both partition sides accept writes
15. **CP safety** — no two servers return conflicting linearizable results
16. **xDS ordering** — CDS before EDS, LDS before RDS, on every push
17. **NACK non-amplification** — a rejected config is never resent unchanged

---

## Code Standards

**Go**
- **Every timer goes through `Clock`.** No bare `time.After`/`time.NewTicker` outside `pkg/clock`. Enforce with a lint rule.
- **`TraceID` in every event.** Phase 16 depends on it and retrofitting is miserable.
- No unbounded goroutines: every `go func()` takes a `context.Context` and exits on cancellation. Verify with a goroutine-count assertion in tests.
- `context` cancellation must tear down the whole subtree — watch, health subscriptions, xDS resources.
- Every fan-out is bounded and non-blocking. A slow consumer is dropped to resync, never allowed to stall the others.
- `sync.RWMutex` on the catalog; the read path is 1000:1 dominant, so never take a write lock on a read.
- Health checks are agent-local and go over loopback. There is no central prober.
- Errors are structured with a code; clients branch on codes, not strings.
- `go test -race` on everything. The catalog and watch registry are the hot spots.

**Frontend**
- SSE with reconnect; ring-buffer the event store — a busy cluster emits thousands of events per second and the browser must not accumulate them.
- D3 owns its SVG subtree; React owns everything around it.
- Every panel handles the "control plane unreachable" state gracefully, because demonstrating that state is half the point.

---

## Startup

```bash
go test -race ./...
make sim                                    # all scenarios

# 3 servers, 5 agents, in-process
go run ./cmd/beacon-server --bootstrap-expect 3 --consistency ap &
go run ./cmd/beacon-agent --node n1 --join localhost:7946 &

beacon register --name payments --port 8080 --check http://localhost:8080/health
beacon watch payments                       # stream changes to stdout
dig @localhost -p 8600 payments.service.beacon SRV

cd console && bun run dev
```

**The command that produces the headline result:**

```
$ beacon bench propagate --nodes 100 --clients 50 --kill payments-3

config                             detect  propagate    notify     apply     TOTAL
─────────────────────────────────────────────────────────────────────────────────
gossip + streaming watch            1840ms      680ms      12ms      18ms    2550ms
healthcheck + streaming            15100ms       50ms      14ms      21ms   15185ms
healthcheck + blocking query       15100ms       50ms    2400ms      19ms   17569ms
healthcheck + DNS (ttl=30)         15100ms       50ms   29800ms    1100ms   46050ms

  gossip + streaming is 18.1× faster than healthcheck + DNS
```

That table is the project. Same cluster, same failure, four configurations, and an 18× spread — with the breakdown showing exactly *where* the time goes. Detection dominates in three of the four rows, which is precisely why integrating the gossip project matters: SWIM already knew the host was dead 13 seconds before the health checks worked it out.

**Then open the console and run it live.** Kill an instance from the topology view and watch the propagation timeline fill in hop by hop, then flip on comparison mode and watch the DNS bar keep growing for another 45 seconds after the gossip bar has finished.

**Then open the Consistency Lab**, run AP and CP side by side, and hit the partition toggle. AP keeps accepting registrations on both sides while the divergence counter climbs; CP's minority side starts returning errors. Heal the partition and watch AP's counter drop back to zero while CP resumes. Neither is wrong — but you can now say *which* you'd choose and defend it with a number instead of a preference.
