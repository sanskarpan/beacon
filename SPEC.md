# SPEC.md — `beacon`: A Service Discovery System

> **Backend: Go 1.22+** — registry, health checking, gossip propagation, watch/notify, DNS + HTTP + gRPC interfaces, xDS control plane, client SDK
> **Frontend: React 18 + TypeScript + Vite + Tailwind + shadcn/ui + D3 + Recharts** — a mesh topology + propagation observatory
> **Extends two existing projects:** the SWIM gossip protocol and the gRPC-with-interceptors service

---

## §1 Language Decision — Go

### This one is decided by the ecosystem and by continuity

Previous projects in this series chose Rust when the problem was *systems-level* (a JIT, a container runtime, an async runtime) — places where you need byte-exact control and there is no ecosystem to lean on.

**Service discovery is the opposite kind of problem.** It is a networking and control-plane problem, and the entire body of prior art is Go:

| System | Language | What it contributes here |
|---|---|---|
| Consul | Go | Agent/server split, anti-entropy, blocking queries, DNS interface |
| Serf | Go | SWIM gossip (**your existing project reimplements this**) |
| etcd | Go | Watch with revisions, lease-based TTL |
| Kubernetes | Go | Endpoints/EndpointSlice, informers, watch cache |
| CoreDNS | Go | Plugin-based DNS server |
| `go-control-plane` | Go | The canonical xDS server implementation |
| Nomad, Traefik, Linkerd control plane | Go | Registration and routing patterns |

Beyond ecosystem, three properties of Go fit this problem specifically:

1. **Goroutine-per-stream is exactly right for watch/notify.** A registry serving 10,000 concurrent `Watch` streams is 10,000 goroutines at ~4 KB each — 40 MB, and the code reads like blocking I/O. This is the dominant workload of the system.
2. **`net/http`, `crypto/tls`, `miekg/dns`, and `grpc-go` are all first-class.** You will implement a DNS server, an HTTP long-polling API, a gRPC streaming API, and mTLS. In Go these are the standard tools; elsewhere they are projects of their own.
3. **`context.Context` cancellation propagates cleanly** through the watch tree — a client disconnecting must tear down its subscription, its health-check subscriptions, and its xDS resources, and Go's cancellation model does this without ceremony.

### Continuity with your existing work

This project is explicitly the **integration layer** over two things you have already built:

```
  Gossip Protocol project  ──┐
  (SWIM membership,          │
   failure detection,        ├──►  beacon
   anti-entropy)             │     (service discovery)
                             │
  gRPC + Interceptors  ──────┘
  (streaming, auth,
   observability middleware)
```

- The **gossip project** becomes `beacon`'s membership and propagation layer. SWIM already gives you node liveness; `beacon` adds *service* liveness on top and piggybacks catalog deltas on the existing gossip stream.
- The **gRPC interceptors project** becomes the client-side integration: the discovery-aware resolver, the load-balancing picker, and interceptors that report per-endpoint outcomes back into passive health checking.

Both are Go. Rewriting them in another language to satisfy an aesthetic preference would be the wrong trade.

### Where Go is genuinely weaker, and how we handle it

- **Nondeterminism** (goroutine scheduling, map iteration) makes deterministic simulation harder than it was in the consensus project. We compensate with a **discrete-event simulator over an injectable clock and transport**, plus `testing/synctest` (Go 1.24+), and we accept that this is testing-by-scenario rather than exhaustive replay.
- **No sum types.** The event and message model uses tagged structs with an explicit `Kind` field and exhaustiveness enforced by a `switch` with a `default: panic(...)` plus a linter check.

### Dependencies

| Package | Role |
|---|---|
| `google.golang.org/grpc` | Watch streams, xDS, client resolver/balancer |
| `google.golang.org/protobuf` | Wire format |
| `github.com/miekg/dns` | DNS server (A/AAAA/SRV/TXT) |
| `github.com/hashicorp/go-memdb` *(or hand-rolled)* | Radix-indexed in-memory catalog with MVCC-ish versioning |
| `golang.org/x/sync/singleflight` | Collapse duplicate resolves — the thundering-herd defence |
| `github.com/prometheus/client_golang` | Metrics |
| `go.opentelemetry.io/otel` | Tracing across register → propagate → resolve |
| *(your gossip project)* | Membership + propagation transport |
| *(your consensus project, optional)* | CP-mode catalog storage |

**No Consul, no etcd, no Kubernetes client.** The point is to build the registry, not to configure one.

---

## §2 What This Project Covers

| Area | Concepts |
|---|---|
| Registration | Self-registration, third-party registration, sidecar registration, registration storms |
| Leases | TTL, heartbeat renewal, grace periods, lease expiry vs explicit deregistration |
| Health checking | Active (HTTP/TCP/gRPC/exec), passive (outlier detection), TTL push, check aggregation, flapping/hysteresis |
| Catalog | Services, instances, metadata, tags, versions, subsets, monotonic index |
| Anti-entropy | Local agent state as authority, periodic reconciliation, catalog repopulation after loss |
| Gossip propagation | Piggybacking catalog deltas on SWIM, convergence time, partition behaviour |
| Consistency | **AP mode** (gossip, eventually consistent) vs **CP mode** (Raft, linearizable) — both implemented, side by side |
| Watch/Notify | Blocking queries with index, gRPC streaming, delta updates, resumption, watch cache, compaction |
| Thundering herd | Response jitter, singleflight collapsing, watch fan-out batching |
| Resolution | gRPC resolver/balancer, DNS (A/SRV), HTTP API, client-side caching |
| Load balancing | Round-robin, weighted, least-request, P2C, ring-hash, locality/zone-aware priority |
| Service mesh | xDS control plane (LDS/CDS/EDS/RDS), ADS ordering, SotW vs Delta, ACK/NACK, mTLS, SPIFFE identity |
| Traffic management | Subsets, canary weights, circuit breaking, retries, outlier ejection |
| Failure modes | The stale-endpoint window, split brain, registration storms, flapping, watch starvation |
| Observability | End-to-end propagation tracing, per-hop latency, convergence measurement |

---

## §3 Architecture

```
                       ┌──────────────── Control Plane ────────────────┐
                       │                                                │
   ┌─────────┐         │   ┌──────────────────────────────────────┐    │
   │ Console │◄────────┼───┤  beacon-server (3 or 5 nodes)         │    │
   │ (React) │  SSE    │   │  ├─ Catalog (services, instances)     │    │
   └─────────┘         │   │  ├─ Health aggregator                 │    │
                       │   │  ├─ Watch registry (index-based)      │    │
                       │   │  ├─ xDS control plane (ADS)           │    │
                       │   │  ├─ DNS server (:8600)                │    │
                       │   │  ├─ HTTP API (:8500)                  │    │
                       │   │  └─ gRPC API (:8502)                  │    │
                       │   └──────────┬───────────────────────────┘    │
                       │              │                                 │
                       │   ┌──────────▼───────────┐  ┌──────────────┐  │
                       │   │  Storage backend      │  │  Gossip pool │  │
                       │   │  AP: gossip-replicated│◄─┤  (SWIM, from │  │
                       │   │  CP: Raft-replicated  │  │   your proj) │  │
                       │   └───────────────────────┘  └──────┬───────┘  │
                       └────────────────────────────────────┬┼──────────┘
                                                            ││
        ┌───────────────────────────────────────────────────┘│
        │                                                     │
   ┌────▼──────────────┐   ┌───────────────────┐   ┌─────────▼─────────┐
   │ beacon-agent      │   │ beacon-agent      │   │ beacon-agent      │
   │ (node 1)          │   │ (node 2)          │   │ (node N)          │
   │ ├─ local state ★  │   │ ├─ local state    │   │ ├─ local state    │
   │ ├─ health runner  │   │ ├─ health runner  │   │ ├─ health runner  │
   │ ├─ anti-entropy   │   │ ├─ anti-entropy   │   │ ├─ anti-entropy   │
   │ └─ gossip member  │   │ └─ gossip member  │   │ └─ gossip member  │
   └────┬──────────────┘   └────────┬──────────┘   └─────────┬─────────┘
        │                            │                        │
   ┌────▼────┐  ┌────────┐     ┌────▼────┐              ┌────▼────┐
   │ svc-a   │  │ svc-b  │     │ svc-a   │              │ svc-c   │
   │ :8080   │  │ :8081  │     │ :8080   │              │ :8082   │
   └─────────┘  └────────┘     └─────────┘              └─────────┘
        ▲
        │  beacon-sdk (Go): resolver + balancer + interceptors
        │  ← extends your gRPC-interceptors project
```

### The architectural decision that matters most: agent-local health checking

```
Health checks run on the AGENT that owns the instance, NOT centrally.

Centralised checking:  10,000 instances × 1 check/5s = 2,000 checks/sec
                       from the control plane, over the network, to every
                       corner of the fleet. The control plane becomes a
                       distributed monitoring system, and its network
                       position determines what "healthy" means.

Agent-local checking:  each agent checks ~10 local instances over loopback.
                       Sub-millisecond. No cross-network health traffic at
                       all. The control plane receives STATE, not probes.

This is why Consul scales and why naive registries don't. It also means the
health signal reflects "can this instance serve" rather than "can the control
plane reach this instance" — which are different questions, and the second one
is the wrong one.
```

### Anti-entropy: the agent is authoritative for its own services

```
The agent's local state is the SOURCE OF TRUTH for services registered with
it. The catalog is a replica.

Consequences:
  1. If the catalog loses data — total server failure, restore from an old
     snapshot — the agents repopulate it within one anti-entropy interval.
     The system self-heals from complete control-plane data loss.
  2. If an operator deletes an instance from the catalog directly, the agent
     puts it back. The catalog is not editable for agent-owned services.
  3. Sync interval scales with cluster size, with per-agent jitter, so 10,000
     agents don't sync in lockstep.
```

---

## §4 Data Model

```go
// pkg/catalog/types.go

// Service is a logical name. Instances implement it.
type Service struct {
    Name        string            `json:"name"`
    Namespace   string            `json:"namespace"`
    Tags        []string          `json:"tags"`
    Meta        map[string]string `json:"meta"`
    // Monotonic, bumped on any change to this service or its instances.
    // Watchers pass this back to block until it changes. Same idea as
    // Consul's X-Consul-Index and etcd's revision.
    ModifyIndex uint64            `json:"modify_index"`
}

type Instance struct {
    ID          string            `json:"id"`           // unique, stable across restarts
    Service     string            `json:"service"`
    Node        string            `json:"node"`         // the agent that owns it
    Address     string            `json:"address"`
    Port        int               `json:"port"`

    // Free-form, drives subsets and routing: version, region, zone, canary…
    Meta        map[string]string `json:"meta"`
    Tags        []string          `json:"tags"`

    // Locality drives zone-aware routing and failover priority.
    Locality    Locality          `json:"locality"`

    // Relative capacity. The balancer honours it.
    Weight      int               `json:"weight"`

    Checks      []CheckID         `json:"checks"`
    Health      HealthStatus      `json:"health"`       // aggregate of Checks

    Lease       *Lease            `json:"lease,omitempty"`

    CreateIndex uint64            `json:"create_index"`
    ModifyIndex uint64            `json:"modify_index"`
}

type Locality struct {
    Region string `json:"region"`
    Zone   string `json:"zone"`
    SubZone string `json:"sub_zone"`
}

type HealthStatus string
const (
    HealthPassing  HealthStatus = "passing"
    HealthWarning  HealthStatus = "warning"   // serve traffic, but flag it
    HealthCritical HealthStatus = "critical"  // remove from the pool
    HealthMaint    HealthStatus = "maintenance"
)

// Aggregation rule: an instance is as healthy as its WORST check.
// One critical check makes the instance critical, regardless of the others.
// This is the conservative choice and it's the right one: a service with a
// working HTTP endpoint and a broken database connection is not healthy.
func Aggregate(checks []Check) HealthStatus { /* … */ }
```

### Leases

```go
// A registration without a lease is permanent until explicitly removed.
// A registration WITH a lease dies if not renewed. This is what makes the
// registry self-cleaning when instances die without deregistering — which is
// the common case, since SIGKILL and OOM don't run shutdown hooks.
type Lease struct {
    ID          string    `json:"id"`
    TTL         Duration  `json:"ttl"`
    GrantedAt   time.Time `json:"granted_at"`
    ExpiresAt   time.Time `json:"expires_at"`
    // On expiry: mark critical immediately, but delay REMOVAL. A brief
    // network blip should not erase an instance's registration and its
    // metadata — it should just take it out of the serving pool.
    DeregisterAfter Duration `json:"deregister_after"`
}
```

---

## §5 Registration Protocols

Three patterns, all supported, because real fleets use all three.

### 5.1 Self-registration

```go
// The service registers itself on startup and deregisters on shutdown.
// Simplest, and couples the application to the registry.
//
// THE FAILURE MODE: SIGKILL, OOM-kill, and kernel panic all skip the
// shutdown hook. Self-registration alone leaves ghost entries forever. It
// MUST be paired with a lease or a health check — this is not optional.
func (c *Client) Register(ctx context.Context, inst *Instance) (*Registration, error)
func (c *Client) Deregister(ctx context.Context, id string) error
```

### 5.2 Third-party registration

```go
// A registrar watches the platform (scheduler, cloud API, container runtime)
// and registers on the service's behalf. The service knows nothing.
//
// This is how Kubernetes works: the kubelet reports pod status, the endpoints
// controller writes EndpointSlices. The application never calls a registry.
type Registrar interface {
    // Emits the full desired set on every change. The reconciler diffs.
    Watch(ctx context.Context) (<-chan []Instance, error)
}
```

### 5.3 Sidecar registration

```go
// The sidecar proxy registers itself and its upstream. Registration and
// traffic interception are the same component, so they cannot disagree.
```

### Registration storms

```go
// Deploying 1,000 instances registers 1,000 times in a few seconds, each
// bumping the catalog index, each waking every watcher for that service.
//
//   1,000 registrations × 500 watchers = 500,000 notifications
//
// Three defences, all implemented:
//   1. BATCH the index bump — coalesce registrations within a window
//      (default 50ms) into one index increment
//   2. JITTER watch responses over a window so 500 watchers don't all
//      re-resolve in the same millisecond
//   3. RATE-LIMIT registration per node, with a clear error rather than
//      silent queueing
```

---

## §6 Health Checking

### Check types

```go
type CheckType string
const (
    CheckHTTP CheckType = "http"   // GET; 2xx=passing, 429=warning, else critical
    CheckTCP  CheckType = "tcp"    // connect succeeds
    CheckGRPC CheckType = "grpc"   // grpc.health.v1.Health/Check
    CheckExec CheckType = "exec"   // exit 0=passing, 1=warning, else critical
    CheckTTL  CheckType = "ttl"    // the SERVICE pushes; absence is failure
    CheckAlias CheckType = "alias" // mirror another service's health
)

type Check struct {
    ID       CheckID
    Type     CheckType
    Interval Duration
    Timeout  Duration

    // Hysteresis. A check must fail N times consecutively before the
    // instance is ejected, and pass M times before it returns.
    //
    // WITHOUT THIS, A FLAPPING INSTANCE OSCILLATES IN AND OUT OF THE POOL
    // EVERY INTERVAL, and each transition is a catalog write, an index bump,
    // and a notification to every watcher. One sick instance can generate
    // more control-plane load than the entire rest of the fleet.
    FailuresBeforeCritical  int   // default 3
    SuccessesBeforePassing  int   // default 2

    // How long an instance may remain critical before it is REMOVED (not
    // just ejected). Separates "temporarily unhealthy" from "gone".
    DeregisterCriticalAfter Duration
}
```

### Active vs passive

```
ACTIVE (probes)              PASSIVE (outlier detection)
─────────────────            ────────────────────────────
Synthetic requests           Observes REAL traffic
Detects total failure        Detects partial/degraded failure
Fixed cost per instance      Zero additional cost
Can pass while the           Catches the case where /health
  real path is broken          returns 200 but /api returns 503

USE BOTH. Active health checking answers "is the process alive". Outlier
detection answers "is this instance actually serving my requests correctly",
and those are different questions with different answers surprisingly often.
```

```go
// Passive health checking, fed by the CLIENT SDK's interceptors —
// this is where your gRPC-interceptors project plugs in.
type OutlierDetection struct {
    ConsecutiveErrors     int      // default 5
    Interval              Duration // analysis sweep, default 10s
    BaseEjectionTime      Duration // default 30s
    MaxEjectionPercent    int      // default 10 — NEVER eject more than this

    // The guard that matters: if the whole fleet is erroring because the
    // DATABASE is down, ejecting every instance turns a degradation into a
    // total outage. MaxEjectionPercent means you always keep 90% of the pool
    // no matter how bad things look.
    SuccessRateMinimumHosts int
    SuccessRateStdevFactor  float64
}

// Ejection time grows with repeat offences: base × ejection_count. An
// instance that keeps failing gets ejected for progressively longer,
// so a persistently sick host stops churning the pool.
func (o *OutlierDetection) EjectionDuration(count int) time.Duration {
    return time.Duration(count) * o.BaseEjectionTime
}
```

---

## §7 Anti-Entropy & Gossip Propagation

**This is where your existing SWIM project becomes a component.**

```go
// The gossip layer already gives us:
//   - node membership (who exists)
//   - failure detection (who is alive)
//   - a piggyback channel with O(log N) convergence
//
// beacon adds SERVICE-level state on top, propagated the same way.
type CatalogDelta struct {
    Type      DeltaType   // Register | Deregister | HealthChange
    Instance  *Instance
    Index     uint64
    Origin    NodeID
    // Lamport-ish counter per origin node. On conflict, the higher
    // incarnation wins — the same mechanism SWIM uses for membership.
    Incarnation uint64
}
```

### Node failure → service removal

```
This is the highest-value integration between the two projects.

When SWIM declares node N dead, every instance registered on N must leave the
serving pool IMMEDIATELY — no waiting for health checks to time out
independently.

SWIM detects node failure in ~2 protocol periods. Waiting for a 10-second
health check interval × 3 failures = 30 seconds would be 15× slower for the
single most common failure mode (a host dying).

  gossip: node-dead(N)  ──►  catalog: all instances on N → critical
                        ──►  watchers notified
                        ──►  clients stop routing
```

### Anti-entropy sync

```go
// The agent periodically pushes its local state to the catalog and reconciles
// differences. Interval scales with cluster size and is jittered per agent.
//
//   ≤ 128 nodes:    1 min
//   ≤ 512 nodes:    5 min
//   ≤ 2048 nodes:   10 min
//   > 8192 nodes:   30 min
//
// Each agent picks a random offset within the window, so 10,000 agents don't
// sync simultaneously and flatten the servers.
func (a *Agent) antiEntropyLoop(ctx context.Context) {
    for {
        jitter := time.Duration(rand.Int63n(int64(a.syncInterval / 4)))
        select {
        case <-time.After(a.syncInterval + jitter):
            a.sync(ctx)
        case <-ctx.Done():
            return
        }
    }
}
```

---

## §8 Consistency: AP vs CP, Side by Side

**Both modes are implemented. Comparing them is a core deliverable.**

```
AP MODE — gossip-replicated catalog
  Writes: accepted by any server, propagated by gossip
  Reads:  local, always available
  During a partition: BOTH SIDES SERVE. Views diverge. Converge on heal.
  Convergence: O(log N) gossip rounds, ~2-5 seconds for 100 nodes
  Failure mode: a client may route to an instance that was deregistered
                seconds ago, or miss one that just registered

CP MODE — Raft-replicated catalog (uses your consensus project)
  Writes: forwarded to the leader, committed by quorum
  Reads:  linearizable (leader + ReadIndex), or stale (any server, faster)
  During a partition: the MINORITY SIDE CANNOT WRITE and, for linearizable
                reads, cannot read either
  Convergence: immediate on commit
  Failure mode: registration fails entirely during a partition; instances
                that come up on the minority side are invisible

THE TRADE, stated plainly:
  AP  = you may route to a stale endpoint
  CP  = you may be unable to register at all

For service discovery, AP is usually right — a stale endpoint produces one
failed request and a retry, while an unavailable registry produces a total
outage. This is why Consul puts MEMBERSHIP in gossip (AP) and the CATALOG in
Raft (CP), and why Eureka is deliberately AP end-to-end.
```

```go
type CatalogStore interface {
    Register(ctx context.Context, inst *Instance) (uint64, error)
    Deregister(ctx context.Context, id string) (uint64, error)
    UpdateHealth(ctx context.Context, id string, h HealthStatus) (uint64, error)

    // index=0 means "return now". index>0 means "block until ModifyIndex > index".
    Get(ctx context.Context, service string, opts QueryOptions) (*Result, error)

    Watch(ctx context.Context, service string, fromIndex uint64) (<-chan Event, error)
}

// Two implementations. Same interface. `--consistency=ap|cp` at startup, and
// the console shows the behavioural difference live.
type GossipStore struct { /* … */ }   // AP
type RaftStore   struct { /* … */ }   // CP
```

---

## §9 Watch / Notify API

Three mechanisms, because clients differ.

### 9.1 Blocking queries (Consul style)

```
GET /v1/health/service/payments?index=4821&wait=5m

  index=0  → return immediately with the current state
  index=N  → block until ModifyIndex > N, or `wait` elapses
  Response header: X-Beacon-Index: 4830

The client loops, feeding the returned index back in. Simple, works through
any HTTP proxy, no special client library.
```

```go
// Three details that are easy to get wrong and painful in production:
func (h *Handler) blockingQuery(w http.ResponseWriter, r *http.Request) {
    idx, wait := parseBlockingParams(r)

    // 1. JITTER THE TIMEOUT by ±16%. Without it, 500 clients that connected
    //    at the same time all time out at the same instant and re-request
    //    together — a self-synchronising thundering herd that gets WORSE
    //    over time as clients converge on the same phase.
    wait = jitter(wait, 0.16)

    // 2. GUARD AGAINST INDEX GOING BACKWARDS. If the client sends an index
    //    from the future (server restart, restore from snapshot), block
    //    forever. Reset to 0 and return immediately instead.
    if idx > h.store.CurrentIndex() {
        idx = 0
    }

    ctx, cancel := context.WithTimeout(r.Context(), wait)
    defer cancel()
    result, err := h.store.Get(ctx, service, QueryOptions{MinIndex: idx})

    // 3. ON TIMEOUT, RETURN THE CURRENT STATE, not an error. The client's
    //    loop should be uniform whether or not anything changed.
    if errors.Is(err, context.DeadlineExceeded) {
        result = h.store.GetNow(service)
    }
    w.Header().Set("X-Beacon-Index", strconv.FormatUint(result.Index, 10))
    json.NewEncoder(w).Encode(result)
}
```

### 9.2 gRPC streaming

```protobuf
service Discovery {
  // Server streaming. First message is the full snapshot; subsequent
  // messages are deltas.
  rpc Watch(WatchRequest) returns (stream WatchEvent);

  // Bidirectional: the client can add/remove subscriptions on one stream
  // without reconnecting. This is what xDS does, and it matters when a
  // client depends on 50 services.
  rpc WatchMulti(stream WatchMultiRequest) returns (stream WatchEvent);
}

message WatchEvent {
  enum Kind { SNAPSHOT = 0; ADD = 1; UPDATE = 2; REMOVE = 3; }
  Kind kind = 1;
  string service = 2;
  repeated Instance instances = 3;
  uint64 index = 4;
}
```

### 9.3 Watch cache and resumption

```go
// A ring buffer of recent events, so a client that disconnects briefly can
// resume from its last index instead of re-fetching everything.
//
// This is exactly Kubernetes' watch cache, and it has exactly the same
// failure mode: if the client falls further behind than the buffer holds,
// its index has been COMPACTED AWAY and the server cannot serve it. The
// honest answer is to tell the client to re-list, not to silently skip
// events — skipping events means the client's view is permanently wrong.
type WatchCache struct {
    mu     sync.RWMutex
    events []Event    // ring buffer, default 1000
    oldest uint64
    newest uint64
}

var ErrIndexCompacted = errors.New("index too old; re-list required")

func (c *WatchCache) Since(index uint64) ([]Event, error) {
    if index < c.oldest {
        return nil, ErrIndexCompacted   // Kubernetes returns HTTP 410 Gone here
    }
    return c.eventsAfter(index), nil
}
```

### Thundering herd defences

```go
// 1. SINGLEFLIGHT — collapse identical concurrent resolves into one.
//    500 goroutines asking for the same service produce ONE store read.
var group singleflight.Group
result, err, _ := group.Do("svc:"+name, func() (any, error) {
    return h.store.GetNow(name)
})

// 2. BATCHED INDEX BUMPS — coalesce writes within a window into one
//    notification, so a 1,000-instance deploy wakes watchers once, not
//    1,000 times.
type IndexBatcher struct { window time.Duration /* default 50ms */ }

// 3. STAGGERED FAN-OUT — spread notifications to N watchers over a window
//    rather than firing all N in the same instant.
func (r *WatchRegistry) notify(service string, ev Event) {
    ws := r.watchers[service]
    spread := time.Duration(len(ws)) * 200 * time.Microsecond
    if spread > 500*time.Millisecond { spread = 500 * time.Millisecond }
    for i, w := range ws {
        delay := time.Duration(i) * spread / time.Duration(len(ws))
        time.AfterFunc(delay, func() { w.send(ev) })
    }
}
```

---

## §10 Client Resolution

### gRPC resolver + balancer

**This is where the gRPC-interceptors project plugs in directly.**

```go
// pkg/sdk/resolver.go
//
// Register a scheme so any gRPC client can use discovery with a URL:
//     conn, _ := grpc.NewClient("beacon:///payments?tag=v2&zone=us-east-1a")
type beaconResolverBuilder struct{ client *Client }

func (b *beaconResolverBuilder) Scheme() string { return "beacon" }

func (b *beaconResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn,
    opts resolver.BuildOptions) (resolver.Resolver, error) {

    r := &beaconResolver{cc: cc, service: target.Endpoint(), client: b.client}
    go r.watch()   // long-lived watch, pushes updates into the ClientConn
    return r, nil
}

func (r *beaconResolver) watch() {
    for {
        events, err := r.client.Watch(r.ctx, r.service, r.lastIndex)
        if err != nil {
            // BACKOFF WITH JITTER. When the control plane restarts, every
            // client reconnects at once; without jitter they arrive as one
            // wall and knock it over again.
            r.cc.ReportError(err)
            time.Sleep(backoffWithJitter(r.attempt))
            r.attempt++
            continue
        }
        r.attempt = 0
        addrs := make([]resolver.Address, 0, len(events.Instances))
        for _, inst := range events.Instances {
            if inst.Health != HealthPassing { continue }
            addrs = append(addrs, resolver.Address{
                Addr: net.JoinHostPort(inst.Address, strconv.Itoa(inst.Port)),
                // Weight and locality ride along as attributes and are read
                // by the picker. This is how the balancer learns about zones.
                Attributes: attributes.New(weightKey, inst.Weight).
                            WithValue(localityKey, inst.Locality),
            })
        }
        // ★ NEVER PUSH AN EMPTY ADDRESS LIST. ★
        //
        // If every instance fails its health check simultaneously — a bad
        // deploy, a shared dependency outage, a health-check bug — pushing
        // an empty list means the client has NOWHERE to send traffic and
        // fails 100% of requests.
        //
        // Keeping the last known good set means it fails some requests,
        // which is strictly better. Envoy calls this "panic mode" and
        // triggers it below 50% healthy: route to everything, healthy or
        // not, on the theory that the health data is more likely wrong than
        // the entire fleet.
        if len(addrs) == 0 {
            addrs = r.lastGoodAddrs
            r.reportPanicMode()
        } else {
            r.lastGoodAddrs = addrs
        }
        r.cc.UpdateState(resolver.State{Addresses: addrs})
    }
}
```

### DNS interface

```
dig @localhost -p 8600 payments.service.beacon        A
dig @localhost -p 8600 payments.service.beacon        SRV     ← includes port + weight
dig @localhost -p 8600 v2.payments.service.beacon     A       ← tag filter
dig @localhost -p 8600 payments.service.us-east.beacon A      ← datacenter

DNS is the universal fallback: it works from any language, any runtime, with
no library. Its limitations are real and worth stating:
  - no health metadata beyond "present or absent"
  - client-side caching honours TTL, so removal is delayed by the TTL
  - A records carry no port (SRV does, but most clients ignore SRV)
  - most resolvers use only the first record, defeating load balancing

We serve TTL=0 by default and document that many stub resolvers ignore it.
```

---

## §11 Load Balancing

```go
type Policy string
const (
    RoundRobin     Policy = "round_robin"
    WeightedRR     Policy = "weighted_round_robin"
    LeastRequest   Policy = "least_request"
    P2C            Policy = "p2c"           // power of two choices
    RingHash       Policy = "ring_hash"     // consistent hashing
    Maglev         Policy = "maglev"
)
```

### Power of two choices

```go
// Pick two endpoints at random; send to the one with fewer outstanding
// requests.
//
// The counterintuitive result: this is dramatically better than "pick the
// least loaded" and nearly as good as a global optimum, at O(1) cost with
// no coordination.
//
// "Pick the least loaded" is actually WORSE than random in a distributed
// setting: every client independently identifies the same least-loaded
// endpoint and stampedes it, so the least-loaded server instantly becomes
// the most-loaded. P2C's randomness breaks that synchronisation.
func (p *p2cPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
    n := len(p.subConns)
    if n == 0 { return balancer.PickResult{}, balancer.ErrNoSubConnAvailable }
    if n == 1 { return balancer.PickResult{SubConn: p.subConns[0].sc}, nil }

    a := p.rand.Intn(n)
    b := p.rand.Intn(n - 1)
    if b >= a { b++ }              // ensure b != a without a retry loop

    sa, sb := p.subConns[a], p.subConns[b]
    // Weighted comparison: inflight/weight, so a 2×-capacity host takes 2× load
    if float64(sa.inflight.Load())/float64(sa.weight) >
       float64(sb.inflight.Load())/float64(sb.weight) {
        sa = sb
    }
    sa.inflight.Add(1)
    return balancer.PickResult{
        SubConn: sa.sc,
        Done: func(di balancer.DoneInfo) {
            sa.inflight.Add(-1)
            // ← Feed the outcome into passive health checking.
            //   This is the hook your interceptors project fills.
            p.outlier.Record(sa.addr, di.Err)
        },
    }, nil
}
```

### Locality-aware routing

```go
// Prefer the local zone; spill over only when local capacity is degraded.
//
//   Priority 0: same zone   — all traffic while ≥ threshold healthy
//   Priority 1: same region — receives overflow
//   Priority 2: any         — last resort
//
// Saves cross-AZ bandwidth (real money) and cuts p99 latency, but the
// overflow calculation must be gradual or you get an oscillation: local zone
// degrades → all traffic shifts to remote → local recovers → all traffic
// shifts back → repeat.
func (l *LocalityPicker) healthyPercent(priority int) float64
func (l *LocalityPicker) overflowWeight(priority int) uint32 {
    // Envoy's formula: degrade gradually rather than switching at a cliff.
    return uint32(math.Min(100, l.healthyPercent(priority)*100/l.overprovision))
}
```

---

## §12 xDS Control Plane

The service-mesh half: `beacon` acts as an Envoy management server.

```go
// Four resource types, in a strict dependency order.
//   LDS — Listeners  (what ports do I open)
//   RDS — Routes     (given a request, which cluster)
//   CDS — Clusters   (a logical upstream + its policy)
//   EDS — Endpoints  (the actual addresses in a cluster)
//   SDS — Secrets    (mTLS certificates)
```

### ADS ordering — "make before break"

```go
// ADS multiplexes all resource types onto ONE gRPC stream, which lets the
// control plane enforce ORDERING. Ordering is not a nicety:
//
//   Send LDS referencing cluster "payments-v2" before CDS defines it, and
//   Envoy rejects the listener — or worse, accepts it and 503s every request
//   routed to the missing cluster.
//
// The safe order for ADDING config is bottom-up:
//     CDS → EDS → LDS → RDS
//   (define the cluster, populate it, then open the listener that uses it)
//
// For REMOVING, the order reverses: LDS → RDS → EDS → CDS. Stop sending
// traffic to it before you delete it.
//
// This is "make before break", and getting it backwards produces exactly the
// 503 spike during deploys that people blame on the application.
var addOrder    = []string{ClusterType, EndpointType, ListenerType, RouteType}
var removeOrder = []string{ListenerType, RouteType, EndpointType, ClusterType}
```

### Version, nonce, ACK/NACK

```go
// Every DiscoveryResponse carries (version_info, nonce). The client replies
// with a DiscoveryRequest:
//
//   ACK  → version_info = the version we just sent, error_detail empty
//   NACK → version_info = the PREVIOUS (still-applied) version, error_detail set
//
// THE NACK SEMANTICS ARE THE SUBTLE PART: a NACK does not mean "resend". It
// means "I rejected this and I am still running the old config". The control
// plane must NOT retry the same bad config in a loop — it must surface the
// error. A NACK loop is a real outage mode: the control plane hammers the
// proxy with config it has already refused, and neither side makes progress.
//
// Detect a NACK by the PRESENCE OF error_detail, not by comparing versions.
// Version comparison breaks for clients that dynamically change their
// subscription set.
func (s *Server) handleRequest(req *discovery.DiscoveryRequest, st *streamState) {
    if req.ErrorDetail != nil {
        s.metrics.NACKs.WithLabelValues(req.TypeUrl).Inc()
        s.events.Emit(XDSNack{
            Node: req.Node.Id, Type: req.TypeUrl,
            Version: req.VersionInfo, Error: req.ErrorDetail.Message,
        })
        st.nacked[req.TypeUrl] = req.ResponseNonce
        return   // ← do NOT resend. Surface it and wait for a new config.
    }
    if req.ResponseNonce != "" && req.ResponseNonce == st.lastNonce[req.TypeUrl] {
        st.acked[req.TypeUrl] = req.VersionInfo
    }
    // …
}
```

### SotW vs Delta

```
STATE OF THE WORLD (SotW) — the original
  Client subscribing to 100 clusters must send all 100 names on every request.
  Server must send all 100 resources on every response, even if 1 changed.
  Simple. Bandwidth scales with TOTAL config on EVERY change.

DELTA / INCREMENTAL xDS
  Client sends only subscribe/unsubscribe deltas.
  Server sends only changed resources, plus removed_resources.
  Bandwidth scales with the SIZE OF THE CHANGE.

The difference is not marginal at scale. 5,000 endpoints in a cluster and one
pod restarts: SotW pushes all 5,000 to every proxy; Delta pushes one. With
1,000 proxies that is 5,000,000 endpoint records versus 1,000.

Both are implemented, and the console graphs the bytes-pushed difference
side by side — it is the clearest possible argument for Delta.
```

---

## §13 Service Mesh & Identity

```go
// SPIFFE identity: every workload gets a verifiable identity, independent of
// its IP. IPs are recycled within seconds in a container fleet, so
// IP-based authorization is authorizing "whatever happens to be at
// 10.0.3.17 right now" — which is not a security property.
//
//   spiffe://beacon.local/ns/production/sa/payments
type Identity struct {
    TrustDomain string
    Namespace   string
    ServiceAccount string
}
func (i Identity) URI() string  // the SAN in the certificate

// SDS: the control plane issues short-lived certs (default 24h, rotated at
// 50%) over the same xDS stream. Short lifetimes mean revocation is mostly
// unnecessary — a compromised cert expires before a CRL would propagate.
type CertificateAuthority interface {
    Sign(ctx context.Context, csr []byte, id Identity) (*Certificate, error)
    Rotate(ctx context.Context, id Identity) (*Certificate, error)
    Bundle(ctx context.Context) ([]byte, error)
}

// Intentions: L4 authorization by identity, not by IP.
type Intention struct {
    Source      string   // "web" or "*"
    Destination string   // "payments"
    Action      Action   // Allow | Deny
    Precedence  int      // more specific wins
}
```

---

## §14 Failure Modes

### The stale-endpoint window — the number that actually matters

```
An instance dies. How long until clients stop sending it traffic?

  t0  instance crashes
  t1  DETECTION
        health check:  interval × failures_before_critical
                       (5s × 3 = 15s)
        gossip:        ~2 SWIM protocol periods (~2s)   ← 7× faster
  t2  PROPAGATION to the catalog
        agent-local check → catalog write: ~50ms
        gossip delta: O(log N) rounds, ~1-3s
  t3  NOTIFICATION to watchers
        streaming watch: ~10ms
        blocking query:  up to the wait timeout
        DNS:             up to the record TTL          ← the slow one
  t4  CLIENT applies the update
        gRPC resolver: immediate
        connection-pooled HTTP: until the pooled conn is retired

  TOTAL: 2s (gossip + streaming) … 60s+ (health check + DNS TTL)

★ THE HEADLINE RESULT OF THIS PROJECT: measure this end-to-end, per
  configuration, and show it on a chart. The 30× spread between the fast and
  slow paths is the entire engineering argument for gossip-driven propagation
  and streaming watches, and almost nobody measures it.
```

### Split brain

```
A network partition splits the gossip pool.

AP mode:  each side converges on its own view. Side A sees 5 instances of
          `payments`, side B sees 3. BOTH SERVE. On heal, incarnation numbers
          reconcile and views merge. Registrations made on either side survive.

CP mode:  the minority side cannot write. Instances starting there are
          invisible until heal. Linearizable reads fail. Stale reads work.

The console shows both, live, with a partition toggle.
```

### Registration storms and health flapping

Covered in §5 and §6. Both are simulated, and both are visible in the console as control-plane load spikes.

---

## §15 Observability

```go
type Event struct {
    Kind      EventKind
    Timestamp time.Time
    TraceID   string   // ★ threads register → propagate → notify → resolve
    // …
}

const (
    // Registration
    EvInstanceRegistered   EventKind = "instance.registered"
    EvInstanceDeregistered EventKind = "instance.deregistered"
    EvLeaseRenewed         EventKind = "lease.renewed"
    EvLeaseExpired         EventKind = "lease.expired"

    // Health
    EvCheckExecuted        EventKind = "check.executed"
    EvHealthChanged        EventKind = "health.changed"
    EvFlappingDetected     EventKind = "health.flapping"
    EvOutlierEjected       EventKind = "outlier.ejected"
    EvOutlierReturned      EventKind = "outlier.returned"
    EvPanicModeEntered     EventKind = "lb.panic_mode"

    // Propagation
    EvGossipDelta          EventKind = "gossip.delta"
    EvAntiEntropySync      EventKind = "antientropy.sync"
    EvConverged            EventKind = "propagation.converged"   // ← with elapsed

    // Watch
    EvWatchOpened          EventKind = "watch.opened"
    EvWatchNotified        EventKind = "watch.notified"
    EvWatchCompacted       EventKind = "watch.compacted"
    EvHerdDetected         EventKind = "watch.herd"

    // xDS
    EvXDSPush              EventKind = "xds.push"
    EvXDSAck               EventKind = "xds.ack"
    EvXDSNack              EventKind = "xds.nack"      // ← always interesting

    // Resolution
    EvResolveRequest       EventKind = "resolve.request"
    EvStaleEndpointUsed    EventKind = "resolve.stale"  // ← the money event
)
```

**The propagation trace is the central artifact.** One trace ID follows a single registration from the SDK call, through the agent, into the catalog, across gossip to every server, out through every watch stream, and into each client's address list — with a timestamp at every hop. That trace is what makes the stale-endpoint window measurable instead of theoretical.

---

## §16 Frontend

Stack: `react` · `vite` · `typescript` · `tailwindcss` · `shadcn/ui` · `d3` · `recharts` · `zustand` · SSE

### View 1 · Mesh Topology ⭐

D3 force graph. Services as clustered nodes, instances as satellites, edges as observed call relationships (learned from client SDK reports).

- Instance color: passing green / warning amber / critical red / ejected gray / draining blue
- Instance size ∝ weight; ring thickness ∝ current inflight requests
- Edge thickness ∝ RPS, color by error rate
- Zone boundaries as translucent regions — **cross-zone traffic is immediately visible**, which is the thing nobody notices until the bandwidth bill arrives
- Click a service → the instance table with per-instance health-check history

### View 2 · Propagation Timeline ⭐ — the flagship

Register or kill an instance and watch the change travel:

```
  t+0ms     ┃ SDK Register("payments-7")
  t+3ms     ┃ agent-01 local state updated
  t+8ms     ┃ agent-01 → catalog write, index 4821
  t+11ms    ┃ ████ server-1 has it
  t+340ms   ┃ ████████ gossip round 1 → server-2
  t+680ms   ┃ ████████████ gossip round 2 → server-3
  t+692ms   ┃ watch stream → 47 clients notified
  t+710ms   ┃ ████████████████ client-12 address list updated
  t+1.2s    ┃ ████████████████████ DNS TTL expiry → dig sees it
            ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
              CONVERGED at t+1.2s  (all 47 clients + DNS)
```

A horizontal swimlane per component, one bar per hop, with the **convergence time called out**. Run the same experiment with gossip off, streaming off, or DNS-only and compare — the spread between 2 seconds and 60 seconds is the entire thesis of the project, made visible.

### View 3 · Health Check Inspector

Per-instance timeline of check results: pass/fail, latency, response body snippet. Hysteresis state machine drawn explicitly (`passing → 1 fail → 2 fails → 3 fails → critical`), so you can see *why* an instance did or did not get ejected. Flapping detection highlighted. Active vs passive (outlier) shown on the same timeline — the case where active passes and passive ejects is the interesting one.

### View 4 · Watch Stream Inspector

Every open watcher: who, which service, at what index, how long connected, how many events received, bytes pushed. **Herd detection** — a histogram of notification timestamps, with a red flag when N watchers are notified within the same 10 ms bucket. Watch-cache occupancy and compaction events.

### View 5 · xDS Console

Per-proxy resource state: LDS/CDS/EDS/RDS with current version, last ACK, and any NACK with its error message prominently displayed. Push timeline showing the ADS ordering (CDS→EDS→LDS→RDS) so out-of-order pushes are visible. **SotW vs Delta bytes-pushed comparison chart** — the same config change under both protocols, side by side.

### View 6 · Consistency Lab ⭐

AP and CP running the same workload side by side. A partition toggle. Watch:
- AP: both sides accept registrations, views diverge, then converge on heal
- CP: minority side rejects writes, linearizable reads fail, stale reads succeed

A live **divergence counter** — how many instances one side sees that the other does not — going up during the partition and returning to zero after heal.

### View 7 · Load Balancing Lab

Live request distribution across instances under each policy. Inject a slow instance and watch P2C route around it while round-robin does not. Show the "least loaded" stampede failure. Locality routing with a zone failure, showing gradual overflow rather than a cliff.

---

## §17 CLI

```
beacon agent   --node n1 --join seed:7946 --data-dir ./data
beacon server  --bootstrap-expect 3 --consistency ap|cp

beacon register   --name payments --port 8080 --tag v2 --zone us-east-1a \
                  --check http://localhost:8080/health --interval 5s --ttl 30s
beacon deregister --id payments-7
beacon maint      --id payments-7 --enable --reason "deploying"

beacon services                       # catalog listing
beacon instances payments [--tag v2] [--passing]
beacon health payments                # per-check detail
beacon watch payments                 # stream changes to stdout
beacon resolve payments --policy p2c --count 1000   # distribution histogram

beacon members                        # gossip pool (from your SWIM project)
beacon xds status [--node envoy-1]
beacon intentions list | create | delete

# The measurement commands
beacon bench propagate --kill payments-3   # measures the stale-endpoint window
beacon bench herd --watchers 1000          # thundering-herd behaviour
beacon sim partition --split a,b|c         # split brain, AP vs CP
beacon sim storm --instances 1000          # registration storm
```

---

## §18 File Structure

```
beacon/
├── cmd/
│   ├── beacon/               # CLI
│   ├── beacon-agent/         # node agent
│   └── beacon-server/        # control plane
├── pkg/
│   ├── catalog/              # types, in-memory store, indexing, MVCC
│   ├── store/
│   │   ├── gossip/           # AP backend  ← your SWIM project
│   │   └── raft/             # CP backend  ← your consensus project
│   ├── health/
│   │   ├── check/            # http, tcp, grpc, exec, ttl, alias
│   │   ├── hysteresis.go     # flapping suppression
│   │   └── outlier/          # passive detection
│   ├── agent/                # local state, anti-entropy, check runner
│   ├── watch/                # registry, cache, blocking query, fan-out
│   ├── api/
│   │   ├── http/             # REST + blocking queries
│   │   ├── grpc/             # streaming watch  ← interceptors project
│   │   └── dns/              # A / AAAA / SRV / TXT
│   ├── xds/                  # ADS server, SotW + Delta, snapshot cache
│   ├── mesh/                 # identity, CA, SDS, intentions
│   ├── sdk/                  # resolver, balancer, interceptors, client
│   ├── lb/                   # rr, wrr, least_request, p2c, ring_hash, locality
│   ├── sim/                  # partition, storm, flap, herd scenarios
│   └── events/               # event bus, SSE, trace propagation
├── console/                  # React app
├── proto/
└── test/
    ├── integration/
    ├── scenario/
    └── bench/
```

---

## §19 Correctness Properties

1. **Registration durability** — an acknowledged registration is visible to a subsequent read on the same server, and to all servers within the convergence bound.
2. **Deregistration completeness** — an explicitly deregistered instance is never returned to any client after convergence.
3. **Lease expiry** — an instance whose lease expires is marked critical within `ttl + grace`, and removed after `deregister_after`.
4. **Health aggregation** — an instance's status is the worst of its checks, always.
5. **Hysteresis** — no instance transitions health state without the configured consecutive count. A flapping instance produces at most one transition per `interval × threshold`.
6. **Monotonic index** — the catalog index never decreases, and every mutation increases it.
7. **Watch completeness** — a watcher at index `N` receives every event with index `> N`, or an explicit `ErrIndexCompacted`. Events are never silently skipped.
8. **Watch liveness** — a blocking query returns within `wait + jitter`, always.
9. **Anti-entropy convergence** — the catalog converges to the agents' local state within one sync interval, even after total catalog loss.
10. **Gossip convergence** — a catalog delta reaches all live nodes within `O(log N)` rounds.
11. **AP availability** — in AP mode, every partition side accepts registrations and serves reads.
12. **CP safety** — in CP mode, no two servers ever return conflicting linearizable results.
13. **Never-empty resolution** — the resolver never pushes an empty address list; it retains the last known good set and reports panic mode.
14. **Ejection bound** — outlier detection never ejects more than `MaxEjectionPercent` of a pool.
15. **xDS ordering** — CDS precedes EDS, and LDS precedes RDS, on every push.
16. **NACK non-amplification** — a NACKed config is never resent unchanged.
17. **Identity binding** — a certificate is issued only for the identity the requesting workload is entitled to.

---

## §20 Performance Targets

| Metric | Target |
|---|---|
| Registration → visible on the local server | < 10 ms |
| **Registration → converged across 100 nodes (gossip)** | **< 2 s** |
| **Instance death → removed from all clients (gossip + stream)** | **< 3 s p99** |
| Instance death → removed (health check + DNS TTL) | ~45–60 s *(the contrast)* |
| Catalog read, 10k instances, warm | < 1 ms |
| Blocking query wake → client notified | < 20 ms |
| Concurrent watch streams per server | > 10,000 |
| Memory per idle watch stream | < 8 KB |
| Registration throughput | > 5,000/s per server |
| Health checks per agent | > 500 concurrent, < 5 % CPU |
| DNS query p99 | < 2 ms |
| xDS push → proxy ACK (1k endpoints) | < 100 ms |
| **Delta xDS bytes vs SotW (1 endpoint change, 5k cluster)** | **> 1000× less** |
| Resolver pick latency (P2C) | < 200 ns |
| Catalog memory, 10k services × 10 instances | < 400 MB |
| Gossip bandwidth per node, 1k nodes | < 50 KB/s |
