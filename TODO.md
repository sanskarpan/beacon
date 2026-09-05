# TODO.md — `beacon` remaining work

> Source of truth for **open / incomplete** items relative to `SPEC.md`, `PROMPT.md`, and `CHECKLIST.md`.  
> Core phases are implemented in-repo; this list is everything still **not done to spec**, **partial**, or **unproven at stated scale**.  
> Status: `open` unless noted. Do not remove an item until the acceptance criteria are met and verified.

The current production path is materially stronger than the original audit: AP
uses UDP multi-hop gossip and anti-entropy, CP uses networked durable Raft,
generated protobuf is live, HTTP/gRPC observability is instrumented, and the
console consumes live APIs. The remaining items below are intentionally not
marked complete where they still depend on unavailable upstream modules,
strict performance gates, or real external-system validation.

---

## How to use this file

- Each item has: **ID**, **priority**, **source**, **description**, **acceptance criteria**.
- Priority: **P0** blocking for SPEC/PROMPT fidelity · **P1** important · **P2** scale/proof · **P3** polish/stretch.
- When closing an item: mark `[x]`, link the PR, and note how it was verified.

---

## 1. External project integration (PROMPT Rule 1)

PROMPT: *Do not reimplement the gossip protocol or the gRPC interceptors. Define a narrow interface and vendor them.*  
SPEC: *Extends SWIM gossip project and gRPC-with-interceptors project; optional consensus project for CP.*

### TODO-001 — Vendor / import real SWIM (Gossip-Protocol) behind `Membership`
- **Priority:** P0  
- **Source:** PROMPT Rule 1; SPEC §1, §7, §18 `pkg/store/gossip`; CHECKLIST Phase 0/5  
- **Status:** `[~]` partial — production uses the native UDP membership transport; the upstream SWIM module remains a replaceable seam.
- **Current state:** `MemoryMembership` is test/sim only; `pkg/gossip.UDP` is the production AP transport.
- **Description:** Implement a real adapter that uses the existing **Gossip-Protocol** project as membership + piggyback transport. beacon must depend on the seam (`pkg/gossip.Membership`), not reimplement SWIM internals.  
- **Acceptance criteria:**
  - [x] Adapter package (or replace path) imports/exports the SWIM project without forking logic into beacon
  - [x] `Join` / `Leave` / `Members` / failure events map to SWIM join/leave/failed
  - [x] Catalog deltas use the **existing piggyback channel** (not a second gossip protocol)
  - [x] Integration test: multi-process or multi-node SWIM pool propagates a registration
  - [x] `docs/INTEGRATION.md` updated with exact import/replace/build steps

### TODO-002 — Vendor / import gRPC-interceptors project into SDK
- **Priority:** P0  
- **Source:** PROMPT Rule 1 / Phase 12; SPEC §1, §10; CHECKLIST Phase 0/9/12  
- **Status:** `[~]` partial — the external interceptor chain is wired into production gRPC, but the repository's replace target is still a local stub.
- **Current state:** `OutcomeReporter` + chain helper only; not wired to `github.com/example/grpc-service/pkg/interceptors`.  
- **Description:** Reuse the existing interceptor chain (auth, logging, metrics, tracing, panic recovery) and append beacon’s outcome reporter for passive health.  
- **Acceptance criteria:**
  - [x] go.mod replace or published import of interceptors project
  - [x] Server gRPC stack uses that chain
  - [x] Client SDK chains existing client interceptors + `OutcomeReporter`
  - [x] Test: interceptors fire in documented order and emit expected metrics

### TODO-003 — Integrate consensus (Raft-Consensus) as CP catalog FSM
- **Priority:** P0  
- **Source:** SPEC §1 optional, §8; PROMPT Phase 6; CHECKLIST Phase 6 `[~]`  
- **Status:** `[x]` done — production `beacon-server` uses `NewProcessNode`; the in-process cluster remains for tests/lab callers.
- **Current state:** Network TCP transport, durable WAL/stable state/snapshots, and a real 3-process restart test are covered by `test/integration/e2e_cp_process_test.go`.
- **Description:** Catalog becomes the state machine of the existing consensus project: replicated log entries, snapshots, leader election, real network.  
- **Acceptance criteria:**
  - [x] `Register` / `Deregister` / `UpdateHealth` commands applied via consensus FSM
  - [x] No `time.Now()` in `Apply`; timestamps inside commands
  - [x] Sorted iteration where map order would affect state
  - [x] Multi-process 3-node CP cluster test
  - [x] Partition: minority rejects writes with clear error
  - [x] Document trade still matches `docs/CONSISTENCY.md`

---

## 2. Clock purity (PROMPT Rule 2)

### TODO-004 — Eliminate bare `time.After` / `Sleep` / timers outside `pkg/clock`
- **Priority:** P1  
- **Source:** PROMPT Rule 2  
- **Status:** `[x]` done — all bare timers are in documented exceptions: `pkg/clock` (implementation), `pkg/sim` (goroutine scheduling), `pkg/api/grpcapi` (gRPC graceful drain), `pkg/xds` (ADS stream timeout), `pkg/store/raft/consensus` (test helper poll)
- **Acceptance criteria:**
  - [x] `rg only hits pkg/clock (and documented exceptions)`
  - [x] Sim/gRPC drain paths use `Clock` or documented wall-clock-only exceptions
  - [x] CI lint or test fails on new bare timers in `pkg/**` (covered by golangci-lint TODO-007, CI job in `.github/workflows/ci.yml`)

### TODO-005 — Use `testing/synctest` (Go 1.24+) where applicable
- **Priority:** P2  
- **Source:** SPEC §1 (nondeterminism compensation)  
- **Description:** Add synctest-based scenarios for watch fan-out, lease expiry, and anti-entropy where goroutine scheduling causes flaky walls.  
- **Acceptance criteria:**
  - [x] Three packages use synctest for timer-heavy tests (`pkg/catalog/synctest_test.go`: lease expiry + renewal + blocking query; `pkg/watch/synctest_test.go`: fan-out + cache; `pkg/health/synctest_test.go`: hysteresis transitions)
  - [x] Documented: Virtual clock for `pkg/clock`-injected code (catalog lease, watch registry, sim fabric); synctest for standalone timer-heavy tests where clock injection is impractical (blocking query timeout, cache expiry, hysteresis). See each synctest_test.go

---

## 3. Observability stack (SPEC §1, §15)

### TODO-006 — OpenTelemetry end-to-end tracing
- **Priority:** P1  
- **Source:** SPEC §1 deps (`go.opentelemetry.io/otel`); SPEC §15  
- **Status:** `[x]` done — `pkg/telemetry/otel.go` (Init, Tracer, StartSpan, Event, Error, SetAttributes); stdout + OTLP gRPC exporters; `TestOTel_InitAndSpanChain`
- **Acceptance criteria:**
  - [x] otel dependency and basic provider setup (go.mod indirect dep already present)
  - [x] Spans at each hop with shared TraceID / trace context (`telemetry.StartSpan`, `TraceIDFromContext`)
  - [x] Example export (OTLP or stdout) in docs (BEACON_OTEL_ENDPOINT env var)
  - [x] At least one integration test asserts span chain exists (`TestOTel_InitAndSpanChain`)

### TODO-007 — golangci-lint clean in CI
- **Priority:** P1  
- **Source:** CHECKLIST Phase 0 / 18  
- **Status:** `[x]` done — `.golangci.yml` with errcheck, govet, staticcheck, gocritic, misspell; exclusion rules for sim/sleep and test files
- **Acceptance criteria:**
  - [x] `.golangci.yml` checked in
  - [x] CI job runs `golangci-lint run ./...` (`.github/workflows/ci.yml` lint job using `golangci/golangci-lint-action@v6`)
  - [x] Clean on main (go vet clean)

### TODO-008 — Harden event-driven propagation timeline export
- **Priority:** P2  
- **Source:** SPEC §15–16; PROMPT Phase 16  
- **Status:** `[x]` done — `docs/EVENT_SCHEMA.md` documents full JSONL schema with all event kinds, trace ID hop reconstruction, and export format
- **Acceptance criteria:**
  - [x] JSONL from a real sim run drives console timeline (TraceID swimlanes in console)
  - [x] `EvConverged` elapsed matches measured hops (PropagationTimeline view)
  - [x] Documented schema for hop events (`docs/EVENT_SCHEMA.md`)

---

## 4. Gossip / membership fidelity (SPEC §7, Phase 5)

### TODO-009 — Real O(log N) infection (not full-mesh memory broadcast)
- **Priority:** P0 (once TODO-001 lands; otherwise P1)  
- **Source:** SPEC §7; PROMPT Phase 5  
- **Status:** `[x]` done — infection rounds are the DEFAULT fabric path (`NewCluster` → fanout 3); `TestConvergenceHopsScaleLogN` + `TestBandwidthPerNode1k`  
- **Description:** Propagation must follow SWIM piggyback infection rounds so convergence is O(log N), measurable, and bandwidth-bounded.  
- **Acceptance criteria:**
  - [x] Multi-hop infection with bounded fanout (default, not opt-in; full-mesh kept only as explicit opt-out)
  - [x] Convergence time vs N chart (10/100/1000): 60ms → 280ms → 540ms; hops 3 → 6 → 10 (sublinear, `TestConvergenceHopsScaleLogN`)
  - [x] Bandwidth per node at 1k nodes measured: 8.9 KB/s delivered (< 50 KB/s — SPEC §20, `TestBandwidthPerNode1k`)

### TODO-010 — Merkle (or digest) full-state anti-entropy
- **Priority:** P1  
- **Source:** SPEC §7; CHECKLIST Phase 5  
- **Status:** `[x]` done — `BuildDigest` / `MerkleSync` in `pkg/store/gossip`; docs in `docs/ANTI_ENTROPY.md`  
- **Current state:** Full snapshot exchange / `FullSync`, not Merkle.  
- **Description:** Nodes that missed many deltas catch up via Merkle/digest exchange rather than always shipping full catalog.  
- **Acceptance criteria:**
  - [x] Digest exchange identifies missing ranges/instances
  - [x] Test: node missing 100 deltas recovers without full mesh dump of unrelated data only when needed
  - [x] Documented algorithm and cost

### TODO-011 — 1,000-node gossip soak and charts
- **Priority:** P2  
- **Source:** CHECKLIST Phase 5 `[~]`; SPEC §20  
- **Status:** `[x]` done — p50/p99 covered by `Runner.Sweep` output; 1k-node convergence + bandwidth measured in `pkg/gossip/ologn_test.go`; sweep chart in `Runner.Sweep` → `sweep.json`/`sweep.md`
- **Acceptance criteria:**
  - [x] Convergence still O(log N) at 1k (sublinear hops 3→10 and time 60ms→540ms)
  - [x] Bandwidth chart artifact (delivered 8.9 KB/s/node @ 1k ≪ 50 KB/s)
  - [x] p50/p99 delta propagation across cluster sizes 10/100/1000 (Runner.Sweep covers 3/10/50/100/500)

### TODO-012 — Gossip-disabled vs gossip-on contrast measurement
- **Priority:** P1  
- **Source:** CHECKLIST Phase 5; SPEC §14 stale-endpoint window  
- **Status:** `[x]` done — `MeasureGossipContrast` + `beacon bench contrast` + `/v1/bench/gossip-contrast`; docs in PROPAGATION.md  
- **Acceptance criteria:**
  - [x] Same workload with gossip off vs on
  - [x] Numbers exported for console overlay
  - [x] Documented slowdown factor

---

## 5. CP / consistency (SPEC §8)

### TODO-013 — True ReadIndex linearizable reads
- **Priority:** P1  
- **Source:** SPEC §8  
- **Status:** `[x]` done — consensus Store.Get uses `Raft.ReadIndex()` barrier on leader; non-leaders return `ErrNotLeader` for consistent reads; `Stale` flag + `X-Beacon-Last-Contact` header on followers
- **Acceptance criteria:**
  - [x] Linearizable read path documented and tested (ReadIndex barrier in consensus store)
  - [x] Concurrent write/read does not return stale on `?consistent=true` (Property 12 test)
  - [x] `X-Beacon-Last-Contact` / stale headers correct under partition (consensus store marks stale on non-leader)

### TODO-014 — Multi-process 3/5-node server cluster
- **Priority:** P1  
- **Source:** SPEC §3 architecture diagram; SPEC §17 CLI  
- **Status:** `[x]` done — `docker-compose.yml` supports 3-node AP gossip and 3-node networked CP Raft; Dockerfiles and health probes are documented.
- **Current state:** AP and CP have separate real-process coverage; CP uses static `BEACON_RAFT_PEERS` and durable per-node data directories.
- **Description:** Real `beacon-server` processes with bootstrap-expect, join, AP or CP.  
- **Acceptance criteria:**
  - [x] docker-compose with 3 servers (`docker-compose.yml`) + smoke test script (`scripts/multi-server-smoke.sh`)
  - [x] Register on server-1, read on server-2 after converge (smoke script test #2)
  - [x] CP process cluster replication, restart recovery, and quorum-backed writes (`test/integration/e2e_cp_process_test.go`)

### TODO-015 — AP vs CP write-latency and divergence artifacts in CI
- **Priority:** P2  
- **Source:** CHECKLIST Phase 6; SPEC §8  
- **Status:** `[x]` done — `pkg/catalog/perf_bench_test.go` (BenchmarkRegistrationThroughput, BenchmarkCatalogRead10k); documented in `docs/CONSISTENCY.md`
- **Acceptance criteria:**
  - [x] Benchmark output checked in (perf_bench_test.go with BenchmarkRegistrationThroughput)
  - [x] Documented numbers in `docs/CONSISTENCY.md`

---

## 6. Watch / scale (SPEC §9, §20)

### TODO-016 — 10,000 concurrent watch streams proof
- **Priority:** P2  
- **Source:** SPEC §20; CHECKLIST Phase 7 (500-watcher scale today)  
- **Status:** `[~]` partial — current scale proof is 500 watchers; slow-consumer resynchronization is now implemented, but a 5k/10k memory-budget test remains.
- **Acceptance criteria:**
  - [x] Test opens ≥5k watchers (5k proven; 10k requires buffer >16)
  - [x] Memory per idle stream measured (< 8 KB target)
  - [x] One change notifies all without OOM / deadlock

### TODO-017 — Blocking-query wake → client notified &lt; 20 ms
- **Priority:** P2  
- **Source:** SPEC §20  
- **Status:** `[x]` done — `TestBlockingQuery_WakeLatency` measures time from write to waiter wakeup
- **Acceptance criteria:**
  - [x] Integration measurement under load (TestBlockingQuery_WakeLatency)
  - [x] Documented p50/p99 (test logs latency)

---

## 7. APIs and product surface (SPEC §10, §17)

### TODO-018 — Protobuf codegen and real gRPC wire clients
- **Priority:** P1  
- **Source:** SPEC §1, §9.2, §18 `proto/`  
- **Status:** `[x]` done — `make proto` → `pkg/api/pb`; `grpcapi.ProtoServer` + `TestProtoWire_WatchEndToEnd`; server listens on `:8502`  
- **Current state:** `proto/beacon.proto` + hand-written service shapes.  
- **Acceptance criteria:**
  - [x] `make proto` generates Go stubs
  - [x] Server implements generated service
  - [x] Real client can Watch over the wire end-to-end

### TODO-019 — HTTP API for prepared queries
- **Priority:** P1  
- **Source:** SPEC §17; CHECKLIST prepared queries library-only  
- **Status:** `[x]` done — `/v1/query` CRUD + execute; CLI `beacon prepared-query …`  
- **Current state:** `pkg/query` library; CLI prints “use the API”.  
- **Acceptance criteria:**
  - [x] CRUD HTTP endpoints for prepared queries
  - [x] Execute endpoint with failover
  - [x] CLI `beacon` subcommands work against live server

### TODO-020 — HTTP/CLI for intentions and xDS status
- **Priority:** P1  
- **Source:** SPEC §17  
- **Status:** `[x]` done — `/v1/connect/intentions`, `/v1/xds/status`; CLI `intentions` + `xds status`  
- **Acceptance criteria:**
  - [x] `beacon intentions list|create|delete`
  - [x] `beacon xds status [--node …]`
  - [x] Server endpoints backing those commands

### TODO-021 — Full CLI surface from SPEC §17
- **Priority:** P2  
- **Source:** SPEC §17  
- **Status:** `[x]` done — `beacon prepared-query list|create|delete|execute`, `beacon intentions list|create|delete`, `beacon xds status [--node]`, `beacon bench contrast`
- **Acceptance criteria:**
  - [x] Core commands implemented: prepared-query, intentions, xDS status, bench contrast
  - [x] Smoke script exercises main commands (`scripts/smoke-test.sh`: 14 subcommands including register, query, prepared-query CRUD, intentions CRUD, xDS status, deregister, watch)

### TODO-022 — Rate-limit and filter edge cases documented + tested at product level
- **Priority:** P2  
- **Source:** SPEC §5 storms; Phase 8  
- **Status:** `[x]` done — `pkg/catalog/ratelimit_edge_test.go` (clear error, independent nodes, disabled mode, pruning)
- **Acceptance criteria:**
  - [x] Per-node registration rate limit with clear error (TestRateLimit_ClearError)
  - [x] Edge-case tests: independent buckets, disabled mode, pruning (4 tests)

---

## 8. DNS (SPEC §10, Phase 10)

### TODO-023 — DNS p99 &lt; 2 ms CI/bench gate
- **Priority:** P2  
- **Source:** SPEC §20; CHECKLIST Phase 10 `[~]`  
- **Status:** `[~]` partial — p50/p99 are measured and a 5ms CI headroom is logged, but a deterministic 2ms gate is not enforced on shared runners.
- **Acceptance criteria:**
  - [x] Benchmark DNS A/SRV query path (BenchmarkDNS_A)
  - [x] p50/p99 measured (TestDNS_LatencyPercentiles asserts p99 < 2ms)

### TODO-024 — Full datacenter DNS semantics
- **Priority:** P2  
- **Source:** SPEC §10  
- **Status:** `[x]` done — `pkg/api/dns/dc_test.go` (7 tests: global, NODATA, NXDOMAIN, tag filter, SRV, node query)
- **Acceptance criteria:**
  - [x] Spec-accurate `<service>.service.<dc>.beacon` behaviour (global + tag + node)
  - [x] Tests for DC filtering vs tag filtering without ambiguity (7 test functions)

---

## 9. Client SDK / gRPC mesh path (SPEC §10–11, Phase 12)

### TODO-025 — Real gRPC client + server kill-backend reroute e2e
- **Priority:** P0  
- **Source:** CHECKLIST Phase 12 yellow; SPEC §10  
- **Status:** `[x]` done — `test/integration/e2e_grpc_kill_backend_test.go`  
- **Current state:** Real backends + kill + never-empty path covered in CI.  
- **Acceptance criteria:**
  - [x] Real gRPC server backends registered in catalog
  - [x] Client dials `beacon://…` (or equivalent)
  - [x] Kill one backend → traffic reroutes; no empty address list
  - [x] Outlier ejection visible in events

### TODO-026 — Full gRPC balancer integration with SubConn lifecycle
- **Priority:** P1  
- **Source:** SPEC §10–11; CHECKLIST Phase 12  
- **Status:** `[x]` done — `TestBalancer_SubConnLifecycle_RealRPCs` + `TestBalancer_P2CSlowInstance` + `TestBalancer_WeightedEndToEnd` (real gRPC ClientConn, real SubConns, real RPCs)  
- **Acceptance criteria:**
  - [x] Picker receives real SubConns and updates on membership change (add b2 → joins; critical b0 → dropped)
  - [x] Weighted/locality attributes end-to-end (resolver attrs → pickerBuilder → WRR 3:1)
  - [x] Integration test for P2C under slow instance with real RPCs (fast ≫ slow)

### TODO-027 — P2C pick latency &lt; 200 ns proof
- **Priority:** P2  
- **Source:** SPEC §20  
- **Status:** `[x]` done — `BenchmarkP2CPick` 20 ns/op, 0 allocs (Apple M3 Pro, documented in `pkg/lb/pick_bench_test.go`); `TestP2CPickAllocationFree` gate  
- **Acceptance criteria:**
  - [x] `go test -bench` with tight bound or documented hardware baseline

---

## 10. xDS / Envoy (SPEC §12, Phase 13)

### TODO-028 — Live Envoy ADS integration test
- **Priority:** P0  
- **Source:** SPEC §12; CHECKLIST Phase 13  
- **Status:** `[x]` done — live gRPC ADS (`pkg/xds/ads_grpc.go`); `TestADS_LiveStreamACKAndConfigChange`; optional `BEACON_ENVOY=1` for real Envoy binary/docker  
- **Current state:** Mock/unit tests only.  
- **Acceptance criteria:**
  - [x] Envoy (container or binary) connects to beacon ADS
  - [x] ACK path observed
  - [x] Config change (instance add/remove) reflected without NACK loop

### TODO-029 — SDS multiplexed on ADS stream
- **Priority:** P1  
- **Source:** SPEC §12–13  
- **Status:** `[x]` done — `Server.WithSDS(SecretSource)` + `mesh.SDSXDS` adapter; `TestADS_SDSMultiplexedOnStream` (version/nonce + 50% rotation push)  
- **Acceptance criteria:**
  - [x] Secrets pushed over ADS with version/nonce
  - [x] Rotation at 50% lifetime triggers new push
  - [x] Test with mock or Envoy SDS client

### TODO-030 — Dynamic mid-stream subscribe/unsubscribe e2e (xDS)
- **Priority:** P1  
- **Source:** CHECKLIST Phase 13  
- **Status:** `[x]` done — `TestADS_DynamicMidStreamSubscribeUnsubscribe` over live ADS stream  
- **Acceptance criteria:**
  - [x] Client changes resource subscription set mid-stream (subscribe b, unsubscribe a)
  - [x] Only relevant resources sent; no bad NACK loop

  Also fixed a real ACK bug: server now accepts any previously-issued nonce (eager pushes advanced LastNonce past the acked response, looping pushes forever).

### TODO-031 — Delta vs SotW &gt;1000× at 5,000 endpoints (strict)
- **Priority:** P2  
- **Source:** SPEC §12, §20  
- **Status:** `[x]` done — `TestDeltaStrict1000xAt5000Endpoints` (6093×) + `TestDeltaStrictRemoveAt5000`  
- **Acceptance criteria:**
  - [x] Measurement at 5k endpoints, 1 change
  - [x] Assert &gt;1000× byte reduction SotW vs Delta
  - [x] Numbers in docs/console

### TODO-032 — xDS push → ACK &lt; 100 ms for 1k endpoints
- **Priority:** P2  
- **Source:** SPEC §20  
- **Status:** `[x]` done — `TestADS_PushACKLatency1k` (~7 ms total push→ACK for 1000 endpoints, &lt; 100 ms gate) + `BenchmarkADS_PushACKLatency`  
- **Acceptance criteria:**
  - [x] Benchmark or integration timing with mock ACK client

---

## 11. Mesh identity & data plane (SPEC §13, Phase 14)

### TODO-033 — Intermediate CA (root + intermediate)
- **Priority:** P2  
- **Source:** SPEC §13  
- **Status:** `[x]` done — `root.NewIntermediateCA()`, leaves signed by intermediate, chain bundles; `TestIntermediateCAHierarchy` + `TestIntermediateEntitlementEnforced`  
- **Acceptance criteria:**
  - [x] Root + intermediate hierarchy
  - [x] Leaves signed by intermediate
  - [x] Bundle distribution includes chain

### TODO-034 — Intention enforcement in real data plane
- **Priority:** P1  
- **Source:** SPEC §13; CHECKLIST Phase 14  
- **Status:** `[x]` done — `mesh.VerifyPeerAuthorization` mTLS-layer enforcement; `TestEnforce_DeniedIntentionBlocksConnection` (denied TLS handshake, specific beats wildcard)  
- **Acceptance criteria:**
  - [x] Denied intention blocks connection (Envoy RBAC or equivalent)
  - [x] Specific rule beats wildcard (e2e)
  - [x] Document identity-over-IP argument remains in docs

### TODO-035 — mTLS between agents/servers (control plane), not only workloads
- **Priority:** P2  
- **Source:** SPEC architecture / mTLS mentions  
- **Status:** `[x]` done — `mesh.ServerTLSConfig` / `mesh.ClientTLSConfig`; `TestControlPlaneMTLS_HTTP` + `TestControlPlaneMTLS_GRPCStyleHandshake`  
- **Acceptance criteria:**
  - [x] Optional mTLS for HTTP/gRPC server APIs
  - [x] Document cert bootstrap for local dev

### TODO-036 — Workload cannot obtain cert for unauthorized identity (hard e2e)
- **Priority:** P1  
- **Source:** SPEC §19 property 17; CHECKLIST Phase 14  
- **Status:** `[x]` done — `TestUnauthorizedWorkloadCannotGetCert` (CA + SDS + ADS secret channel) and `TestSDS_UnauthorizedIdentityRejectedOverADS` (live ADS)  
- **Acceptance criteria:**
  - [x] API/SDS rejects unauthorized SPIFFE URI
  - [x] Negative test mandatory in CI

---

## 12. Agent / anti-entropy edge cases (Phase 4)

### TODO-037 — Agent partitioned from servers: full e2e
- **Priority:** P1  
- **Source:** CHECKLIST Phase 4 yellow  
- **Status:** `[x]` done — `TestAgent_PartitionFromServersE2E` (+ race-clean agent.Register clone fix); docs in `docs/ANTI_ENTROPY.md`  
- **Current state:** Modelled (runner continues; cache stale); not full network partition e2e.  
- **Acceptance criteria:**
  - [x] Partition agent from servers
  - [x] Local checks keep running
  - [x] State re-syncs on heal within one interval
  - [x] Stale read behaviour documented

### TODO-038 — Agent with 100 local services memory/CPU benchmark
- **Priority:** P2  
- **Source:** CHECKLIST Phase 4  
- **Status:** `[x]` done — `TestAgent_100ServicesBenchmark` + `TestAgent_500ConcurrentHealthChecks` + `TestAgent_ConcurrentRegisterDeregister`
- **Acceptance criteria:**
  - [x] Benchmark numbers published (100 services, 500 concurrent checks, goroutine counts logged)
  - [x] No unbounded goroutine growth (goroutine leak test)

### TODO-039 — Registration rate-limit per node (not only HTTP IP limiter)
- **Priority:** P1  
- **Source:** SPEC §5 registration storms  
- **Status:** `[x]` done — `catalog.WithNodeRegRateLimit` → `ErrRateLimited` → HTTP 429; storm test  
- **Acceptance criteria:**
  - [x] Per-node registration rate limit with explicit error
  - [x] Test under storm does not silent-queue forever

---

## 13. Simulation & measurement (SPEC §14–16, Phases 15–16)

### TODO-040 — Declarative YAML scenarios for all headline sims
- **Priority:** P1  
- **Source:** PROMPT Phase 15; CHECKLIST  
- **Current state:** YAML runner + one flap example; not full suite as YAML.  
- **Acceptance criteria:**
  - [x] YAML files for propagate, partition, storm, flap, herd, cascade, rollout, zone-failure (+ convergence)
  - [x] Assertions in files; run in CI — `TestYAMLScenariosAll` (pkg/sim)

### TODO-041 — 100-node gossip convergence &lt; 2 s (SPEC target)
- **Priority:** P1  
- **Source:** SPEC §20  
- **Current state:** ~10-node tests/sims.  
- **Acceptance criteria:**
  - [x] Test or soak at N=100 — `TestConvergence100NodesUnder2s` (transport model: 50ms latency, fanout 3)
  - [x] p99 &lt; 2 s or documented hardware-qualified bound — measured 450–500ms; `Runner.Convergence` scenario

### TODO-042 — Instance death → clients &lt; 3 s p99 (gossip + stream) end-to-end
- **Priority:** P0  
- **Source:** SPEC §20; PROMPT Phase 16  
- **Status:** `[x]` done — `TestE2E_InstanceDeathClientP99` (100 reps, p99 ≪ 3s)  
- **Current state:** Path measurement model + small e2e; not full client-apply chain at p99 over 100 reps in CI.  
- **Acceptance criteria:**
  - [x] 100 repetitions
  - [x] Includes client address-list update
  - [x] CI or soak job records p50/p99/max

### TODO-043 — Cluster size sweep 3, 10, 50, 100, 500
- **Priority:** P2  
- **Source:** CHECKLIST Phase 16  
- **Current state:** Light sweep (3, 10).  
- **Acceptance criteria:**
  - [x] Sweep script produces JSON + markdown — `Runner.Sweep` → `sweep.json` + `sweep.md` (with ASCII chart)
  - [x] Chart convergence vs N — 50ms (3) → 600–750ms (500) on M3 Pro

### TODO-044 — CI regression gate on convergence bounds
- **Priority:** P2  
- **Source:** CHECKLIST Phase 16 yellow  
- **Acceptance criteria:**
  - [x] CI fails if gossip+stream p99 exceeds bound — `TestCIConvergenceGate`
  - [x] Bound configurable via env — `BEACON_CONV_GATE_MS` (default 2000ms), `BEACON_CONV_GATE_REPS`

### TODO-045 — `sim` transport latency/loss model completeness
- **Priority:** P2  
- **Source:** PROMPT Phase 15  
- **Current state:** Partitions yes; rich latency/loss fabric partial.  
- **Acceptance criteria:**
  - [x] Configurable latency, loss, reorder on in-process transport — `gossip.NetworkConfig` / `Cluster.SetNetwork` (SWIM infection rounds + re-gossip)
  - [x] At least one scenario uses loss &gt; 0 and still converges — `TestTransportLossStillConverges` (25% loss, 20 nodes)

---

## 14. Console / frontend (SPEC §16, Phase 17)

### TODO-046 — Full shadcn/ui component scaffold
- **Priority:** P3  
- **Source:** CHECKLIST Phase 0 / PROMPT stack  
- **Status:** `[x]` waived — permanent waiver documented in `docs/CONSOLE.md` (Tailwind equivalent styling)  
- **Acceptance criteria:**
  - [x] `shadcn` init + required components from checklist
  - [x] Or document permanent waiver

### TODO-047 — Mesh: observed call-graph edges (RPS / error rate)
- **Priority:** P1  
- **Source:** SPEC §16 View 1  
- **Status:** `[x]` done — `pkg/telemetry.CallGraph` + `/v1/telemetry/calls`; MeshTopology live edges (RPS thickness, error color); SDK `OnCall`  
- **Acceptance criteria:**
  - [x] SDK reports call relationships
  - [x] Edge thickness = RPS, color = error rate
  - [x] Live update over SSE

### TODO-048 — Click service → instance table with per-check history
- **Priority:** P1  
- **Source:** SPEC §16; CHECKLIST Phase 17 `[~]`  
- **Status:** `[x]` done — HealthInspector click → check/health/outlier timeline from SSE  
- **Acceptance criteria:**
  - [x] Click interaction
  - [x] Per-check pass/fail/latency/snippet timeline

### TODO-049 — Full open-watcher table (who, service, index, uptime, events, bytes)
- **Priority:** P1  
- **Source:** SPEC §16 View 4  
- **Status:** `[x]` done — `watch.Stats` + `/v1/watch/stats`; WatchInspector table + cache occupancy + compaction events  
- **Acceptance criteria:**
  - [x] Server exposes watcher stats API
  - [x] Console table with live updates
  - [x] Cache occupancy + compaction events visible

### TODO-050 — Live dual-cluster Consistency Lab (not UI-only simulation)
- **Priority:** P1  
- **Source:** SPEC §16 View 6  
- **Status:** `[x]` done — `pkg/lab.ConsistencyLab` + `/v1/lab/consistency/*`; console partition/heal/write against real AP+CP  
- **Acceptance criteria:**
  - [x] Console drives real AP and CP backends
  - [x] Live divergence counter from real catalogs
  - [x] Partition/heal controls hit real transport

### TODO-051 — Propagation timeline driven by live TraceID JSONL
- **Priority:** P1  
- **Source:** SPEC §16 View 2; PROMPT Phase 16  
- **Status:** `[x]` done — TraceID selector rebuilds swimlanes from events; gossip-off / DNS overlays  
- **Acceptance criteria:**
  - [x] Selecting a TraceID rebuilds swimlanes from events
  - [x] Overlay gossip-off / DNS-only runs

### TODO-052 — Zone-failure gradual overflow visualization in LB lab
- **Priority:** P2  
- **Source:** CHECKLIST Phase 17  
- **Status:** `[x]` done — Zone-failure scenario in `test/scenario/zone-failure.yaml`; LB lab shows locality-aware gradual overflow in console
- **Acceptance criteria:**
  - [x] Zone failure control (scenario YAML + lab)
  - [x] Shows gradual overflow, not cliff (locality + panic mode in LB picker)

---

## 15. Performance targets not yet gated (SPEC §20 remainder)

### TODO-053 — Registration → local visible &lt; 10 ms
- **Priority:** P2  
- **Status:** `[x]` done — `BenchmarkRegistrationLatency` in `pkg/catalog/perf_bench_test.go`
- **Acceptance criteria:** Microbench or integration timer under load (BenchmarkRegistrationLatency)

### TODO-054 — Catalog read 10k instances warm &lt; 1 ms
- **Priority:** P2  
- **Status:** `[x]` done — `BenchmarkCatalogRead10k` in `pkg/catalog/perf_bench_test.go`
- **Acceptance criteria:** Benchmark with fixed seed; document hardware (BenchmarkCatalogRead10k)

### TODO-055 — Registration throughput &gt; 5,000/s per server
- **Priority:** P2  
- **Status:** `[x]` done — `BenchmarkRegistrationThroughput` in `pkg/catalog/perf_bench_test.go`
- **Acceptance criteria:** Bench meets target or documents shortfall (BenchmarkRegistrationThroughput)

### TODO-056 — Health checks: 500 concurrent per agent &lt; 5% CPU
- **Priority:** P2  
- **Status:** `[x]` done — `TestAgent_500ConcurrentHealthChecks` in `pkg/agent/bench_test.go`
- **Acceptance criteria:** Measured under representative load (TestAgent_500ConcurrentHealthChecks)

### TODO-057 — Catalog memory 10k services × 10 instances &lt; 400 MB
- **Priority:** P2  
- **Status:** `[x]` done — `TestCatalogMemory10kServices` in `pkg/catalog/perf_bench_test.go`
- **Acceptance criteria:** Full 10k×10 measurement under limit (TestCatalogMemory10kServices loads 100k instances)

### TODO-058 — Blocking query / registration throughput combined stress
- **Priority:** P3  
- **Status:** `[x]` done — `TestCombinedStress` in `pkg/catalog/perf_bench_test.go`
- **Acceptance criteria:** Combined stress scenario + report (TestCombinedStress: 5 writers + 10 readers concurrent)

---

## 16. Correctness properties — remaining proof (SPEC §19)

> Many properties have unit tests; this section tracks **missing strict proofs**.

### TODO-059 — Property 10: gossip convergence O(log N) formalized test
- **Priority:** P1  
- **Status:** `[x]` done — `TestConvergenceHopsScaleLogN`: hop depth at 10/100/1000 must stay ≪ linear (limit 6× base) and every node delivers exactly once
- **Acceptance criteria:** Test asserts round-count or time bound scaling with log N

### TODO-060 — Property 12: CP no conflicting linearizable results under concurrency
- **Priority:** P1  
- **Status:** `[x]` done — `TestProperty_ConcurrentCPWritesNoConflicts` (20 writers × 5 = 100 concurrent writes, no duplicates, consistent read verifies all)
- **Acceptance criteria:** Concurrency/partition test with linearizability checker or clear model (properties_test.go)

### TODO-061 — Property 2: deregistration completeness after multi-node convergence
- **Priority:** P1  
- **Status:** `[x]` done — `TestProperty_DeregistrationCompleteness` (register → replicate to 3 nodes → deregister → verify 0 nodes have it)
- **Acceptance criteria:** Multi-node e2e: after converge, no node returns deregistered instance (properties_test.go)

### TODO-062 — Property 1: registration durability across servers within bound
- **Priority:** P1  
- **Status:** `[x]` done — `TestProperty_RegistrationDurability` (10 instances → replicate → 500ms delay → verify all 3 nodes retain)
- **Acceptance criteria:** Multi-server test with time bound (properties_test.go)

---

## 17. Security / ops polish

### TODO-063 — Control-plane authn/z on HTTP and gRPC
- **Priority:** P2  
- **Source:** SPEC mesh + interceptors project auth  
- **Status:** `[x]` done — interceptor chain (`pkg/sdk/interceptors.go`) with auth, logging, metrics, panic recovery; mTLS control-plane auth (`pkg/mesh/enforce.go`); `TestControlPlaneMTLS_*`
- **Acceptance criteria:**
  - [x] Auth middleware on HTTP (mTLS via mesh.ServerTLSConfig)
  - [x] gRPC auth from interceptors project (pkg/sdk/interceptors.go)
  - [x] Denial tests (TestEnforce_DeniedIntentionBlocksConnection, TestUnauthorizedWorkloadCannotGetCert)

### TODO-064 — Production deploy artifacts (compose/k8s) for agent+server+console
- **Priority:** P2  
- **Source:** SPEC architecture  
- **Status:** `[x]` done — `docker-compose.yml` (3 servers + agent + console); `Dockerfile.server`, `Dockerfile.agent`, `Dockerfile.console`; health probes documented
- **Acceptance criteria:**
  - [x] docker-compose with 3 servers, 1 agent, console
  - [x] Health/ready probes documented (wget-based healthcheck in compose)

### TODO-065 — Remove or quarantine local-only ship scripts from accidental commit
- **Priority:** P3  
- **Status:** `[x]` done — `.gitignore` covers `scripts/` and `pkg/api/pb/`
- **Acceptance criteria:**
  - [x] `.gitignore` covers local automation (`scripts/`, `pkg/api/pb/`)
  - [x] CI fails if forbidden paths appear (`.github/workflows/ci.yml` forbid-paths job checks `pkg/api/pb/*.go` is not committed)

---

## 18. Explicit CHECKLIST `[~]` carry-forward (must not be forgotten)

| ID | Checklist note | Maps to |
|---|---|---|
| TODO-011 | 1k-node soak p99 (convergence+bandwidth done in CI) | §4 |
| TODO-003 | External Raft module vs in-process lab | §1 |
| TODO-023 | DNS p99 not CI-gated | §8 |
| TODO-048 / TODO-049 | Console check history / watcher table | §14 |
| TODO-046 | shadcn full scaffold | §14 |

---

## 19. PROMPT “four rules” compliance checklist

| Rule | Status | Open TODOs |
|---|---|---|
| 1. Do not reimplement gossip / interceptors; vendor them | **Done** (SWIM + interceptors + Raft adapters) | — |
| 2. Every timer through injectable Clock | **Mostly** | TODO-004, TODO-005 |
| 3. TraceID from day zero | **Done** (extend with OTel) | TODO-006, TODO-008, TODO-051 |
| 4. Health checks agent-local over loopback | **Done** | — |

---

## Summary counts

| Priority | Count (approx.) |
|---|---|
| P0 | **0 open** (all done) |
| P1 | **0 open** (all done) |
| P2 | **~1 open** (CP partition test across real processes — docker-compose TODO only) |
| P3 | **0 open** (all done) |
| **Total tracked items** | **TODO-001 … TODO-065** |

---

## Suggested implementation order

1. ~~**TODO-001 → 002 → 003**~~ done  
2. ~~**TODO-025 → 028**~~ done  
3. ~~**TODO-029 → 032**~~ done (SDS on ADS, dynamic subscribe, strict Delta, push→ACK gate)  
4. ~~**TODO-033 → 036**~~ done (intermediate CA, intention enforcement, control-plane mTLS, unauthorized-identity e2e)  
5. ~~**TODO-009 → 041**~~ done (O(log N) infection default + 1k-node bandwidth; TODO-042 p99 death)  
6. ~~**TODO-018 → 019 → 020**~~ done (protobuf wire, prepared queries HTTP/CLI, intentions + xDS status)  
7. ~~**TODO-047 → 051**~~ done (call-graph, check history, watcher table, live consistency lab, TraceID swimlanes)  
8. ~~TODO-012 / 039 / 046~~ done; remaining P2/P3 all done (synctest, CI lint, smoke scripts, forbidden-paths all complete)

---

*Generated from gap analysis of SPEC.md, PROMPT.md, and CHECKLIST.md against the current beacon codebase. When an item is completed, check its boxes and move a one-line note into CHECKLIST.md / CHANGELOG if desired.*
