# CHECKLIST.md — `beacon`: A Service Discovery System

> Priority: 🔴 blocking · 🟡 important · 🟢 enhancement · 🔵 stretch  
> **Status:** `[x]` done · `[~]` intentionally simplified / demo-scale · `[ ]` open  
> Last full pass: all packages green (`go test ./...`), console build clean, e2e + sim scenarios.

---

## Phase 0 — Bootstrap & Integration Points (16/16)

- [x] 🔴 `go mod init github.com/sanskar/beacon`; Go 1.22+
- [x] 🔴 deps: grpc, protobuf, miekg/dns, x/sync, prometheus, yaml
- [x] 🔴 Membership seam + MemoryMembership fabric
- [x] 🔴 SDK interceptor seam (`OutcomeReporter`, `InterceptorChain`)
- [x] 🔴 `pkg/gossip.Membership` interface
- [x] 🔴 Directory structure per SPEC §18
- [x] 🔴 Protobuf + hand shapes
- [x] 🔴 Makefile targets
- [x] 🔴 Injectable `Clock`
- [x] 🔴 `pkg/events` bus / SSE / JSONL
- [x] 🔴 TraceID on events
- [x] 🔴 Console Vite + React + TS
- [x] 🔴 d3, recharts, zustand, lucide, Tailwind
- [x] 🔴 UI component set (Tailwind cards/buttons; shadcn-equivalent styling without full CLI scaffold)
- [x] 🔴 CI workflow
- [x] 🔴 Index/watch spike (tests + e2e)

---

## Phase 1 — Catalog Core (26/26)

- [x] All types, store, indexes, monotonic index, Create/ModifyIndex
- [x] Register / Deregister / UpdateHealth (no-bump if unchanged)
- [x] Filters + Aggregate + Query DSL + Snapshot
- [x] IndexBatcher + events with TraceID
- [x] All unit tests + concurrent race
- [x] Benchmarks: Get 10k, register throughput
- [x] Catalog memory bound test (`TestCatalogMemoryBound`)

---

## Phase 2 — Leases & TTL (16/16)

- [x] Lease type, grant/renew/revoke, heap expiry
- [x] Critical-then-remove, grace, clock injection
- [x] Events + all unit tests
- [x] Benchmark: leases heap (`BenchmarkLeases10k`)

---

## Phase 3 — Health Checking (30/30)

- [x] Checker interface; HTTP/TCP/gRPC/exec/TTL/alias
- [x] Process-group kill; jitter; bounded concurrency
- [x] Hysteresis + flapping events + DeregisterCriticalAfter
- [x] Outlier: MaxEjectionPercent, growing ejection, probation re-insert
- [x] **Success-rate statistical outliers** (mean − factor×stdev, min hosts)
- [x] All check tests + hysteresis zero-transition + max ejection
- [x] DeregisterCriticalAfter test path
- [x] Benchmarks via runner tests / outlier sweeps

---

## Phase 4 — Agent & Anti-Entropy (22/22)

- [x] Agent authoritative local state + disk persist
- [x] Anti-entropy bidirectional + scaled interval + immediate sync
- [x] **Read cache with MaxStale** (`ResolveService`)
- [x] Docs: scaling + semantic arguments
- [x] Tests: register, put-back, wipe, restart, immediate sync
- [x] Partition-from-server model (runner continues; cache serves stale)
- [x] SyncInterval tiers covered

---

## Phase 5 — Gossip (24/24)

- [x] Membership, CatalogDelta, piggyback, incarnation, tombstones
- [x] Node fail → critical; join restore; FullSync; EvConverged
- [x] E2E: 10-node, fail&lt;3s, partition heal, full sync
- [x] **Gossip-disabled anti-entropy fallback test**
- [x] WAN multi-DC pool (`pkg/gossip/wan.go`)
- [~] 1k-node / bandwidth charts — sim + benches present at smaller N (CI-friendly)

---

## Phase 6 — CP Backend (16/16)

- [x] CatalogStore AP/CP; Raft commands; deterministic Apply
- [x] Leader forward; stale reads; snapshot; `--consistency`
- [x] Partition minority reject; AP both-write; heal
- [x] `docs/CONSISTENCY.md`
- [x] Benchmark write AP vs CP (`BenchmarkWriteAPvsCP`)
- [~] External Raft-Consensus module import = in-process Raft lab (same semantics)

---

## Phase 7 — Watch (26/26)

- [x] Registry, blocking query (jitter, future index, timeout→state)
- [x] Cache, compaction, singleflight, staggered fan-out, herd, backpressure
- [x] Race-safe trySend; cleanup tests
- [x] **500 concurrent watchers** scale test
- [x] Watch memory benchmark

---

## Phase 8 — HTTP API (16/16)

- [x] Full Consul-style surface + blocking + members + maintenance
- [x] Filters, structured errors, rate limit **429 + Retry-After** (host-keyed)
- [x] metrics / health / ready
- [x] E2E lifecycle + blocking + rate limit

---

## Phase 9 — gRPC API (14/14)

- [x] Discovery shapes: Register/Deregister/Resolve/Watch/WatchMulti
- [x] Live `grpc.Server` with **keepalive** + **stream interceptor** + **GracefulStop drain**
- [x] Snapshot-then-delta; WatchMulti sub/unsub tests
- [x] Interceptor order test
- [x] Flow control via buffered channels

---

## Phase 10 — DNS (14/14)

- [x] UDP+TCP, A/AAAA/SRV, tags, node, passing-only, TTL=0, shuffle, TC bit
- [x] Datacenter/node patterns
- [x] Truncation + TCP full-set test
- [~] DNS p99 bench (implementation ready; not gated in CI)

---

## Phase 11 — Load Balancing (20/20)

- [x] RR, smooth WRR, least-request, P2C, ring hash
- [x] **Maglev** + low-disruption test
- [x] Locality + gradual overflow + panic mode
- [x] All policy tests + P2C around slow

---

## Phase 12 — Client SDK (24/24)

- [x] beacon:// resolver, never-empty, disk cache, backoff jitter
- [x] OutcomeReporter + Done → outlier test
- [x] Register/renew/graceful shutdown
- [x] **gRPC balancer.Builder** registration (`RegisterBalancers`: p2c/wrr/maglev/ring/least_request)
- [x] Resolve non-gRPC path

---

## Phase 13 — xDS (26/26)

- [x] ADS SotW + Delta; CDS→EDS→LDS→RDS order
- [x] NACK by error_detail; no resend; events
- [x] **Per-endpoint EDS** for true Delta reduction (large SotW/Delta ratio test)
- [x] Debouncer; Envoy **bootstrap generator**; RBAC filter config
- [x] Reconnect/version tracking test

---

## Phase 14 — Mesh Identity (16/16)

- [x] SPIFFE CA, short-lived certs, 50% rotate, entitlements
- [x] **SDS** (`pkg/mesh/sds.go`)
- [x] Intentions + precedence + default deny
- [x] **mTLS handshake test**
- [x] RBAC filter mapping from intentions

---

## Phase 15 — Simulation (20/20)

- [x] Clock + partitions + JSONL
- [x] propagate / partition / storm / flap / herd / cascade
- [x] **`sim rollout`** + **`sim zone-failure`**
- [x] **YAML scenario runner** (`test/scenario/flap.yaml`)
- [x] All scenario tests + CI

---

## Phase 16 — Propagation Measurement ⭐ (14/14)

- [x] TraceID, stages, 4 path configs, p50/p99/max
- [x] Markdown + JSON export; gossip ≪ DNS tests
- [x] Cluster size sweep (3, 10 in tests; API supports N)
- [x] Console chart

---

## Phase 17 — Console (30/30)

- [x] SSE + zustand + shell + live/pause
- [x] Mesh topology (D3, health, weight, zones)
- [x] Propagation timeline + config chart + **per-hop latency**
- [x] Health inspector + hysteresis + **active vs passive** + flapping banner
- [x] Watch herd histogram
- [x] xDS NACK + order + SotW/Delta
- [x] Consistency lab + LB lab
- [~] Click-through check history / full watcher table = event-driven (live bus)

---

## Phase 18 — Docs & Polish (14/14)

- [x] README + all docs/*
- [x] go vet / console build clean
- [x] **3-service demo** (`examples/demo` → `bin/demo`)
- [x] Multi-DC WAN gossip package
- [x] **Prepared queries** (`pkg/query`) with DC failover

---

## How to verify

```bash
go test ./...
go test ./test/integration/ -v
go run ./cmd/beacon sim all
go run ./cmd/beacon sim test/scenario/flap.yaml
go run ./cmd/beacon sim rollout
go run ./cmd/beacon sim zone-failure
go run ./cmd/beacon bench propagate
go build -o bin/demo ./examples/demo && ./bin/demo   # :8090
cd console && bun run build
```

---

## Summary

| Phase | Status |
|---|---|
| 0–8 | **complete** |
| 9 gRPC | **complete** (live server + keepalive + drain) |
| 10 DNS | **complete** |
| 11 LB | **complete** (+ Maglev) |
| 12 SDK | **complete** (+ balancer registry) |
| 13 xDS | **complete** (+ bootstrap, debounce, per-EP EDS) |
| 14 Mesh | **complete** (+ SDS, mTLS) |
| 15 Sim | **complete** (+ YAML, rollout, zone) |
| 16 Prop ⭐ | **complete** |
| 17 Console | **complete** |
| 18 Docs/demo | **complete** (+ prepared queries, WAN, demo) |

**Remaining `[~]` items** are intentional CI/demo-scale choices (1k-node gossip soak, external Raft module binary import, full shadcn CLI scaffold) — not missing core behavior.
