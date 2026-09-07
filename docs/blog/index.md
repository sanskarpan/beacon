# Blog

Short design notes. Each post links back to the doc or ADR that is the source of truth.

## Post 1 — Why health checks are agent-local

**Sources:** [Architecture](../ARCHITECTURE.md#why-health-checks-are-agent-local) · [ADR 0002](../adr/0002-agent-local-health.md) · `pkg/health/` · `pkg/agent/`

Centralized health checking does not scale, and it asks the wrong question.

**The math.** 10,000 instances × 1 check / 5 s = **2,000 checks/s** from a central control plane, over the network, to every corner of the fleet. Agent-local, each agent checks its ~10 local instances over loopback — the per-node cost stays flat as the fleet grows. (See `ARCHITECTURE.md` and ADR 0002, which state exactly this 10k × 5 s = 2k/s figure.)

**The semantics.** Agent-local answers *"can this instance serve traffic"*. Central checks answer *"can the control plane reach this instance"*. Those two diverge under any partition between the control plane and the data plane — and the second question is the wrong one for load balancing. A server that cannot reach an instance over the network will mark healthy instances critical and flap the catalog.

**The consequences in this repo:**

- Agent local state is **authoritative**; the catalog is a replica. Agent-owned entries deleted out-of-band are put back, and wipes repopulate within one anti-entropy interval (`docs/ANTI_ENTROPY.md`, `pkg/agent/`).
- `UpdateHealth` only bumps `ModifyIndex` when status **actually changes**. Without this micro-rule, a healthy fleet of 10k instances checked every 5 s would generate ~2k index bumps/s and wake every watcher continuously (`docs/ARCHITECTURE.md#monotonic-catalog-index`).
- The console Health inspector distinguishes **active vs passive** health (check results vs `OutcomeReporter` from real traffic) — both feed hysteresis + flapping detection (`pkg/health/`, console `HealthInspector.tsx`).

Further reading: [Health](../HEALTH.md) · [Anti-Entropy](../ANTI_ENTROPY.md).

---

## Post 2 — Gossip+streaming vs health+DNS: the ~22× spread, and how `bench propagate` measures it

**Sources:** [Propagation](../PROPAGATION.md) · [Benchmarks](../BENCHMARKS.md) · `pkg/sim/propagate_measure.go` (`MeasurePropagation`, `PathConfig`) · `cmd/beacon/main.go` (`cmdBench`)

An instance dies. How long until clients stop sending it traffic? The answer depends almost entirely on the **detection + notification path**, not on the catalog write itself:

| Stage | Fast path (gossip+streaming) | Slow path (health+DNS) |
|---|---|---|
| t1 Detection | gossip, ~2 SWIM periods (~2 s) | check interval × failures (5 s × 3 = 15 s) |
| t2 Propagation | gossip delta, O(log N) rounds | agent → catalog, ~50 ms |
| t3 Notification | streaming, ~10 ms | DNS TTL / stub cache (modelled 30 s) |
| t4 Client apply | gRPC resolver, immediate (~1 ms) | ~100 ms |

**The measured numbers** (`go run ./cmd/beacon bench propagate`, checked into `docs/BENCHMARKS.md`):

| Configuration | p50 | p99 | max |
|---|---|---|---|
| gossip+streaming | 2.011s | 2.011s | 2.011s |
| health+streaming | 15.061s | 15.061s | 15.061s |
| health+blocking | 17.56s | 17.56s | 17.56s |
| health+dns | 45.15s | 45.15s | 45.15s |

~22× fast path vs slow path (`docs/index.md`: "gossip+streaming converges ~2s vs health+DNS ~45s (22×)"). `docs/PROPAGATION.md` frames the same spread as ~20–30×.

**How `bench propagate` works** (no mocks of the conclusion — the model is explicit in code):

- Entry point: `cmdBench` in `cmd/beacon/main.go` calls `sim.MeasurePropagation(20, 10)` (20 reps, 10 nodes), prints `sim.MarkdownTable(results)`, and writes `tmp/sim/propagation.json`.
- `MeasurePropagation` in `pkg/sim/propagate_measure.go` iterates the four `PathConfig` values (`gossip+streaming`, `health+streaming`, `health+blocking`, `health+dns`) and returns a `PathResult` per config with `p50`/`p99`/`max` plus per-stage means (`detection_stage`, `propagate_stage`, `notify_stage`, `apply_stage`).
- The gossip path builds a real `gossip.Cluster` + `MemoryMembership` pool and `gstore.Store` per node, registers a victim instance with a `TraceID`, fails the member, advances the virtual clock, and checks convergence; the three health paths model detection as 15 s with notification costs of ~10 ms (streaming), ~2.5 s (blocking, half of a 5 s wait), and 30 s (DNS TTL — note even `TTL=0` does not save you: stub resolvers routinely cache 30 s+).
- Every registration carries a `TraceID` agent → catalog → gossip → watch → client with a timestamp per hop; `EvConverged` fires when the last observer has the change. The console Propagation Timeline (`PropagationTimeline.tsx`) renders these swimlanes, with gossip-off / DNS overlays.

Reproduce it: `go run ./cmd/beacon bench propagate`. Compare contrast mode: `go run ./cmd/beacon bench contrast` (`MeasureGossipContrast`, gossip-off vs gossip-on over the same workload).
