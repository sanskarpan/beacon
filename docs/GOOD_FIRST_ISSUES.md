# Good First Issues

Starter tasks scoped to be completable in one PR each. Every item names the
files involved and the acceptance criteria. General workflow: see
`CONTRIBUTING.md` (dev setup, commit style, DCO sign-off).

---

## 1. Record `docs/assets/demo.gif`

**Status:** Complete. `docs/assets/demo.gif` is a real browser capture of the running
console and is embedded by both the README and Pages.

**Files:**

- `docs/DEMO.md` (60-second run + recording script — follow it exactly)
- `docs/assets/demo.gif` (new)
- `README.md` (verify the embed renders)

**Acceptance criteria:**

- [x] `docs/assets/demo.gif` exists, ≤ ~5 MB, and is ~960 px wide
- [x] GIF shows live registration/deregistration, benchmark, mesh, and health views
- [x] `README.md` + Pages render the image (relative path `docs/assets/demo.gif`)
- [x] No unrelated beacon assets changed

---

## 2. Fill in per-package bench table entries in `docs/BENCHMARKS.md`

**Gap:** `docs/BENCHMARKS.md` has the propagation headline table but the
LB/catalog section only says *"Record `ns/op`, `B/op`, `allocs/op` per package
here on every release"* — no numbers are recorded.

**Files:**

- `docs/BENCHMARKS.md`
- Benchmarks: `pkg/lb/pick_bench_test.go` (`BenchmarkP2CPick`), `pkg/catalog/perf_bench_test.go` (`BenchmarkRegistrationLatency`, `BenchmarkCatalogRead10k`, `BenchmarkRegistrationThroughput`), `pkg/catalog/bench_test.go`, `pkg/api/dns/bench_test.go` (`BenchmarkDNS_A`), `pkg/xds/ads_ack_latency_test.go`

**Acceptance criteria:**

- [ ] Run `go test -bench=. -benchmem ./pkg/lb/... ./pkg/catalog/... -count=1` (+ dns/xds benches) on one machine; paste output into a dated table with CPU model + `go version`
- [ ] Table columns: benchmark | ns/op | B/op | allocs/op | target (from `SPEC.md` §20 / `TODO.md` where one exists)
- [ ] Note any target miss honestly (e.g. DNS p99 vs 2 ms — see `TODO-023`); do not hand-wave
- [ ] Repro command recorded verbatim

---

## 3. Console accessibility pass (keyboard + ARIA + contrast)

**Gap:** The console ships D3/Recharts views (`MeshTopology`, `PropagationTimeline`, `HealthInspector`, `WatchInspector`) with no documented keyboard path or ARIA roles. Tab-through and screen-reader users are stuck.

**Files:**

- `console/src/views/PropagationTimeline.tsx`
- `console/src/components/` (Topology, HealthInspector, WatchInspector)
- `docs/CONSOLE.md` (document what was fixed)

**Acceptance criteria:**

- [ ] All interactive views reachable + operable by keyboard (Tab/Enter/Escape); visible focus ring
- [ ] Charts have `role="img"` + text alternative (summary table or `aria-label` with current values)
- [ ] Live SSE regions use `aria-live="polite"`; pause control stops announcements
- [ ] `cd console && bun run build` (tsc + vite) passes; note manual screen-reader/keyboard test in the PR

---

## 4. Runbook drill script for one incident

**Gap:** `docs/runbooks/` covers gossip-partition, xds-nack, and flapping, but
there is no runnable drill that proves the runbook steps work against a live
server. `scripts/multi-server-smoke.sh` exists for happy-path; nothing exercises
the incident path.

**Files:**

- `docs/runbooks/RUNBOOK.md` + one of `gossip-partition.md` / `xds-nack.md` / `flapping.md`
- New: `scripts/runbook-drill-<name>.sh` (pick one incident)
- `CONTACTS.md` / `SECURITY.md` (verify escalation/SLA references still correct)

**Acceptance criteria:**

- [ ] Script drives `docker-compose.yml` (or local `bin/beacon-server`) through: inject fault → observe symptom (`beacon members`, `/metrics`, `GET /v1/events`) → follow runbook → verify recovery
- [ ] Non-zero exit on any step failure; prints the runbook step number as it goes
- [ ] Runbook updated with drill command + expected output (copy-pasteable)
- [ ] `scripts/` ignore/CI rules respected (see `TODO-065`: `.gitignore` + `forbid-paths` job)

---

## 5. Deterministic DNS TC-bit (truncation) test

**Gap:** `pkg/api/dns/server.go` sets `Truncated = true` when a UDP response
exceeds 512 bytes, and `pkg/api/dns/dns_more_test.go::TestDNSTruncationSetsTC`
covers it — but the test logs *"may not exceed 512 with few records"*, i.e. it
can pass vacuously without ever forcing truncation.

**Files:**

- `pkg/api/dns/server.go` (truncation block, ~L98–105)
- `pkg/api/dns/dns_more_test.go` (`TestDNSTruncationSetsTC`)
- `pkg/api/dns/dc_test.go` (patterns for DC/tag/SRV table tests)

**Acceptance criteria:**

- [ ] Test registers enough instances (or shrinks the UDP budget via the same code path) that the UDP response **must** exceed 512 bytes — assert `Truncated == true` unconditionally, no soft `t.Logf` fallback
- [ ] Companion assertion: same query over TCP returns the full answer set (`TC == false`, all records present)
- [ ] `go test -race ./pkg/api/dns/...` passes

---

## 6. Document the DNS p99 headroom (TODO-023 follow-up)

**Gap:** `TODO-023` is `[~]` partial: `TestDNS_LatencyPercentiles` measures 10k
queries with a 5 ms CI headroom, but a strict 2 ms gate is not enforced on
shared runners, and the reasoning lives only in code comments — not in docs.

**Files:**

- `pkg/api/dns/` latency test (`TestDNS_LatencyPercentiles`)
- `docs/BENCHMARKS.md` (DNS row)
- `CHECKLIST.md` Phase 10 note

**Acceptance criteria:**

- [ ] Short section in `docs/BENCHMARKS.md`: what is measured (10k queries, A/SRV), current p50/p99 on reference hardware, why CI asserts 5 ms headroom instead of the SPEC §20 2 ms gate
- [ ] Numbers reproduced locally with the exact `go test -bench` / `-run` command recorded
- [ ] No change to the gate itself (that is a separate, harder issue)

---

## 7. Watch-scale doc + 5k-watcher reproduction note (TODO-016 follow-up)

**Gap:** `TODO-016` is `[~]` partial: 5k watchers proven, 10k memory-budget proof
remains open. The current state (what passes, what OOMs, buffer > 16 caveat) is
scattered across `TODO.md` and `pkg/watch/scale_test.go`.

**Files:**

- `pkg/watch/scale_test.go` (`BenchmarkWatchMemory`, scale tests)
- `docs/BENCHMARKS.md` or `docs/WATCH.md` (wherever you put the note — link it from the other)
- `TODO.md` `TODO-016` (update only if you actually close a checkbox)

**Acceptance criteria:**

- [ ] Reproduce the 5k-watcher run locally; record memory per idle stream (target < 8 KB), machine, and command
- [ ] One-paragraph note in docs: what is proven at 5k, what blocks 10k (buffer sizing), pointer to the test
- [ ] Do **not** claim 10k unless `TestCombinedStress`-style proof passes with the gate in CI
