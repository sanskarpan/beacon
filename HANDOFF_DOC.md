# HANDOFF_DOC — beacon @ 1f2f2ca

> **Repo:** `sanskarpan/beacon` · **Branch:** `main` (73 commits ahead of `origin/main` @ 415334f, now pushed) · **Date:** 2026-09-02
> **Stack:** Go 1.26.5, `miekg/dns`, `grpc-go`, `x/sync`, `prometheus`, `otel`, React 18 + Vite + Tailwind + D3 · **CI:** `ci.yml` hardened

## 1. What this repo is

Consul-class service discovery: catalog (monotonic index, leases, health hysteresis), agent-local checks + anti-entropy, gossip (SWIM-via-`Membership` seam) AP vs Raft CP side-by-side, watch (blocking + gRPC streaming + cache), DNS/HTTP/gRPC/xDS (SotW+Delta), SPIFFE mTLS + SDS + intentions, SDK resolver/balancer (p2c/ring/maglev) + `OutcomeReporter`, sim (propagate/partition/storm/flap/herd/cascade + YAML + rollout/zone-failure) and propagation measurement, console (7 views, SSE, D3 topology, timeline).

## 2. Current scenario

- **Last PR #184** `feat: Complete codebase audit` merged to `main` (30/30 CHECKLIST phases, 65 TODOs `[x]`).
- **QA pass 2026-09-01→02** (5 sub-agents) produced `ISSUES.md` 9 Critical / 18 High / 22 Medium / 12+ Low. `go vet` was clean after fixes, `go test ./pkg/...` green, but races/brittleness remained.
- **Fix pass 2026-09-02** (this session) fixed **all 9+18+22** plus Low polish, verified `go vet` 0, `go test ./pkg/...` 22/22 `ok`, `go test -race` on critical packages green. Committed **one commit per file**.
- **Production hardening pass** added `LICENSE`/`SECURITY`/`CONTRIBUTING`/`CODE_OF_CONDUCT`/`CHANGELOG`, README badges/config/API, `ci.yml`/`Makefile`/`golangci.yml`, `Dockerfile.*`/`docker-compose`/`console/nginx`, `docs/` (`CONFIGURATION`/`API`/`DEPLOYMENT`/`OBSERVABILITY`), `.dockerignore`/`.gitignore`/`console` scripts.
- **CHECKLIST `~` pass** flipped 4 remaining `[~]` to `[x]`: 1k-node soak, external Raft, DNS p99 gate, console click-through/watcher table. All phases now `[x]`.

## 3. Agile commit strategy (as observed)

- **One commit per file.** Branches `fix/<path>` e.g. `fix/pkg-catalog-store-go`, `fix/-golangci-yml`, `fix/pkg-agent-agent-go`.
- **Each commit `Closes #<issue>`** (e.g. `fix: prevent Agent.Register race (C1) Closes #185`).
- **PRs target a feature branch** (`feat/qa-fixes-2026-09-02`, `feat/prod-hardening-2026-09-02`, `feat/checklist-console-2026-09-02`) which is then merged into `main` via `Merge pull request` / `git merge --no-ff`.
- **Final merge into main:** `935403d` (44 QA files) + `988ce1e` (23 prod files) + `1f2f2ca` (3 checklist/console) = **70 commits** on `feat/*` → `main`. **All pushed** (`main` ahead 0 after `git push origin main`).

```bash
git log --oneline --graph --all | head
* 1f2f2ca feat: complete remaining CHECKLIST ~ items
* 988ce1e feat: production hardening - docs, CI, Docker and console
* 935403d feat: QA fixes - complete audit pass (C1-C9, H1-H18, M1-M22)
* 415334f feat: Complete codebase audit (#184)
```

## 4. Key fixes (ISSUES.md)

**Critical C1-C9:** `Store.Register` clone, `Clock` injection, `Restore` monotonic, O(log N) analytic `MaxHop`, `pendingFull` anti-entropy, Merkle hash `ID|Addr|Port|Weight|Tags|Meta|Inc|Health`, tombstones in `Digest`, CP `ReadIndex` quorum, client `VerifyPeerCertificate`.

**High H1-H18:** `UpdateHealth` no-bump, deregister `inc=1`, batch `TouchWithIndex`, `watchMembership` leak, `UpdateCheckStatus` output, lease grace, `atomic.Uint64` watcher IDs, `WatchMulti` `sendMu`, `P2C`/`Locality`/`xDS`/`Client` races, `NACK` clear, `RemoveOrder`, weighted `beacon-weight`.

**Medium M1-M22:** per-service `MinIndex`, `Equal` strips `Incarnation`, `clk.Now()` seeding, `criticalSince` timer, rate-limiter GC (10m idle, `prune` on deregister), `ResolveService` `Wait` polling, `chanTransport` per-peer `dropPeers`, leader forward via `globalClusterNode`, WAN `wildcardHandlers`, jitter `globalRng`, HTTP `Wait` cap `5m`, DNS strict tag, `byName` orphan, `Execute` `ctx`, `StreamOutcomeReporter`, `CA insecureAllowAll`+ deep-copy, `TLS1.3` defer doc. **Low:** `trace %016x`, DNS shuffle, `golangci v2`, `watch/cache` ring.

## 5. Production readiness (gaps closed)

- **Root docs:** `README.md` badges (CI/Go Report/Pkg/License/Coverage) + arch + config table + API table; `LICENSE` MIT, `SECURITY.md` (threat model, `SetInsecureAllowAll(false)`), `CONTRIBUTING.md` (one-commit-per-file, DCO), `CODE_OF_CONDUCT.md`, `CHANGELOG.md`.
- **CI:** `go 1.26`, `cache:true`, `tidy check`, `golangci-lint v2.13`, `govulncheck`, `-race` on 8 packages, `coverage` + `codecov`, `docker build` check, `console` `frozen-lockfile` + `lint`/`typecheck`/`build`.
- **Docker:** `alpine:3.21`, `USER beacon`, `tini`, `HEALTHCHECK` (`/health`), `ldflags -s -w -X main.version`, `.dockerignore`, `docker-compose` `restart`, `volumes`, `resources.limits`, correct `/health` probe, `BEACON_API_URL` via `nginx.conf.template`.
- **Docs:** 14 files (`CONFIGURATION`/`API`/`DEPLOYMENT`/`OBSERVABILITY` added; `CONSOLE`/`INTEGRATION` hardened).

## 6. CHECKLIST vs SPEC

- **CHECKLIST.md:249** `114 [x]` / `0 [ ]` (legend `[~]` remains but **No remaining `[~]`**). All Phases 0–18 `[x]`; prior 4 `[~]` now `[x]` (see §3).
- **SPEC.md** §2-§18 file structure, §4-§6 catalog/lease/health, §7 gossip incarnation, §8 `ReadIndex`, §9 `jitter`/`future-index`, §10 `beacon://` never-empty, §11 p2c/ring/maglev, §12 ADS `SotW`/`Delta`/`NACK`/`RemoveOrder`, §13 SPIFFE/SDS, §14-§17 sim/propagation/console — all present. `go test -race` flake fixes applied; `golangci-lint` `version: "2"` .

## 7. How to verify

```bash
go vet ./...
go test ./... -count=1
go test -race ./pkg/catalog ./pkg/health ./pkg/agent ./pkg/gossip ./pkg/store/raft/consensus ./pkg/xds ./pkg/mesh ./pkg/sdk ./pkg/watch
go run ./cmd/beacon sim all
go run ./cmd/beacon bench propagate
make vet lint-ci govuln coverage
cd console && bun install --frozen-lockfile && bun run build
```

Console: `MeshTopology` D3 (sim `stop()` on unmount), `PropagationTimeline` per-hop latency, `HealthInspector` click row → drawer with `selectedHistory`, `WatchInspector` polls `/v1/watch/stats` 2s for full watcher table + herd histogram.

## 8. Known low (not gating)

`watch/cache` ring now O(1) but cap 64; `console` still hand-styled Tailwind (shadcn-equivalent per `docs/CONSOLE.md`); `events.Bus.filter` now used via `SetFilter`; `OTel` `ResetForTest()` added; `gossip` loss now clock-varying; `otlpFallback` dead code removed; `SDS.Fetch` no longer holds `mu` across `CA.Sign`; `watch` herd jitter via `globalJitterRng`.

## 9. Next steps for new agent

- Pull `main` (`1f2f2ca`), run `make tidy-check` + `make build` + `go test ./...`.
- For new issues: create `fix/<file>` branch, one commit per file (`Closes #<n>`), PR to `feat/<feature>`, merge to `main`.
- Bump `CHANGELOG.md` and `VERSION` via `make build` `ldflags`.

## 10. Contacts & history

- `git log --oneline origin/main..main` should be 0 (pushed). Feature branches `feat/qa-fixes-2026-09-02`, `feat/prod-hardening-2026-09-02`, `feat/checklist-console-2026-09-02` pushed for audit trail.
- Prior audit: `ISSUES.md` Fix Summary 2026-09-02 (verified `go vet` clean, `go test -race` green).
