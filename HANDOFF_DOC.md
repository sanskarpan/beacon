# HANDOFF_DOC — beacon @ cc026c8 + production-readiness working tree

> **Repo:** `sanskarpan/beacon` · **Branch:** `feat/production-readiness-2026-09-06` · **Base:** `main` @ `cc026c8` · **Date:** 2026-09-06
> **Stack:** Go 1.26.5, `miekg/dns`, `grpc-go`, `x/sync`, `prometheus`, `otel`, React 18 + Vite + Tailwind + D3 · **CI:** `ci.yml` hardened

## 1. What this repo is

Consul-class service discovery: catalog (monotonic index, leases, health hysteresis), agent-local checks + anti-entropy, gossip (SWIM-via-`Membership` seam) AP vs Raft CP side-by-side, watch (blocking + gRPC streaming + cache), DNS/HTTP/gRPC/xDS (SotW+Delta), SPIFFE mTLS + SDS + intentions, SDK resolver/balancer (p2c/ring/maglev) + `OutcomeReporter`, sim (propagate/partition/storm/flap/herd/cascade + YAML + rollout/zone-failure) and propagation measurement, console (7 views, SSE, D3 topology, timeline).

## 2. Current scenario

- **Last merged PR:** #184 `feat: Complete codebase audit`; later hardening commits `ff604fe` and `cc026c8` are on `main`. This branch contains the next production-readiness batch.
- **QA pass 2026-09-01→02** (5 sub-agents) produced `ISSUES.md` 9 Critical / 18 High / 22 Medium / 12+ Low. `go vet` was clean after fixes, `go test ./pkg/...` green, but races/brittleness remained.
- **Fix pass 2026-09-02** fixed the prior QA set and verified `go vet`, package tests, and critical races. This branch additionally verifies full Go tests, targeted races, frontend npm lint/build, simulations, protobuf checks, and Compose parsing.
- **Production hardening pass** added `LICENSE`/`SECURITY`/`CONTRIBUTING`/`CODE_OF_CONDUCT`/`CHANGELOG`, README badges/config/API, `ci.yml`/`Makefile`/`golangci.yml`, `Dockerfile.*`/`docker-compose`/`console/nginx`, `docs/` (`CONFIGURATION`/`API`/`DEPLOYMENT`/`OBSERVABILITY`), `.dockerignore`/`.gitignore`/`console` scripts.
- **Checklist audit correction:** the production CP/UDP paths and live console views are implemented, but the 1k-process soak, strict DNS gate, 5k/10k watcher proof, upstream SWIM/interceptor replacements, and real-process propagation measurement remain explicitly partial.

## 3. Agile commit strategy (as observed)

- **One commit per file.** Branches `fix/<path>` e.g. `fix/pkg-catalog-store-go`, `fix/-golangci-yml`, `fix/pkg-agent-agent-go`.
- **Each commit `Closes #<issue>`** (e.g. `fix: prevent Agent.Register race (C1) Closes #185`).
- **PRs target a feature branch** (`feat/qa-fixes-2026-09-02`, `feat/prod-hardening-2026-09-02`, `feat/checklist-console-2026-09-02`) which is then merged into `main` via `Merge pull request` / `git merge --no-ff`.
- **Current branch policy:** this batch must land through a pull request; it is not to be pushed directly to `main`.

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

- **CHECKLIST.md:249** now records the remaining `[~]` evidence gaps instead of claiming all prior demo-scale items are complete.
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

Local verification note: Docker Desktop's daemon and Bun were unavailable in
this environment; npm frontend checks, Docker Compose parsing, Go builds, and
all non-container Go validation passed.

## 8. Known low (not gating)

`watch/cache` ring now O(1) but cap 64; `console` still hand-styled Tailwind (shadcn-equivalent per `docs/CONSOLE.md`); `events.Bus.filter` now used via `SetFilter`; `OTel` `ResetForTest()` added; `gossip` loss now clock-varying; `otlpFallback` dead code removed; `SDS.Fetch` no longer holds `mu` across `CA.Sign`; `watch` herd jitter via `globalJitterRng`.

## 9. Next steps for new agent

- Pull `main` (`1f2f2ca`), run `make tidy-check` + `make build` + `go test ./...`.
- For new issues: create `fix/<file>` branch, one commit per file (`Closes #<n>`), PR to `feat/<feature>`, merge to `main`.
- Bump `CHANGELOG.md` and `VERSION` via `make build` `ldflags`.

## 10. Contacts & history

- `git log --oneline origin/main..main` should be 0 (pushed). Feature branches `feat/qa-fixes-2026-09-02`, `feat/prod-hardening-2026-09-02`, `feat/checklist-console-2026-09-02` pushed for audit trail.
- Prior audit: `ISSUES.md` Fix Summary 2026-09-02 (verified `go vet` clean, `go test -race` green).
