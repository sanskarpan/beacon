# Changelog

All notable changes to `beacon` are documented here. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [SemVer](https://semver.org/).

## [Unreleased]

### Changed
- **BREAKING:** Go module path `github.com/sanskar/beacon` → `github.com/sanskarpan/beacon` (repo URL). Update imports: `sed -i 's|github.com/sanskar/beacon|github.com/sanskarpan/beacon|g'`. Old path frozen at `v0.1.0` on proxy.
- `docs/assets/demo.gif` regenerated with realistic terminal frames (6× 960×540, `python3 + PIL` via `python3.12 /tmp/gen_demo.py`).

## [v0.1.0] - 2026-09-12

First tagged release. Includes the audit-complete baseline (`#184`: 65 TODOs,
18 phases, AP/CP backends, watch, DNS, xDS, mesh, sim, console — see
`CHECKLIST.md`) plus everything below.

### Added
- Console migrated to Tailwind CSS 4 (`@tailwindcss/postcss`, CSS-first `@theme`
  tokens, obsolete JS config removed)
- Console on TypeScript 7 (regenerated `bun.lock`, TS7-incompatible options removed)
- Real-process CP partition/recovery coverage: `TestE2E_CPComposePartition`
  isolates a Docker Compose minority at the network layer and verifies majority
  writes, minority rejection/stale reads, reconnect, and recovery in CI
- WAN partition/heal gossip convergence test (`pkg/gossip/wan_convergence_test.go`)
- `Dockerfile.server` creates writable `/data` for the non-root `beacon` user

### Fixed
- `pkg/mesh/sds.go`: `Fetch` signs outside the cache lock (no `mu` across `CA.Sign`)
- `pkg/xds/server.go`: `sortStrings` → `sort.Strings`; removal pushes ordered via `RemoveOrder` (`OrderedTypes`, make-before-break)
- `pkg/lb/picker.go`: ring-hash O(n²) sort → `sort.Slice`
- `pkg/telemetry`: `otlpFallback` wraps live OTLP exporter (was dead code)
- `cmd/beacon-server`: real gRPC `ProtoServer.Serve` on `--grpc :8502` with `GracefulStop` (was placeholder)
- `pkg/mesh`: `NewCAProduction` fail-closed constructor (production must use it)
- `pkg/gossip`: deterministic loss drop (Loss=1.0 blocks all); `pkg/xds` debouncer test race fixed (`atomic.Int64`)
- **Critical (9):** Agent.Register race, Clock injection, Restore monotonic, O(log N) docs, pendingFull anti-entropy, Merkle hash, tombstone propagation, CP ReadIndex quorum, Client mTLS VerifyPeerCertificate — see `ISSUES.md` C1-C9
- **High (18):** UpdateHealth no-bump, Deregister incarnation 0, batch coalescing, watchMembership leak, UpdateCheckStatus output, lease grace, watcher ID collision, WatchMulti Send, P2C/Locality/xDS/Client races, NACK clear, RemoveOrder, weighted LB — H1-H18
- **Medium (22):** per-service future-index, Equal Incarnation, Clock rng, criticalSince timer, rate-limiter GC, ResolveService Wait, per-peer partition, leader forward, WAN wildcard, jitter, Wait cap, HTTP limiter GC, DNS strict tag, byName orphan, ctx Execute, StreamOutcomeReporter, CA dev-mode flag, entitlements copy, TLS1.3 defer — M1-M22
- `trace.NewID` zero-padded hex, DNS shuffle dead code

### Added
- `pkg/api/pb/pb.go` hand-written stub, `pkg/mesh/sds_xds.go` SDS-XDS adapter, `external/` stubs (`gossip-system`, `grpc-service`)
- `LICENSE` (MIT), `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `CHANGELOG.md`
