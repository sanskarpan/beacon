# Changelog

All notable changes to `beacon` are documented here. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [SemVer](https://semver.org/).

## [Unreleased]

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

## [0.1.0] - 2024-08-24

- Initial audit-complete release (`#184`): 65 TODOs, 18 phases, AP/CP backends, watch, DNS, xDS, mesh, sim, console — see `CHECKLIST.md`.
