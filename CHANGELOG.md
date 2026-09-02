# Changelog

All notable changes to `beacon` are documented here. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [SemVer](https://semver.org/).

## [Unreleased]

### Fixed
- **Critical (9):** Agent.Register race, Clock injection, Restore monotonic, O(log N) docs, pendingFull anti-entropy, Merkle hash, tombstone propagation, CP ReadIndex quorum, Client mTLS VerifyPeerCertificate — see `ISSUES.md` C1-C9
- **High (18):** UpdateHealth no-bump, Deregister incarnation 0, batch coalescing, watchMembership leak, UpdateCheckStatus output, lease grace, watcher ID collision, WatchMulti Send, P2C/Locality/xDS/Client races, NACK clear, RemoveOrder, weighted LB — H1-H18
- **Medium (22):** per-service future-index, Equal Incarnation, Clock rng, criticalSince timer, rate-limiter GC, ResolveService Wait, per-peer partition, leader forward, WAN wildcard, jitter, Wait cap, HTTP limiter GC, DNS strict tag, byName orphan, ctx Execute, StreamOutcomeReporter, CA dev-mode flag, entitlements copy, TLS1.3 defer — M1-M22
- `trace.NewID` zero-padded hex, DNS shuffle dead code

### Added
- `pkg/api/pb/pb.go` hand-written stub, `pkg/mesh/sds_xds.go` SDS-XDS adapter, `external/` stubs (`gossip-system`, `grpc-service`)
- `LICENSE` (MIT), `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `CHANGELOG.md`

## [0.1.0] - 2024-08-24

- Initial audit-complete release (`#184`): 65 TODOs, 18 phases, AP/CP backends, watch, DNS, xDS, mesh, sim, console — see `CHECKLIST.md`.
