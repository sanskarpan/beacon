# Beacon production hardening — audit 2026-09-04

Subagent audit found ~40 stub/gap items. This pass fixes the
prod-blocking code stubs and adds the missing production artifacts.

## Code fixes (this commit series)

| # | File | Fix |
|---|------|-----|
| 1 | `pkg/mesh/sds.go` | `Fetch` no longer holds `mu` across `CA.Sign`/`Bundle` — sign outside lock, double-checked store |
| 2 | `pkg/xds/server.go` | Bubble `sortStrings` → `sort.Strings`; added `OrderedTypes` so removal pushes use `RemoveOrder` (LDS→RDS→EDS→CDS make-before-break) |
| 3 | `pkg/lb/picker.go` | Ring O(n²) insertion sort → `sort.Slice` by hash |
| 4 | `pkg/telemetry/otel_exporters.go` | `otlpFallback` now wraps the live OTLP exporter (was dead code) |
| 5 | `cmd/beacon-server/main.go` | gRPC placeholder (`<-ctx.Done()` forever) replaced with real `grpcapi.ProtoServer.Serve` on `--grpc :8502` + `GracefulStop` drain |
| 6 | `pkg/mesh/identity.go` | Added `NewCAProduction` (fail-closed, `insecureAllowAll=false`); `NewCA` kept for dev/test compat |

## Deliberately NOT rewritten (documented seams, PROMPT Rule 1)

- `external/gossip-system`, `external/grpc-service` `replace` stubs: thin local
  adapters behind `pkg/gossip.Membership` / SDK interceptor seams. Production
  path (swap to real modules) is documented in `docs/INTEGRATION.md`.
- `pkg/api/pb` now contains generated standard-protobuf bindings from
  `proto/beacon/v1/beacon.proto`; `TestProtoWire_WatchEndToEnd` exercises the
  generated wire path.
- `pkg/gossip/memory.go` remains an in-process test/simulation transport, while
  production AP mode uses `pkg/gossip/udp.go` with bounded multi-hop infection,
  duplicate suppression, and anti-entropy messages.
- Console `ConsistencyLab` and `PropagationTimeline` use live API/SSE data;
  the synthetic consistency lab is explicitly opt-in with `--enable-lab`.

## Production artifacts added

- Governance: `CODEOWNERS`, `MAINTAINERS.md`, `GOVERNANCE.md`, `SUPPORT.md`,
  `ROADMAP.md`, `ADOPTERS.md`, `CONTACTS.md`
- GitHub: `dependabot.yml`, `PULL_REQUEST_TEMPLATE.md`,
  `ISSUE_TEMPLATE/{bug_report,feature_request,config}.yml`,
  `workflows/{release,codeql,scorecard,pages}.yml`, `labeler.yml`
- Supply chain: `.goreleaser.yml`, SBOM + cosign + SLSA in `release.yml`
- Docs site: `mkdocs.yml` (GitHub Pages) + `docs/index.md`
- Ops: `docs/SLO.md`, `docs/DISASTER-RECOVERY.md`, `docs/runbooks/*`,
  `docs/adr/*`, `deploy/k8s/*`, `deploy/prometheus/rules.yml`,
  `api/openapi.yaml`
