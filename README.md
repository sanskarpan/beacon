# beacon

[![CI](https://github.com/sanskar/beacon/actions/workflows/ci.yml/badge.svg)](https://github.com/sanskar/beacon/actions/workflows/ci.yml)
[![Go Report](https://goreportcard.com/badge/github.com/sanskar/beacon)](https://goreportcard.com/report/github.com/sanskar/beacon)
[![Go Reference](https://pkg.go.dev/badge/github.com/sanskar/beacon.svg)](https://pkg.go.dev/github.com/sanskar/beacon)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Coverage](https://img.shields.io/badge/coverage-80%25-green)](https://github.com/sanskar/beacon/actions)

**A Consul-class service discovery system** — catalog with monotonic indexing, TTL leases, agent-local health checking with hysteresis, gossip-driven propagation, watch/notify, DNS + HTTP + gRPC interfaces, an xDS control plane, and a client SDK with resolver + balancer.

Beacon integrates two prior projects:

| Project | Role in beacon |
|---|---|
| **Gossip-Protocol** (SWIM) | Membership + piggybacked catalog deltas. Node failure → instances critical in ~2s |
| **gRPC-Service-with-Interceptors** | Interceptor chain; beacon adds `OutcomeReporter` for passive/outlier health |

Two consistency backends run side by side:

- **AP** — gossip-replicated catalog (available under partition; may serve stale endpoints)
- **CP** — Raft-replicated catalog (linearizable; minority rejects writes)

The **measurement is the deliverable**. The stale-endpoint window — how long after an instance dies until clients stop routing to it — is measured end-to-end per configuration.

## Propagation comparison (headline)

```
| Configuration       | p50     | p99     | max     |
|---------------------|---------|---------|---------|
| gossip+streaming    | 2.011s  | 2.011s  | 2.011s  |
| health+streaming    | 15.061s | 15.061s | 15.061s |
| health+blocking     | 17.56s  | 17.56s  | 17.56s  |
| health+dns          | 45.15s  | 45.15s  | 45.15s  |
```

Roughly **22×** between the fast path (gossip + streaming watch) and the slow path (health check + DNS). That spread is the engineering argument for this design.

Run it yourself:

```bash
go run ./cmd/beacon bench propagate
```

## Quickstart

```bash
# Build
make build

# Run control plane (AP mode)
./bin/beacon-server --http :8500 --dns :8600 --consistency ap --node server-1

# Register a service
./bin/beacon register --name payments --port 8080 --tag v2

# List / watch
./bin/beacon services
./bin/beacon instances payments --passing
./bin/beacon watch payments

# Scenarios (virtual clock, no wall-time wait)
./bin/beacon sim all
./bin/beacon sim flap
./bin/beacon sim rollout
./bin/beacon sim zone-failure
./bin/beacon sim test/scenario/flap.yaml   # declarative YAML

# 3-service demo (web → api → db)
./bin/demo   # GET :8090/web /api /services

# Console
cd console && bun install && bun run dev
# → http://localhost:5173  (proxies API to :8500)
```

## Architecture (short)

```
Console (React) ──SSE──► beacon-server
                           ├─ Catalog (monotonic index)
                           ├─ Watch registry (blocking + streaming)
                           ├─ xDS ADS (CDS→EDS→LDS→RDS)
                           ├─ DNS :8600 · HTTP :8500 · gRPC :8502
                           └─ Store: AP gossip | CP Raft
                                    ▲
                         anti-entropy / register
                                    │
                           beacon-agent (per node)
                           ├─ local state ★ authoritative
                           ├─ health checks over loopback
                           └─ SWIM membership
```

**Health checks run on the agent, over loopback — never centrally.**  
10k instances × 1 check/5s = 2k checks/s from a central plane; agent-local is ~10 checks per agent. Semantics matter too: agent-local answers “can this instance serve”, central answers “can the control plane reach it”.

**The agent’s local state is authoritative.** If the catalog is wiped, agents repopulate it within one anti-entropy interval. If an operator deletes an agent-owned instance from the catalog, the agent puts it back.

## Package map

| Path | Purpose |
|---|---|
| `pkg/catalog` | Types, in-memory store, index batching, leases |
| `pkg/health` | Hysteresis, check runners (HTTP/TCP/gRPC/exec/TTL/alias), outlier |
| `pkg/agent` | Local state, anti-entropy, agent-local checks |
| `pkg/gossip` | `Membership` seam + in-process SWIM fabric for tests/sim |
| `pkg/store/gossip` | AP backend; node-fail → critical |
| `pkg/store/raft` | CP backend; quorum writes, stale reads |
| `pkg/watch` | Blocking queries, cache, staggered fan-out |
| `pkg/api/httpapi` | Consul-style HTTP + SSE |
| `pkg/api/dns` | A/AAAA/SRV, TTL=0, TC bit |
| `pkg/sdk` | Client, `beacon://` resolver, never-empty addresses |
| `pkg/lb` | RR, smooth WRR, least-request, P2C, ring-hash, locality |
| `pkg/xds` | ADS SotW + Delta, NACK non-amplification |
| `pkg/mesh` | SPIFFE CA, short-lived certs, intentions |
| `pkg/sim` | Scenario runner + propagation measurement |
| `console/` | Mesh topology, propagation timeline, labs |

## Design rules (non-negotiable)

1. **Do not reimplement gossip or gRPC interceptors** — depend on narrow interfaces.
2. **Every timer goes through injectable `Clock`** — sim cannot control bare `time.After`.
3. **`TraceID` from day zero** — register → agent → catalog → gossip → watch → client.
4. **Health checks are agent-local.**

## Tests

```bash
make test
make test-race
make sim
```

Key properties under test:

- `UpdateHealth` does not bump the index when status is unchanged
- Hysteresis: flapping produces **zero** state transitions
- `MaxEjectionPercent` respected when 100% of endpoints fail
- Agent puts catalog-deleted instances back; repopulates after wipe
- Node failure via membership marks instances critical
- Resolver never pushes an empty address list (panic mode)
- xDS resources pushed in dependency order; NACK does not resend
- Propagation: gossip+streaming ≪ health+DNS

## Docs

- [ARCHITECTURE](docs/ARCHITECTURE.md)
- [CONSISTENCY](docs/CONSISTENCY.md)
- [HEALTH](docs/HEALTH.md)
- [PROPAGATION](docs/PROPAGATION.md)
- [WATCH](docs/WATCH.md)
- [XDS](docs/XDS.md)
- [INTEGRATION](docs/INTEGRATION.md)
- [ANTI_ENTROPY](docs/ANTI_ENTROPY.md)

## Configuration

| Flag / Env | Default | Purpose |
|---|---|---|
| `--http` / `BEACON_HTTP` | `:8500` | HTTP API (`/v1/*`, `/health`, `/metrics`, `/v1/events`) |
| `--dns` / `BEACON_DNS` | `:8600` | DNS (UDP+TCP) `A/AAAA/SRV`; TTL=0, `TC` on UDP 512 |
| `--grpc` / `BEACON_GRPC` | `:8502` | gRPC `Discovery` (`Watch`/`WatchMulti`) + keepalive + `GracefulStop` |
| `--consistency` | `ap` | `ap` (gossip) or `cp` (Raft `ReadIndex` linearizable) |
| `--join` / `BEACON_JOIN` | — | SWIM seed `addr:port` |
| `--data-dir` | `./data` | Agent persist `services.json` |
| `--otel-endpoint` / `BEACON_OTEL_ENDPOINT` | — | OTLP gRPC endpoint |
| `BEACON_API_URL` | `http://localhost:8500` | Console proxy target (vite `server.proxy`) |

## API

**HTTP (Consul-style):**
- `PUT /v1/agent/service/register` · `PUT /v1/agent/service/deregister/:id`
- `GET /v1/catalog/service/:name?index=&wait=&passing=&tag=&filter=&consistent=&stale=` → `X-Beacon-Index`, `X-Beacon-Stale`
- `GET /v1/health/service/:name` · `GET /v1/catalog/services` · `GET /v1/agent/members`
- `GET /v1/watch/stats` · `GET /v1/events` (SSE `data: {Event}`) · `GET /metrics` · `/health` · `/ready`

**gRPC:**
- `Discovery.Watch(WatchRequest) → stream WatchEvent` (snapshot+delta)
- `Discovery.WatchMulti(stream WatchMultiRequest) → stream WatchEvent` (bidirectional)

**DNS (port 8600):**
- `payments.service.beacon` `A/AAAA/SRV` · `v2.payments.service.beacon` tag filter · `payments.service.dc1.beacon` datacenter

**xDS:**
- ADS `SotW` + `Delta` (`CDS→EDS→LDS→RDS` add, `LDS→RDS→EDS→CDS` remove), ACK/NACK via `error_detail`

See `docs/API.md`, `docs/CONFIGURATION.md`, `docs/DEPLOYMENT.md`, `docs/OBSERVABILITY.md`.

## License

[MIT](LICENSE) — see `LICENSE`. `CHANGELOG.md` for version history. Security disclosures per `SECURITY.md`.
