# Migration (Consul → beacon)

Beacon is Consul-style on purpose: same `/v1/*` shapes, same DNS names.

| Consul | beacon |
|---|---|
| `PUT /v1/agent/service/register` | `PUT /v1/agent/service/register` (same) |
| `GET /v1/catalog/service/:name?passing=1` | same + `X-Beacon-Index`, `X-Beacon-Stale` |
| `GET /v1/health/service/:name` | same |
| Blocking `?index=&wait=` | same + jittered timeout, staggered fan-out |
| DNS `name.service.consul` | DNS `name.service.beacon` on `:8600`, tag `v2.name.service.beacon` |
| Connect intentions | `pkg/mesh` intentions + SPIFFE SDS |

## Steps

1. Point DNS for `*.service.beacon` at beacon `:8600` alongside Consul.
2. Dual-register via agent, then shift reads service-by-service (`?passing=1` first).
3. Compare `bench propagate` against your Consul p99 before cutting writes.
4. AP first; switch single writes to `--consistency cp` only where linearizability is required.

See [Configuration](CONFIGURATION.md) for flags/env and [Deployment](DEPLOYMENT.md) for rollout.
