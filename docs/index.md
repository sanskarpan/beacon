# beacon docs

Published to GitHub Pages on every push to `main`
([`pages.yml`](../.github/workflows/pages.yml), MkDocs Material).

Start here:

- [Architecture](ARCHITECTURE.md) — agent/server split, monotonic index, storm defences
- [Configuration](CONFIGURATION.md) — flags, env vars, defaults
- [API](API.md) — HTTP/gRPC/DNS/xDS surface
- [Deployment](DEPLOYMENT.md) — Docker, Compose, Kubernetes (`deploy/`)
- [Observability](OBSERVABILITY.md) — events, metrics, tracing
- [SLOs](SLO.md) — objectives, error budgets, alerts
- [Disaster Recovery](DISASTER-RECOVERY.md) — backup/restore, partition heal
- [Runbooks](runbooks/RUNBOOK.md) — incident index
- [ADRs](adr/0001-ap-vs-cp.md) — why AP vs CP, agent-local checks, monotonic index
- [Production audit 2026-09-04](PRODUCTION_AUDIT_2026_09_04.md) — stub inventory and fixes

The headline measurement: `beacon bench propagate` — gossip+streaming
converges ~2s vs health+DNS ~45s (22×). See [Propagation](PROPAGATION.md).
