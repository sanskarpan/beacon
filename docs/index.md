# beacon docs

Published to GitHub Pages on every push to `main`
(`.github/workflows/pages.yml`, MkDocs Material).

> Visual showcase: [Demo](DEMO.md) (topology, propagation timeline, consistency lab).
> Assets live in `docs/assets/` (`demo.gif`, `topology.svg`, `timeline.svg`, `consistency-lab.svg`).

Start here:

- [Demo](DEMO.md) — GIF, screenshots, 60-second record script
- [Installation](INSTALL.md) — Homebrew / go install / Docker / source
- [Architecture](ARCHITECTURE.md) — agent/server split, monotonic index, storm defences
- [Comparison](COMPARISON.md) — beacon vs Consul / etcd / Eureka / CoreDNS
- [Configuration](CONFIGURATION.md) — flags, env vars, defaults
- [API](API.md) — HTTP/gRPC/DNS/xDS surface
- [Migration](MIGRATION.md) — Consul → beacon mapping
- [Benchmarks](BENCHMARKS.md) — reproducible numbers
- [Cookbook](COOKBOOK.md) — recipes
- [Deployment](DEPLOYMENT.md) — Docker, Compose, Kubernetes (`deploy/`)
- [Observability](OBSERVABILITY.md) — events, metrics, tracing
- [SLOs](SLO.md) — objectives, error budgets, alerts
- [Disaster Recovery](DISASTER-RECOVERY.md) — backup/restore, partition heal
- [Runbooks](runbooks/RUNBOOK.md) — incident index
- [ADRs](adr/0001-ap-vs-cp.md) — why AP vs CP, agent-local checks, monotonic index
- [Production audit 2026-09-04](PRODUCTION_AUDIT_2026_09_04.md) — stub inventory and fixes
- [Blog](blog/index.md) — agent-local checks, gossip+streaming vs health+DNS
- [Good First Issues](GOOD_FIRST_ISSUES.md) — scoped starter tasks with acceptance criteria
- [Module Alignment](MODULE_ALIGNMENT.md) — plan to align go.mod to `github.com/sanskarpan/beacon` on next major (no rename now)

The headline measurement: `beacon bench propagate` — gossip+streaming
converges ~2s vs health+DNS ~45s (22×). See [Propagation](PROPAGATION.md).
