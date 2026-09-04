# Service-level objectives

## SLOs

| Signal | Objective | Window | Alert |
|---|---|---|---|
| Stale-endpoint window (gossip+streaming p99) | < 3s | 5m | `StaleWindowP99 > 3` page |
| Blocking-query wake → client notified p99 | < 20ms | 5m | `WatchWakeP99 > 0.02` ticket |
| HTTP `/health` availability | 99.9% | 30d | burn-rate page |
| xDS NACK rate | < 0.1% of pushes | 1h | `NACKRatio > 0.001` ticket |
| DNS p99 latency | < 5ms (target 2ms) | 5m | `DNSP99 > 0.005` ticket |

## Error budgets

- Availability 99.9% → 43m/month. Burn-rate alerts in
  `deploy/prometheus/rules.yml`. Freeze deploys when the 1h burn exceeds 14×.
- Stale-window budget: every `SimPartition`/`bench propagate` CI run must keep
  gossip+streaming p99 under the gate (`TestCIConvergenceGate`,
  `BEACON_CONV_GATE_MS`, default 2000ms).

## Instrumentation

- Metrics: `GET /metrics` (Prometheus), per-hop trace latencies via `TraceID`
  events (`docs/EVENT_SCHEMA.md`).
- Dashboards: propagation timeline (console View 2), consistency divergence
  counter (console View 6), xDS ACK/NACK panel (console View 5).
