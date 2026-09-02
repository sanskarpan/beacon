# Observability

## Metrics (`/metrics` Prometheus)

| Metric | Labels | Description |
|---|---|---|
| `beacon_catalog_index` | `service` | Monotonic `ModifyIndex` |
| `beacon_watch_open` | `service` | Open watchers |
| `beacon_watch_notified_total` | `service` | Notifications |
| `beacon_xds_push_total` | `type` | xDS pushes |
| `beacon_xds_nack_total` | `type` | NACKs (error_detail) |
| `beacon_gossip_delta_total` | `node` | Deltas broadcast |
| `beacon_health_check_total` | `check, status` | Check executions |
| `beacon_outlier_ejected` | `service` | Ejections |

## Tracing (OTel)

Set `BEACON_OTEL_ENDPOINT=otel:4317` (OTLP gRPC). Spans: `register → agent → catalog → gossip → watch → resolve`. `TraceID` propagates via `X-Beacon-TraceID` and `events.Event.TraceID`.

Console `Propagation Timeline` consumes `EvConverged` + `EvWatchNotified`.

## Events (`/v1/events` SSE)

```bash
curl -N http://localhost:8500/v1/events
# data: {"kind":"instance.registered","service":"payments","index":42,"trace_id":"..."}
curl -N "http://localhost:8500/v1/events?trace_id=abc"
```

JSONL: `beacon sim all --jsonl trace.jsonl`

## Logs

Structured via `zap` (indirect). Levels via `BEACON_LOG_LEVEL`. Key events: `EvInstanceRegistered`, `EvHealthChanged`, `EvFlappingDetected`, `EvConverged`, `EvXDSNack`.

## Dashboards & Alerts

Example PromQL:

```promql
# Stale endpoint window (p99)
histogram_quantile(0.99, beacon_propagation_seconds_bucket)
# NACK rate
rate(beacon_xds_nack_total[5m]) > 0
# Watch herd
beacon_watch_notified_total - ignoring(service) group_left
```

Runbook: `docs/RUNBOOK.md` (future) — on `EvXDSNack`, check `error_detail` and rollback config; on `EvFlappingDetected`, inspect `Health Inspector`.
