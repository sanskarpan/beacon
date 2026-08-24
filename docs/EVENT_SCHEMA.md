# Event Schema — JSONL Hop Events (TODO-008)

Every event published on the bus is a JSONL line with this schema.
The console `PropagationTimeline` view reconstructs swimlanes from these events.

## Event JSON Schema

```json
{
  "kind": "string",
  "trace_id": "string",
  "service": "string",
  "instance": "string",
  "node": "string",
  "from": "string",
  "to": "string",
  "index": 0,
  "detail": "string",
  "elapsed": "0ms",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | string | yes | Event type (see below) |
| `trace_id` | string | yes | Shared trace identifier linking all hops |
| `service` | string | no | Service name |
| `instance` | string | no | Instance ID |
| `node` | string | no | Node name |
| `from` | string | no | Previous state (for transitions) |
| `to` | string | no | New state (for transitions) |
| `index` | uint64 | yes | Catalog index at time of event |
| `detail` | string | no | Free-form detail string |
| `elapsed` | string | no | Duration since event creation (e.g. "3ms") |
| `timestamp` | string | yes | ISO 8601 UTC timestamp |

## Event Kinds

### Registration Path
| Kind | Description |
|---|---|
| `instance.registered` | Instance registered in catalog |
| `instance.deregistered` | Instance removed from catalog |
| `health.changed` | Instance health status changed |
| `check.executed` | Health check completed |

### Gossip / Propagation Path
| Kind | Description |
|---|---|
| `gossip.delta` | Gossip delta sent/received |
| `gossip.fullsync` | Full catalog sync completed |
| `gossip.converged` | All nodes converged (includes elapsed) |
| `gossip.infection` | Infection round completed |

### Watch Path
| Kind | Description |
|---|---|
| `watch.opened` | Watch subscription opened |
| `watch.notified` | Watcher notified of change |
| `watch.compacted` | Watcher index too old, re-list required |

### xDS Path
| Kind | Description |
|---|---|
| `xds.push` | xDS push sent to Envoy |
| `xds.ack` | Envoy ACK received |
| `xds.nack` | Envoy NACK received |

### Consistency Lab
| Kind | Description |
|---|---|
| `lab.partition` | Network partition simulated |
| `lab.heal` | Network partition healed |

### System
| Kind | Description |
|---|---|
| `index.bumped` | Catalog index incremented |
| `herd.detected` | Notification herd detected |

## TraceID Hop Reconstruction

The console traces a single `trace_id` through:

1. **Registration**: `instance.registered` with trace_id
2. **Catalog bump**: `index.bumped` (shared trace_id)
3. **Gossip push**: `gossip.delta` → `gossip.infection` (piggyback)
4. **Convergence**: `gossip.converged` (with elapsed time)
5. **Watch fan-out**: `watch.notified` (per-watcher)
6. **Client receive**: Last event in the chain

The total elapsed from `instance.registered` to `watch.notified` is the
propagation latency measured by the `PropagationTimeline` view.

## Export Format

JSONL (one JSON object per line) for `EvConverged` events. The sim runner
writes these to `tmp/sim/propagation.json` and the console fetches via
`/v1/events/jsonl?trace_id=<id>`.
