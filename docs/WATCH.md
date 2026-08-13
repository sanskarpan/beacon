# Watch / Notify

## Blocking queries (Consul-style)

```
GET /v1/health/service/payments?index=4821&wait=5m
X-Beacon-Index: 4830
```

Three production details:

1. **Jitter the wait by ±16%** — without it, clients that connected together time out together and *converge on the same phase over time*, making herds worse.
2. **Guard against a future index** — after restart/snapshot restore, blocking forever is a silent permanent failure. Reset to 0 and return now.
3. **Timeout returns current state, not an error** — the client loop stays uniform.

## Streaming

First message is a full snapshot; subsequent messages are deltas. `WatchMulti` is bidirectional for multi-service clients.

## Watch cache

Ring buffer (default 1,000 events). If the client’s index ages out → `ErrIndexCompacted` (HTTP 410 semantics). **Never silently skip events** — a skipped deregister leaves a ghost in the client forever.

## Herd defences

- singleflight on resolves  
- batched index bumps  
- staggered fan-out (≤500ms spread)  
- slow consumers get resync, never block the fan-out  
