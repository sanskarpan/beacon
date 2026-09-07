# Cookbook

Copy-pasteable, API-accurate recipes.

## Flapping instance (hysteresis holds)

```bash
./bin/beacon-server --http :8500 --dns :8600 --consistency ap --node server-1
./bin/beacon register --name payments --port 8080
go run ./cmd/beacon sim flap   # expect zero state transitions under flap
```

## Rolling deploy without thundering herd

Batched index bumps (50ms) + jittered watch timeouts (±16%) + staggered fan-out (≤500ms)
mean 1,000 deploys wake watchers once. Verify with `GET /v1/watch/stats`.

## Zone failure

```bash
go run ./cmd/beacon sim zone-failure
```

## Partition: AP vs CP

```bash
BEACON_CONSISTENCY=ap docker compose up --build   # stale reads allowed
BEACON_CONSISTENCY=cp docker compose up --build   # minority rejects writes
```

See [Runbooks](runbooks/RUNBOOK.md), [SLOs](SLO.md), [Disaster Recovery](DISASTER-RECOVERY.md).
