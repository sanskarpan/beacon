# Benchmarks

Reproducible numbers only. No hand-waved claims.

## Propagation (headline)

```bash
go run ./cmd/beacon bench propagate
```

| Configuration | p50 | p99 | max |
|---|---|---|---|
| gossip+streaming | 2.011s | 2.011s | 2.011s |
| health+streaming | 15.061s | 15.061s | 15.061s |
| health+blocking | 17.56s | 17.56s | 17.56s |
| health+dns | 45.15s | 45.15s | 45.15s |

~22× fast path vs slow path. Details in [Propagation](PROPAGATION.md).

## LB / catalog microbenchmarks

```bash
go test -bench=. -benchmem ./pkg/lb/... ./pkg/catalog/... -count=1
```

Record `ns/op`, `B/op`, `allocs/op` per package here on every release.
CI runs `bench gate (non-blocking)` via `TestCIConvergenceGate`; sim via `go run ./cmd/beacon sim flap`.
