# Integration with existing projects

## Gossip-Protocol (SWIM)

beacon depends on `pkg/gossip.Membership`, not on `gossip-system` internals:

```go
type Membership interface {
    Members() []Member
    Size() int
    LocalName() string
    Join(seeds []string) (int, error)
    Leave() error
    Subscribe(ch chan<- MemberEvent)
    Unsubscribe(ch chan<- MemberEvent)
    Broadcast(payload []byte) error
    OnBroadcast(fn func(from NodeID, payload []byte))
}
```

`pkg/gossip.MemoryMembership` is the in-process fabric for tests and sim. Production wires the SWIM project behind this interface (the upstream module uses `internal/`, so a thin adapter package in that repo — or a replaceable vendor — is the integration path). In this repo `go.mod` has `replace` directives to local stubs `external/gossip-system` and `external/grpc-service` for CI; remove them when wiring the real modules.

**Payoff:** when SWIM declares node N dead, every instance on N is marked critical immediately (~2s vs ~15s for health checks).

Catalog deltas piggyback on the existing gossip stream (`Broadcast` / `OnBroadcast`). Payload > 512 bytes falls back to anti-entropy.

## gRPC-Service-with-Interceptors

The interceptors project supplies auth, logging, metrics, tracing, panic recovery.

beacon adds:

```go
func (c *Client) OutcomeReporter() grpc.UnaryClientInterceptor
```

which records per-endpoint outcomes into `outlier.Detector`. Picker `Done` callbacks do the same. Applications do not know about passive health checking.

## Production swap (remove local stubs)

The `replace` directives in `go.mod` point at `external/gossip-system` and
`external/grpc-service` so CI builds without network access. For production:

1. Publish (or vendor) the real SWIM + interceptors modules.
2. Implement the thin adapter: SWIM `Join/Leave/Members` → `Membership`
   (`Subscribe` failure events, `Broadcast` piggyback ≤512B); interceptor
   chain + beacon `OutcomeReporter` appended last.
3. Delete the `replace` lines, `go mod tidy`, run `go test -race ./...`.
4. Gate with `TestConvergenceHopsScaleLogN` (O(log N)) and
   `TestInterceptors_OrderAndMetrics` (chain order).
