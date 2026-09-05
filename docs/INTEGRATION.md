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

`pkg/gossip.MemoryMembership` is the in-process fabric for tests and sim.
Production currently wires `pkg/gossip.UDP`, which provides bounded multi-hop
infection and anti-entropy behind the same interface. The repository still
contains a replaceable SWIM adapter seam, but `external/gossip-system` remains
a local stub until the upstream module exposes a compatible public transport.

**Payoff:** when SWIM declares node N dead, every instance on N is marked critical immediately (~2s vs ~15s for health checks).

Catalog deltas piggyback on the existing gossip stream (`Broadcast` / `OnBroadcast`). Payload > 512 bytes falls back to anti-entropy.

## gRPC-Service-with-Interceptors

The interceptors project supplies auth, logging, metrics, tracing, panic
recovery. The production gRPC server now installs its server interceptor chain;
Beacon's bearer-token and stream-drain interceptors wrap that chain.

beacon adds:

```go
func (c *Client) OutcomeReporter() grpc.UnaryClientInterceptor
```

which records per-endpoint outcomes into `outlier.Detector`. Picker `Done` callbacks do the same. Applications do not know about passive health checking.

## Upstream integration boundary

The `replace` directives in `go.mod` point at `external/gossip-system` and
`external/grpc-service` so CI builds without network access. To replace the
local seams with upstream modules:

1. Publish (or vendor) the real SWIM + interceptors modules.
2. Implement the thin adapter: SWIM `Join/Leave/Members` → `Membership`
   (`Subscribe` failure events, `Broadcast` piggyback ≤512B); interceptor
   chain + beacon `OutcomeReporter` appended last.
3. Delete the `replace` lines, `go mod tidy`, run `go test -race ./...`.
4. Gate with `TestConvergenceHopsScaleLogN` (O(log N)) and
   `TestInterceptors_OrderAndMetrics` (chain order).
