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

`pkg/gossip.MemoryMembership` is the in-process fabric for tests and sim. Production wires the SWIM project behind this interface (the upstream module uses `internal/`, so a thin adapter package in that repo — or a replaceable vendor — is the integration path).

**Payoff:** when SWIM declares node N dead, every instance on N is marked critical immediately (~2s vs ~15s for health checks).

Catalog deltas piggyback on the existing gossip stream (`Broadcast` / `OnBroadcast`). Payload > 512 bytes falls back to anti-entropy.

## gRPC-Service-with-Interceptors

The interceptors project supplies auth, logging, metrics, tracing, panic recovery.

beacon adds:

```go
func (c *Client) OutcomeReporter() grpc.UnaryClientInterceptor
```

which records per-endpoint outcomes into `outlier.Detector`. Picker `Done` callbacks do the same. Applications do not know about passive health checking.
