# Consistency: AP vs CP

Both backends implement the same `CatalogStore` interface. Select with `--consistency=ap|cp`.

## AP (gossip)

- Writes accepted by any server, propagated by infection-style piggyback
- Reads always available locally
- During partition: **both sides serve**; views diverge; converge on heal via incarnation
- Failure mode: client may route to a recently-deregistered instance (one failed request + retry)

## CP (Raft)

- Writes go to the leader, committed by quorum
- Linearizable reads on leader (ReadIndex); stale reads on any server with `X-Beacon-Last-Contact`
- During partition: **minority cannot write** (and cannot linearizable-read)
- Failure mode: registration fails entirely on the minority side (outage for new instances)

## The trade, stated plainly

| | Cost of being wrong |
|---|---|
| **AP** | Stale endpoint → one failed request |
| **CP** | Unavailable registry → cannot register / discover at all |

For service discovery, AP is usually right. This is why Consul puts **membership** in gossip (AP) and the **catalog** in Raft (CP), and why Eureka is deliberately AP end-to-end.

## Console

The Consistency Lab shows both modes with a partition toggle and a live divergence counter.
