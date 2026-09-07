# Comparison

When to pick beacon, and when to stay on what you have.

| Capability | beacon | Consul | etcd | Eureka | CoreDNS |
|---|---|---|---|---|---|
| Service catalog + health | ✅ agent-local checks, hysteresis | ✅ agent checks | ❌ KV only | ✅ heartbeat/lease | ❌ DNS only |
| Gossip membership (SWIM) | ✅ in-process fabric + `Membership` seam | ✅ Serf | ❌ Raft only | ❌ peer replication | ❌ |
| AP vs CP in one binary | ✅ `--consistency ap\|cp` | AP-ish (stale reads) + CP (Raft) split | CP only | AP only | n/a |
| Stale-endpoint measurement | ✅ `bench propagate` p50/p99/max | ❌ manual | ❌ | ❌ | ❌ |
| Watch/streaming | ✅ blocking + SSE + gRPC stream | ✅ blocking + streaming | ✅ watch | ✅ delta/fetch | ❌ |
| DNS | ✅ A/AAAA/SRV, TTL=0, TC bit | ✅ | ❌ | ❌ | ✅ |
| xDS control plane | ✅ ADS SotW + Delta | ✅ (separate) | ❌ | ❌ | ❌ |
| Client SDK + LB | ✅ resolver, never-empty, RR/WRR/P2C/ring | ✅ (connect/native) | ❌ | ✅ Ribbon-era | ❌ |
| mTLS / intentions | ✅ SPIFFE SDS + intentions | ✅ Connect | ❌ (TLS only) | ❌ | ❌ |

## When to choose beacon

- You want **AP and CP side-by-side** to compare stale-window vs linearizability.
- You care about the **measured propagation spread** (gossip+streaming ~2s vs health+DNS ~45s).
- You want agent-local health semantics ("can this instance serve") over central probing.

## When not to

- Single-purpose KV only → etcd is simpler.
- DNS-only service discovery → CoreDNS is enough.
- Managed control plane already standardized on Consul → use [Migration](MIGRATION.md) only for evaluation.
