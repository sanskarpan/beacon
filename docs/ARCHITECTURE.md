# Architecture

## Agent / server split

```
beacon-server  — catalog, watch registry, DNS, HTTP, gRPC, xDS
beacon-agent   — local registrations, health checks, anti-entropy, gossip member
beacon-sdk     — resolver, balancer, interceptors, lease renewal
```

## Why health checks are agent-local

**Scaling:** 10,000 instances × 1 check / 5s = 2,000 checks/s from a central control plane, over the network, to every corner of the fleet. Agent-local: each agent checks ~10 instances over loopback.

**Semantics:** Agent-local answers “can this instance serve traffic”. Central checks answer “can the control plane reach this instance”. Those diverge under network partitions between the control plane and the data plane — and the second question is the wrong one for load balancing.

## Monotonic catalog index

Every mutation that changes observable state bumps a global (and per-service) `ModifyIndex`. Watchers block until `ModifyIndex > lastSeen`.

Critical micro-rule: **`UpdateHealth` only bumps when status actually changes.** A healthy cluster with 10k instances checked every 5s would otherwise generate 2k index bumps/s and wake every watcher continuously.

## Registration-storm defences

1. **Batched index bumps** (50ms window) — 1,000 deploys wake watchers once, not 1,000 times  
2. **Jittered watch timeouts** (±16%) — prevents phase-locked reconnect herds  
3. **Staggered fan-out** — notifications spread over up to 500ms  
4. **singleflight** on identical concurrent resolves  
5. **Rate-limit** registration per client (HTTP 429 + Retry-After)

## Data flow (register → resolve)

```
SDK Register(traceID)
  → agent local state (authoritative)
  → catalog write (index N)
  → gossip CatalogDelta piggyback
  → other servers ApplyDelta
  → watch registry Notify (staggered)
  → client resolver UpdateState
  → picker serves traffic
```
