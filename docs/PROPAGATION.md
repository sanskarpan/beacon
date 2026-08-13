# The Stale-Endpoint Window

An instance dies. How long until clients stop sending it traffic?

```
t0  instance crashes
t1  DETECTION
      health check:  interval × failures_before  (5s × 3 = 15s)
      gossip:        ~2 SWIM protocol periods    (~2s)     ← 7× faster
t2  PROPAGATION to catalog / peers
      agent check → catalog: ~50ms
      gossip delta: O(log N) rounds, ~1–3s for 100 nodes
t3  NOTIFICATION to watchers
      streaming:   ~10ms
      blocking:    up to wait timeout
      DNS:         up to record TTL / stub cache  ← the slow one
t4  CLIENT applies update
      gRPC resolver: immediate
      pooled HTTP:   until conn retired
```

## Measured comparison

See `beacon bench propagate` and the console Propagation Timeline.

The **~20–30×** spread between gossip+streaming and health+DNS is the entire engineering argument for this project — and almost nobody measures it.

## TraceID

Every registration carries a `TraceID` from the SDK call through agent → catalog → gossip → watch → client, with a timestamp at every hop. `EvConverged` fires when the last observer has the change.
