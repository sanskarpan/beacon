# Health Checking

## Active (agent-local probes)

| Type | Passing | Warning | Critical |
|---|---|---|---|
| HTTP | 2xx | **429** | else |
| TCP | dial ok | — | fail |
| gRPC | SERVING | — | else |
| exec | exit 0 | exit 1 | else |
| TTL | fresh push | — | silence |
| alias | mirrors another service | | |

Exec checks kill the **process group** on timeout (negative PID), not just the process — otherwise a script that spawns `curl` leaks children forever.

## Hysteresis

Defaults: **3** consecutive failures → critical; **2** consecutive passes → returning.

Without this, a flapping instance produces on every interval: catalog write + index bump + watcher fan-out + client pool rebuild. One sick instance can dominate control-plane load.

Test: alternating pass/fail for 100 intervals → **0** transitions.

## Passive (outlier detection)

Fed by the client SDK’s `Done` callback / `OutcomeReporter` interceptor.

**`MaxEjectionPercent` (default 10) is a hard cap.** If a shared dependency is down, every endpoint errors. Ejecting all of them turns a degradation into a total outage. The cap keeps 90% of the pool no matter how bad the evidence looks.

Ejection duration grows with offences: `base × ejection_count`.
