# Flapping instance

One sick instance can generate more control-plane load than the fleet via
catalog writes + index bumps + watcher wakeups. Hysteresis must absorb it.

1. Confirm: `health.flapping` events + `transitions == 0` in
   `TestHysteresis_FlappingProducesZeroTransitions` pattern.
2. Check thresholds: `FailuresBeforeCritical` (default 3),
   `SuccessesBeforePassing` (default 2). Warning never resets counters.
3. If churning persists: cordon via `beacon maint --enable`, fix the check
   (agent-local, over loopback), then re-enable.
4. Shared-dependency outage (all instances failing): do NOT eject —
   `MaxEjectionPercent` (default 10) caps ejection; the problem is downstream.
