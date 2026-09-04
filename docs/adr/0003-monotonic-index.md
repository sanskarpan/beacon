# ADR 0003 — Monotonic index + no-op suppression

- Status: accepted. Date: 2026-09-04.
- Context: watchers block on `ModifyIndex`; spurious bumps DDoS the fleet.
- Decision: global + per-service monotonic index; `UpdateHealth` bumps ONLY on
  status change; registration storms coalesce via 50ms `IndexBatcher`;
  future-index resets to 0; timeout returns state, not error.
- Consequences: watchers converge without observing every intermediate step;
  restore/snapshot paths must preserve monotonicity (`max`).
