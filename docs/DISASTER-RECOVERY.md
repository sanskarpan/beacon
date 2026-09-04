# Disaster recovery

## Backup

- Catalog snapshots: `Store.Snapshot()` → JSON; schedule hourly to object
  storage. Restore path is monotonic (`Restore` takes `max(snapshot, live)`)
  so an old snapshot never rewinds the index.
- Agent local state persists to `--data-dir/services.json`; agents repopulate
  a wiped catalog within one anti-entropy interval (authority rule).

## Restore

1. Stop writers (pause deploys / registration storms).
2. Restore newest snapshot to one server, restart it.
3. Let agents anti-entropy sync (≤1 interval); verify `beacon services`
   counts and `X-Beacon-Index` monotonicity.
4. Rejoin remaining servers; confirm gossip `EvConverged` and watch
   `ErrIndexCompacted == 0`.

## Partition heal

- AP: views diverge then merge on heal via incarnation + tombstones;
  watch the console divergence counter return to 0.
- CP: minority rejects writes (`ErrNoQuorum`) during partition; on heal the
  leader re-reads (`ReadIndex`) before serving linearizable reads.

## RTO/RPO

- RTO ≤ 1 anti-entropy interval for catalog repopulation; RPO = last snapshot
  (agent state closes the gap for agent-owned services).
