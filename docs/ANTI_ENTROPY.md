# Anti-Entropy

## The agent is source of truth

For services registered with an agent, **local state is authoritative**. The catalog is a replica.

Consequences:

1. **Catalog loss** (server wipe, bad restore) → agents repopulate within one sync interval. Self-healing from complete control-plane data loss.
2. **Operator deletes** an agent-owned instance from the catalog → agent puts it back on next sync. Document this loudly.
3. **Bidirectional reconcile**: catalog-only entries for this node are removed; agent-only entries are added.

## Sync interval

Scales with cluster size, jittered per agent so 10k agents don’t lockstep:

| Cluster size | Interval |
|---|---|
| ≤128 | 1 min |
| ≤512 | 5 min |
| ≤2048 | 10 min |
| ≤8192 | 20 min |
| else | 30 min |

Local changes trigger an **immediate** sync (rate-limited), not only the interval.
