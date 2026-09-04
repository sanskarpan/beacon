# Runbooks

| Symptom | Runbook |
|---|---|
| Partition / split-brain | [gossip-partition](gossip-partition.md) |
| xDS NACK loop | [xds-nack](xds-nack.md) |
| Flapping instance churning catalog | [flapping](flapping.md) |

General: confirm scope via `beacon members` + console Consistency Lab,
check `/metrics` + `EvConverged`/`EvXDSNack` in `GET /v1/events`, then follow
the specific runbook. Escalation: see `CONTACTS.md`, SLA in `SECURITY.md`.
