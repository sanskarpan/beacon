# xDS NACK loop

A NACK means the proxy rejected config and still runs the previous version.
Do NOT resend the same config (the server already suppresses resend by hash).

1. Read the NACK: console xDS view or `EvXDSNack` (`type`, `version`, `error`).
2. Fix the config source (usually a bad cluster/listener reference or RBAC
   mapping), then push a NEW version — ACK clears `st.Nacked[type]`.
3. Verify ordering: adds go CDS→EDS→LDS→RDS; removals go LDS→RDS→EDS→CDS
   (`OrderedTypes`). Out-of-order pushes 503 during deploys.
4. Alert if `NACKRatio > 0.1%` over 1h (see `docs/SLO.md`).
