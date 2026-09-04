# ADR 0002 — Agent-local health checking

- Status: accepted. Date: 2026-09-04.
- Context: 10k instances × 1 check/5s = 2k checks/s centrally vs ~10/agent.
- Decision: checks run on the owning agent over loopback; control plane
  receives STATE, not probes. Semantics: "can this instance serve".
- Consequences: agent local state is authoritative; catalog is a replica
  (deleted agent-owned entries are put back; wipes repopulate in one interval).
