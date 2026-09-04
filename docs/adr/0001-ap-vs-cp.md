# ADR 0001 — AP vs CP, side by side

- Status: accepted. Date: 2026-09-04.
- Context: service discovery must choose availability vs linearizability.
- Decision: implement both `GossipStore` (AP) and `RaftStore` (CP) behind
  `CatalogStore`; `--consistency=ap|cp`; console Consistency Lab compares live.
- Consequences: AP may route stale (one failed request + retry); CP minority
  rejects writes (`ErrNoQuorum`) and linearizable reads need `ReadIndex`
  quorum. AP is the default for discovery.
