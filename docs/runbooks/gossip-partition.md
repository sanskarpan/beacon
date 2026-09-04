# Gossip partition

1. Confirm: `beacon members` shows two sides; console divergence counter > 0.
2. AP mode: both sides keep serving (expected). Do NOT force writes to "fix"
   divergence — heal first, incarnation/tombstones reconcile automatically.
3. CP mode: minority returns `ErrNoQuorum` on writes and consistent reads
   (expected). Serve stale reads or fail over writers to the majority side.
4. Heal the network, then verify: divergence → 0, `EvConverged` emitted,
   `beacon instances <svc>` identical on both sides.
5. If deletions resurrected: check `Digest.Tombstones` propagated
   (`MerkleSync` handles remote tombstones); force `FullSync` if needed.
