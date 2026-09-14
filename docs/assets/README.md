# Visual assets

| File | What it shows |
|---|---|
| `demo.gif` | Real browser capture of the running console: live registration events, deregistration, the gossip contrast chart, populated mesh topology, and health tables (960×600, 124 frames, captured from the Compose stack and downscaled with `ffmpeg`) |
| `topology.svg` | Mesh topology: 3 beacon-servers + agent + console + client; AP gossip (dashed) vs CP Raft (solid) edges; health-colored nodes |
| `timeline.svg` | Propagation timeline t0 crash → t4 client with fast (~2s) vs slow (~45s) path comparison bars (`beacon bench propagate` p50s) |
| `consistency-lab.svg` | Consistency lab: AP stale-read vs CP rejected-write under partition |

See `docs/DEMO.md` for the record script (re-recording `demo.gif` with `ffmpeg`/`asciinema`).
