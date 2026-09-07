# Visual assets

| File | What it shows |
|---|---|
| `demo.gif` | Animated 6-frame loop: mesh healthy → instance dies → SWIM detects (~2s) → gossip propagates → clients stop routing → healed (960×540, generated with `python3 + PIL`, see script in `docs/DEMO.md` history) |
| `topology.svg` | Mesh topology: 3 beacon-servers + agent + console + client; AP gossip (dashed) vs CP Raft (solid) edges; health-colored nodes |
| `timeline.svg` | Propagation timeline t0 crash → t4 client with fast (~2s) vs slow (~45s) path comparison bars (`beacon bench propagate` p50s) |
| `consistency-lab.svg` | Consistency lab: AP stale-read vs CP rejected-write under partition |

See `docs/DEMO.md` for the record script (re-recording `demo.gif` with `ffmpeg`/`asciinema`).
