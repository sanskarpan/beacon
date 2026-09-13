# Module Alignment: `github.com/sanskar/beacon` → `github.com/sanskarpan/beacon`

> **Status: DONE in v0.2.0.**
> `go.mod` is now `module github.com/sanskarpan/beacon` (aligned with repo URL).
> This doc is kept as history + rollback reference.

## Why align

- The repo lives at **`github.com/sanskarpan/beacon`**: `mkdocs.yml`
  (`site_url`, `repo_url`, `repo_name`), `.goreleaser.yml` (docker
  `ghcr.io/sanskarpan/beacon`, brew tap `sanskarpan/homebrew-tap`,
  homepage `https://github.com/sanskarpan/beacon`).
- The Go module declares **`github.com/sanskar/beacon`** (`go.mod` line 1).
  Anyone running `go get github.com/sanskarpan/beacon@latest` gets a module-path
  mismatch; `pkg.go.dev/github.com/sanskarpan/beacon` cannot resolve to the
  declared module path, and vanity-import / proxy lookups split across two
  identities.
- External seams already point at the `sanskarpan` org
  (`github.com/sanskarpan/raft-consensus` in `go.mod`), so the org name is
  established — the root module is the outlier.

## Blast radius (measured 2026-09-07)

| Surface | Impact |
|---|---|
| Go imports | **~131 files** under `pkg/`, `cmd/`, `test/`, `api/` import `github.com/sanskar/beacon/...` (count: `grep -rl` over Go sources). Every one must be rewritten. |
| `go.mod` + `go.sum` | Module line changes; `go mod tidy` rewrites sums. The two `replace` directives (`github.com/example/grpc-service => ./external/grpc-service`, `gossip-system => ./external/gossip-system`) are relative and unaffected. |
| `pkg.go.dev` | Old path `pkg.go.dev/github.com/sanskar/beacon` freezes at the last v1 tag; new path starts fresh with no version history carried over. |
| GoReleaser | No module-path field, but release tags feed the proxy: tags cut after the rename resolve only under the new path. Brew formula (`brews.name: beacon`, tap `sanskarpan/homebrew-tap`) reinstalls from the new import path on next release. |
| Docker / GHCR | Image names (`ghcr.io/sanskarpan/beacon`) unchanged — no action. |
| Docs | `mkdocs.yml` URLs already use `sanskarpan` (no change). Grep docs + `README.md` + `SPEC.md` + `PROMPT.md` for `sanskar/beacon` import snippets and update them in the same release. |
| `buf.yaml` / `buf.gen.yaml` + `pkg/api/pb/` | Generated stubs carry the old import path in headers/options; must regenerate via `make proto` after the rename. (`pkg/api/pb/*.go` is git-ignored per `TODO-065` — regenerate, do not hand-edit.) |
| CI (`.github/workflows/`) | `forbid-paths` job and any cached module paths referencing `github.com/sanskar/beacon` must be updated; Go build cache invalidates once (one slow run). |
| Consumers | **Breaking change.** Any external importer must change their import. That is why this rides a major version (below). |

## Execution (v0.2.0, 2026-09-12)

Executed as breaking change in 0.x (allowed by SemVer: `0.y.z` minors may break).
Steps: `go mod edit -module`, import rewrite via `sed`, `go mod tidy`, proto `go_package` + `buf.gen.yaml` update, doc updates, `go build/vet/test`, `goreleaser check`.

## Rollback

- **If needed:** `go mod edit -module github.com/sanskar/beacon` + rewrite imports back, `go mod tidy`, tag `v0.2.1`.
- Consumers on `v0.1.0` stay on old path; `v0.2.0+` uses new path `github.com/sanskarpan/beacon`.
