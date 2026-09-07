# Module Alignment Plan: `github.com/sanskar/beacon` → `github.com/sanskarpan/beacon`

> **Status: PLAN ONLY. DO NOT rename the module now.**
> `go.mod` stays `module github.com/sanskar/beacon` until the major-version
> cut described below. This doc exists so the rename is a checklist execution,
> not a discovery exercise.

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

## Plan (execute on the next MAJOR, e.g. v2.0.0)

1. **Pre-freeze.** File the tracking issue; announce the rename + date in `CHANGELOG.md` `[Unreleased]` and `README.md`. Freeze new `sanskar/beacon`-path references (CI grep gate).
2. **Branch** `chore/module-rename` from `main`, green CI required before starting.
3. **Rewrite imports.** `go mod edit -module github.com/sanskarpan/beacon`, then rewrite all Go imports (`gofmt -w` safe: only the module prefix), `go mod tidy`, `make proto` to regenerate `pkg/api/pb/`.
4. **Docs + metadata.** Update import snippets in `README.md`, `SPEC.md`, `PROMPT.md`, `docs/**`, `deploy/`, examples. `mkdocs.yml` / `.goreleaser.yml` org URLs stay as-is (already correct).
5. **Verify.** `go build ./...`, `go vet ./...`, `golangci-lint run ./...`, `go test ./... -count=1`, `go test -race` on the catalog/health/gossip/xds set per `CONTRIBUTING.md`, `cd console && bun run build`, `goreleaser check`.
6. **Release as MAJOR.** Tag `v2.0.0` (SemVer major: import-path break). Cut release notes with old→new mapping + `go get` line. Confirm `pkg.go.dev/github.com/sanskarpan/beacon@v2.0.0` resolves.
7. **Post-release.** Archive note on the old proxy path (retract directive if spam/abuse appears: `retract` in a patch release). Update `ADOPTERS.md` / downstream pins.

## Rollback

- **Before tag push:** revert the branch, delete the tag locally if created (`git tag -d v2.0.0`). Nothing external has resolved the new path yet.
- **After tag push / proxy cached:** do NOT force-push or delete the tag (the Go proxy is immutable). Instead cut `v2.0.1` reverting the module line and imports, and document both paths in `CHANGELOG.md`. Accept that both paths now permanently exist in the proxy.
- **Consumer rollback:** `go mod edit -module` back + `go get github.com/sanskar/beacon@<last-v1>`; the v1 line stays untouched and installable throughout.

## Explicit non-goals (until v2)

- No `go.mod` edit now. No import rewrites now. No dual-module (`v2/` suffix) layout now.
- No `replace github.com/sanskarpan/beacon => .` shim — it masks the break instead of versioning it.
