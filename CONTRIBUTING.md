# Contributing to beacon

## Dev Setup

```bash
go version # 1.26+ (see go.mod)
make tidy         # go mod tidy
make vet          # go vet ./...
make lint         # golangci-lint run ./...
make test         # go test ./...
make test-race    # go test -race ./pkg/...
make build        # bin/beacon{,-server,-agent}, bin/demo
cd console && bun install && bun run build
```

## Commit Style

One commit per file. Branch naming:

```
fix/pkg-catalog-store-go
fix/-golangci-yml
feat/xxx
docs/xxx
```

Each commit message:

```
<type>: <scope> — <what>

Closes #<issue>
```

Types: `fix`, `feat`, `test`, `docs`, `ci`, `chore`. All PRs target `feat/<feature>` or `integration`, which is then merged into `main`.

## Testing

```bash
go test ./... -count=1
go test -race ./pkg/catalog ./pkg/health ./pkg/agent ./pkg/gossip ./pkg/store/raft/consensus ./pkg/xds ./pkg/mesh
go run ./cmd/beacon sim all
go run ./cmd/beacon bench propagate
```

## Lint & Security

```bash
golangci-lint run ./...
govulncheck ./...
gosec ./...
```

## DCO

All commits must be signed off (`git commit -s`). By contributing you agree to the [Developer Certificate of Origin](https://developercertificate.org/).

## Releases

Releases are cut from `main` by tagging `vX.Y.Z` (SemVer, Keep-a-Changelog).
`release.yml` builds binaries + images, attaches SBOM, and cosign-signs.
Update `CHANGELOG.md` under `[Unreleased]` with every user-facing change.
