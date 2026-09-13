# Stability

SemVer from `v1.0.0`. No breaking changes within a major version.

- Public API: `api/openapi.yaml` (HTTP) + `proto/beacon.proto` (gRPC/xDS). Buf breaking check in `make proto-verify`.
- Anything under `internal/` or `external/` stubs carries no compatibility guarantee.
- Deprecation: one minor release of warnings before removal; noted in `CHANGELOG.md` under `[Unreleased]`.
- Module path is `github.com/sanskarpan/beacon` (aligned with repo URL since v0.2.0).
