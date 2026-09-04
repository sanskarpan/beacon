# Governance

- Decision making: lazy consensus on PRs; silent 72h = approval for docs/chore,
  explicit approval required for `pkg/mesh`, `pkg/store/raft`, `pkg/xds`.
- Releases: SemVer tags `vX.Y.Z` via `release.yml` (GoReleaser + SBOM + cosign).
  No direct pushes to `main` — PR + CI green + owner review.
- Security: see `SECURITY.md` (48h triage / 7d fix SLA); `codeql.yml` +
  `scorecard.yml` run weekly and on push.
- Records: ADRs in `docs/adr/`; production incidents get a runbook update.
