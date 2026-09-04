# Security Policy

## Supported Versions

| Version | Supported |
|---|---|
| `main` | ✅ |
| `0.1.x` | ✅ (pre-release) |
| `< 0.1` | ❌ |

## Threat Model

`beacon` is a control-plane for service discovery. Trust boundaries:

- **mTLS (SPIFFE):** Every workload gets a short-lived leaf cert (`pkg/mesh`) via SDS, rotated at 50% lifetime. IPs are *not* identities.
- **Intentions:** L4 `Source → Destination` `Allow/Deny` with precedence; default deny.
- **Entitlements:** production CAs come from `mesh.NewCAProduction` (fail-closed:
  `Sign` denies any workload→SPIFFE pair not granted via `CA.Entitle`).
  `mesh.NewCA` keeps a permissive dev default for backward compat — never use
  it in production.
- **Rate limiting:** Per-node registration (`pkg/catalog`) and per-IP HTTP (`pkg/api/httpapi`) with GC; not an authz boundary.

## Reporting a Vulnerability

Email `sanskarpandey2004@gmail.com` with:

- Affected version/commit and `go.mod` toolchain
- Repro steps or PoC (prefer `go test -run` or `curl` against `beacon-server`)
- Impact assessment (discovery poisoning, mTLS bypass, gossip amplification, etc.)

We will acknowledge within 48 hours, provide a fix timeline within 7 days, and coordinate disclosure. Do **not** open a public issue for unpatched vulnerabilities.

## Hardening Checklist (operator)

- Build production CAs with `mesh.NewCAProduction` and explicit `Entitle` per workload.
- CI enforces `gosec` + `govulncheck` + CodeQL + OpenSSF Scorecard (weekly and on push).
- Serve `ServerTLSConfig` with `ClientAuth: RequireAndVerifyClientCert`; do not set `InsecureSkipVerify` on clients without `VerifyPeerCertificate`.
- Rotate CA via `NewIntermediateCA`; distribute `Bundle()` via SDS, not static files.
- Enable `golangci-lint` `gosec` and `govulncheck` in CI before release.
