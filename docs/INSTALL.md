# Installation

Three ways to get a running `beacon-server` (plus `beacon` CLI and `beacon-agent`).
Pick one, then verify with the health check at the bottom.

## Option 1 — Homebrew (macOS / Linux)

The formula is published to `sanskarpan/beacon` (`Formula/beacon.rb` in this
repo) by GoReleaser on every `vX.Y.Z` tag. Homebrew 4.4+ requires an explicit
trust for non-core taps:

```bash
brew trust --tap sanskarpan/beacon
brew tap sanskarpan/beacon https://github.com/sanskarpan/beacon
brew install sanskarpan/beacon/beacon
beacon-server --http :8500 --dns :8600 --consistency ap --node server-1
```

Direct URL (no tap) also works:

```bash
brew install https://raw.githubusercontent.com/sanskarpan/beacon/main/Formula/beacon.rb
```

Installs `beacon`, `beacon-server`, and `beacon-agent`.

## Option 2 — `go install` (Go 1.27+)

Canonical command (works once `go.mod` is realigned to `v2`):

```bash
go install github.com/sanskarpan/beacon/cmd/beacon-server@latest
```

> **Module-path caveat:** `go.mod` still declares `github.com/sanskar/beacon`
> while the repo lives at `github.com/sanskarpan/beacon`, so the one-liner
> above fails with a module-path mismatch until the v2 realignment lands.
> **Working command today** — clone and install from source:
>
> ```bash
> git clone https://github.com/sanskarpan/beacon.git
> cd beacon
> go install ./cmd/beacon-server ./cmd/beacon ./cmd/beacon-agent
> # binaries land in $(go env GOPATH)/bin
> ```

## Option 3 — Docker (GHCR, no Go toolchain needed)

Published by GoReleaser on every `vX.Y.Z` tag as
`ghcr.io/sanskarpan/beacon:<version>` and `:latest`
(SBOM + cosign signature attached — see `release.yml`).

```bash
docker run --rm -p 8500:8500 -p 8600:8600/udp ghcr.io/sanskarpan/beacon:latest
```

Pin a release instead of `latest`:

```bash
docker run --rm -p 8500:8500 -p 8600:8600/udp ghcr.io/sanskarpan/beacon:v0.1.0
```

Full 3-node cluster (servers + agent + console): see
[Deployment](DEPLOYMENT.md) — `docker compose up --build`.

## Option 4 — Build from source

```bash
git clone https://github.com/sanskarpan/beacon.git
cd beacon
make build   # → ./bin/beacon-server ./bin/beacon ./bin/beacon-agent
./bin/beacon-server --http :8500 --dns :8600 --consistency ap --node server-1
```

Requires Go 1.27+. The web console needs Bun (`cd console && bun install && bun run dev`).

## Verify

```bash
curl -s localhost:8500/health
# DNS (UDP+TCP on 8600, TTL=0):
dig @127.0.0.1 -p 8600 payments.service.beacon +short
```

| Port | Protocol | Surface |
|---|---|---|
| `8500` | HTTP | `/v1/*`, `/health`, `/ready`, `/metrics`, `/v1/events` (SSE) |
| `8600` | DNS UDP+TCP | `A/AAAA/SRV`, TTL=0 |
| `8502` | gRPC | `Discovery` Watch/WatchMulti, xDS ADS |

Next: [Deployment](DEPLOYMENT.md) (Compose/K8s/bare metal),
[Configuration](CONFIGURATION.md) (flags/env), [Demo](DEMO.md) (60-second tour).
