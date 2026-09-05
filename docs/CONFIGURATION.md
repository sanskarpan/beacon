# Configuration

All flags have env equivalents (`BEACON_*`). Flags override env.

## beacon-server

| Flag | Env | Default | Description |
|---|---|---|---|
| `--http` | `BEACON_HTTP` | `:8500` | HTTP API listen |
| `--dns` | `BEACON_DNS` | `:8600` | DNS listen (UDP+TCP) |
| `--grpc` | `BEACON_GRPC` | `:8502` | gRPC listen |
| `--node` | `BEACON_NODE` | `server-1` | Node ID |
| `--consistency` | `BEACON_CONSISTENCY` | `ap` | `ap` or `cp` (Raft) |
| `--join` | `BEACON_JOIN` | — | Seed `host:port` (SWIM) |
| `--gossip` | `BEACON_GOSSIP` | `:7946` | UDP membership listen address |
| `--advertise` | `BEACON_ADVERTISE_ADDR` | bind host | Address advertised to peers |
| `--auth-token` | `BEACON_AUTH_TOKEN` | — | Bearer token for HTTP and gRPC control planes |
| `--tls-cert` | `BEACON_TLS_CERT` | — | Server TLS certificate |
| `--tls-key` | `BEACON_TLS_KEY` | — | Server TLS private key |
| `--tls-client-ca` | `BEACON_TLS_CLIENT_CA` | — | Require clients chaining to this CA |
| `--enable-lab` | `BEACON_ENABLE_LAB` | `false` | Enable synthetic consistency lab endpoints |
| `--data-dir` | `BEACON_DATA_DIR` | `./data` | Persist (agent) |
| `--otel-endpoint` | `BEACON_OTEL_ENDPOINT` | — | OTLP gRPC endpoint |
| `--version` | — | `dev` | Print version and exit |

Example:

```bash
./bin/beacon-server --http :8500 --dns :8600 --grpc :8502 --node s1 --consistency ap --join seed:7946
BEACON_HTTP=:8500 BEACON_CONSISTENCY=cp ./bin/beacon-server
```

## beacon-agent

| Flag | Env | Default |
|---|---|---|
| `-server` | `BEACON_SERVER_URL` | `http://localhost:8500` |
| `-node` | `BEACON_NODE` | hostname |
| `-data-dir` | `BEACON_DATA_DIR` | `./data` |
| `-auth-token` | `BEACON_AUTH_TOKEN` | — |

## Console

The Kubernetes deployment expects operator-managed Secrets named
`beacon-server-auth` (key `token`) and `beacon-server-tls` (keys `tls.crt` and
`tls.key`). The local Docker Compose deployment intentionally leaves auth and
TLS unset for development; provide the corresponding environment variables for
non-local use.

`BEACON_API_URL` (build-time `VITE_BEACON_API_URL` or runtime nginx `BEACON_API_URL`) — proxies `/v1`, `/health`, `/metrics` to control plane. Vite dev proxies to `127.0.0.1:8500`; Docker nginx uses `envsubst` template.
