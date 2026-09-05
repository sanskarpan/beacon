# Deployment

## Docker Compose (3-node AP + agent + console)

```bash
docker compose up --build
# or CP mode
BEACON_CONSISTENCY=cp docker compose up --build
# console at http://localhost:3000, API at :8500, DNS at :8600
```

Compose uses `Dockerfile.server`/`agent`/`console` (multi-stage, `tini`, `USER beacon`, `HEALTHCHECK`, `VERSION` ldflag, `.dockerignore`).

## Kubernetes (example)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: beacon-server}
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: server
        image: beacon:server
        args: ["-http=:8500","-consistency=ap","-join=beacon-server-0.beacon:7946"]
        ports: [{containerPort: 8500}, {containerPort: 8600}, {containerPort: 8502}]
        livenessProbe: {httpGet: {path: /health, port: 8500}, periodSeconds: 10}
        readinessProbe: {httpGet: {path: /ready, port: 8500}, periodSeconds: 5}
```

## Bare Metal

```bash
make build
./bin/beacon-server --http :8500 --node s1 --consistency ap --join seed:7946 &
./bin/beacon-agent --server http://localhost:8500 --node n1 --data-dir ./data
```

## Persistence

Agent `services.json` in `BEACON_DATA_DIR` (default `./data`, volume `agent-data` in compose). AP server catalogs are in-memory and replicated by UDP gossip. CP mode uses the networked `raft-consensus` transport with one WAL/stable/snapshot directory per node. Compose supplies the static peer set automatically; bare-metal and Kubernetes deployments must provide `BEACON_RAFT_PEERS` explicitly.

## Upgrades

Rolling restart: agents repopulate catalog via anti-entropy within one `SyncInterval` (scales with cluster size, 1m-30m). No downtime.
