# API Reference

## HTTP (`:8500`)

All responses JSON. Mutations return `X-Beacon-Index`.

| Method | Path | Query | Description |
|---|---|---|---|
| `PUT` | `/v1/agent/service/register` | — | Body `Instance` JSON; returns `{id,index}` |
| `PUT` | `/v1/agent/service/deregister/:id` | — | Returns `X-Beacon-Index` |
| `PUT` | `/v1/agent/check/pass/:id` `warn` `fail` `maintenance` | `?enable=true` | TTL/maintenance |
| `GET` | `/v1/catalog/services` | — | `map[service][]tags` |
| `GET` | `/v1/catalog/service/:name` | `index,wait,passing,tag,filter,consistent,stale` | Blocking query |
| `GET` | `/v1/health/service/:name` | as above | Health-filtered |
| `GET` | `/v1/agent/members` | — | SWIM members |
| `GET` | `/v1/watch/stats` | — | Registry stats |
| `GET` | `/v1/events` | `trace_id` | SSE `data: {Event}` |
| `GET` | `/v1/xds/status` | `node` | xDS stream states |
| `GET` | `/metrics` | — | Prometheus |
| `GET` | `/health`, `/ready` | — | Liveness |

**Blocking query:** `?index=N&wait=5m` — `200` with `X-Beacon-Index`; timeout returns current state (not error); `429` with `Retry-After: 1` on rate limit.

**Example:**

```bash
curl -X PUT http://localhost:8500/v1/agent/service/register -d '{"service":"payments","port":8080}'
curl "http://localhost:8500/v1/health/service/payments?passing=true&index=10&wait=5m" -H "X-Beacon-Index: 11"
```

## gRPC (`:8502`)

```protobuf
service Discovery {
  rpc Watch(WatchRequest) returns (stream WatchEvent);
  rpc WatchMulti(stream WatchMultiRequest) returns (stream WatchEvent);
  rpc Register(RegisterRequest) returns (RegisterResponse);
  rpc Deregister(DeregisterRequest) returns (DeregisterResponse);
}
```

`WatchEvent { kind: SNAPSHOT|ADD|UPDATE|REMOVE, service, instances, index }`

## DNS (`:8600` UDP+TCP)

```
payments.service.beacon          A/AAAA/SRV (TTL=0, shuffle, TC on UDP 512)
v2.payments.service.beacon       A (tag filter)
payments.service.dc1.beacon      A (datacenter)
node-1.node.beacon               A (node lookup)
```

`dig @localhost -p 8600 payments.service.beacon SRV`

## Production codegen path

`pkg/api/pb/pb.go` is wire-compatible with `proto/beacon.proto` and served
live by `grpcapi.ProtoServer` (`beacon-server --grpc :8502`). To move to
generated stubs:

```bash
buf generate --template proto/buf.gen.yaml
git diff --exit-code pkg/api/pb/
go test ./pkg/api/grpcapi/ -run TestProtoWire -count=1
```

Contract changes require a `buf breaking` check in CI and an entry in
`docs/API-CHANGELOG.md` (create on first breaking change). HTTP contract is
pinned in `api/openapi.yaml`.
