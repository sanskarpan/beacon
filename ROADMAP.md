# Roadmap

- [ ] Real SWIM transport behind `Membership` (UDP piggyback, MTU split) — replaces in-process fabric in prod.
- [ ] `protoc`/`buf` codegen for `proto/beacon.proto` (`buf breaking` gate).
- [ ] Multi-process CP chaos in CI (`docker-compose` CP partition test — TODO-014 remainder).
- [ ] Console live dual-cluster Consistency Lab wiring (backend `pkg/lab` exists).
- [ ] K8s operator / Helm chart (raw manifests in `deploy/k8s` today).
- [ ] Fuzz + kind e2e + coverage gates in CI.
