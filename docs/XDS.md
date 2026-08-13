# xDS Control Plane

beacon acts as an Envoy management server (ADS).

## Make before break

| Operation | Order |
|---|---|
| ADD | CDS → EDS → LDS → RDS |
| REMOVE | LDS → RDS → EDS → CDS |

Pushing LDS that references a cluster CDS hasn’t defined → listener reject or 503 spike during deploys.

## ACK / NACK

- ACK: `version_info` matches what we sent, `error_detail` empty  
- NACK: detected by **presence of `error_detail`**, not version comparison  
- **A NACK must NOT resend the same config** — surface the error and wait. NACK loops are a real outage mode.

## SotW vs Delta

One pod restart in a 5,000-endpoint cluster:

| Protocol | Bytes to 1,000 proxies |
|---|---|
| SotW | 5,000 × 1,000 = 5,000,000 records |
| Delta | 1 × 1,000 = 1,000 records |

\>1000× reduction. The console charts this.
