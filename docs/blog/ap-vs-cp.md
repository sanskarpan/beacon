# AP vs CP: the one-failed-request vs the rejected write

> Full post lives on the [Blog index](index.md).
> This page is a stub so `mkdocs.yml` deep-links stay stable.

The trade, stated plainly ([Consistency](../CONSISTENCY.md), [ADR 0001](../adr/0001-ap-vs-cp.md)):

| | Cost of being wrong |
|---|---|
| **AP** (gossip) | Stale endpoint → one failed request + retry |
| **CP** (Raft) | Unavailable registry → cannot register / discover at all on the minority side |

For service discovery, AP is usually right — which is why AP is the default
(`--consistency=ap|cp`). Try it live in the console Consistency Lab, and read
the design context on the [blog index](index.md).
