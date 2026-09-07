# The zero-stale-window fallacy

> Full post lives on the [Blog index](index.md#post-2--gossipstreaming-vs-healthdns-the-22x-spread-and-how-bench-propagate-measures-it).
> This page is a stub so `mkdocs.yml` deep-links stay stable.

There is no zero-stale window — only windows you choose and windows you inherit.
Gossip+streaming buys ~2 s; health+DNS inherits ~45 s (see `bench propagate`).
Read the full measurement post on the [blog index](index.md), and the
stage-by-stage breakdown in [Propagation](../PROPAGATION.md).
