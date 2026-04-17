# TL-Bus Documentation

TL-Bus is a Rust message bus for microservices. It keeps service routing, message lineage, and pool federation explicit so the system stays debuggable as it grows.

This docs tree mirrors the FastAPI pattern: one locale tree, one landing page, and a small set of focused sections underneath.

## Start here

- [About](about/index.md)
- [Tutorial](tutorial/index.md)
- [Reference](reference/index.md)
- [Resources](resources/index.md)

## What TL-Bus focuses on

- explicit envelopes and headers
- `txn_id` propagation end to end
- service manifests with capabilities and modes
- local Unix-socket delivery
- cross-pool delivery through bridge and sidecar

## Where to look in the repo

- [README](../../../README.md)
- [Italian README](../../../READMEIT.md)
- [MIT License](../../../LICENSE)

