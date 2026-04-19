# Tutorial

This short path shows the project from the outside in.

## 1. Run the tests

```bash
cargo test --workspace --all-targets
```

## 2. Inspect the client

```bash
cargo run -p tlbus-client -- --help
```

## 3. Send one envelope

```bash
cargo run -p tlbus-client -- send \
  --bus-socket /run/tlb.sock \
  --service-name ps1.client \
  --service-secret shared-secret \
  --to ps1.echo \
  --payload "{\"message\":\"hello\"}"
```

## 4. Start from manifests

Service discovery is manifest-driven. A service describes its capabilities and modes, and the protocol plugin can answer `*.manifest` requests from that registry data.

To enumerate services registered in a pool, use the reserved target `<pool>.__tlbus__.services` (it returns a `PoolManifest`).
From there you can query each service descriptor with `<pool>.<service>.manifest`.

## 5. Explore federation

Use the sidecar and bridge when a message must leave the local pool.
That keeps local delivery and cross-pool delivery separate, which is easier to reason about than a single mixed path.

## 6. Build base containers

Use one of these local builds:

```bash
docker build -f Dockerfile-client -t tlbus-client:local .
docker build -f Dockerfile-py -t tlbus-pyclient:local .
docker build -f Dockerfile-worker -t tlbus-worker:local .
```
