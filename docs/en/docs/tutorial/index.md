# Tutorial

This short path shows the project from the outside in.

## 1. Run the tests

```bash
cargo test --workspace --all-targets
```

## 2. Inspect the sender

```bash
cargo run -p tlbus-send -- --help
```

## 3. Send one envelope

```bash
cargo run -p tlbus-send -- \
  --socket /run/tlb.sock \
  --from ps1.client \
  --to ps1.echo \
  --payload "hello"
```

## 4. Start from manifests

Service discovery is manifest-driven. A service describes its capabilities and modes, and the protocol plugin can answer `*.manifest` requests from that registry data.

## 5. Explore federation

Use the sidecar and bridge when a message must leave the local pool.
That keeps local delivery and cross-pool delivery separate, which is easier to reason about than a single mixed path.

## Demo path

The quickest end-to-end demo lives in the sibling `examples/compose-demo` folder of the project workspace.
