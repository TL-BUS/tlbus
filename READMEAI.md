# TL-Bus AI Guide

Machine-oriented execution guide for agents working in this repository.

## 1. Hard constraints

1. This repository is **core-only** (`tlbus` branch scope).
2. Do not reference external application stacks from here.
3. No monkey patching.
4. Prefer robust implementations over temporary hacks.
5. Run automatic checks for touched code.
6. Do not invent unknown libraries.
7. Never expose secrets.

## 2. Repository contract

```yaml
spec_version: "2.0"
repo_name: "tlbus"
scope: "core-only"
languages:
  primary: "rust"
  secondary: ["python", "bash", "yaml", "markdown"]
workspace_paths:
  core:
    - "crates/core"
    - "crates/daemon"
    - "crates/bridge"
    - "crates/sidecar"
    - "crates/send"
  clients:
    - "clients/client"
    - "clients/pyclient"
    - "clients/worker"
  plugins:
    - "crates/plugins/auth"
    - "crates/plugins/lineage"
    - "crates/plugins/hmac"
    - "crates/plugins/manifest"
  docs:
    - "docs/en"
    - "docs/it"
```

## 3. Messaging contract

```yaml
addressing_pattern: "<pool>.<service>.<action>"
headers:
  required:
    - "txn_id"
    - "protocol"
  recommended:
    - "transport"
    - "protocol_version"
    - "content_type"
    - "reply_to"
txn_id:
  ingress: "generate once if missing"
  internal_calls: "propagate unchanged"
  replies: "reuse inbound txn_id"
```

## 4. Logging contract

Clients and workers must emit explicit operational events.

```yaml
events:
  required:
    - "register"
    - "send"
    - "recv"
  optional:
    - "reply"
    - "drop"
fields:
  required:
    - "service"
    - "txn_id"
  context:
    - "from"
    - "to"
    - "reply_to"
python_logger:
  env_level: "TLB_LOG_LEVEL"
  format: "%(asctime)s %(levelname)s [%(name)s] %(message)s"
```

## 4.1 Worker composition contract

```yaml
tlbus_worker_binary:
  responsibility:
    - "argument parsing"
    - "payload handler logic"
  must_not_reimplement:
    - "worker registration loop"
    - "receive/reply runtime plumbing"
tlbus_client_library:
  runtime_api:
    - "WorkerLoopConfig"
    - "WorkerReply"
    - "run_worker_loop(...)"
```

## 4.2 Observability contract

```yaml
observability_plugin:
  name: "observability"
  enable_via: "TLBUS_PLUGINS includes observability"
  env:
    TLB_METRICS_ADDR:
      default: "127.0.0.1:9090"
  endpoint: "/metrics"
  metrics:
    - "tlbus_messages_total"
    - "tlbus_message_latency_seconds"
```

## 5. Container contract

```yaml
canonical_file: "Dockerfile"
local_files:
  - "Dockerfile-client"
  - "Dockerfile-py"
  - "Dockerfile-worker"
  - "Dockerfile-tlbusnet-obs"
targets:
  tlbusd:
    image: "ghcr.io/<owner>/tlbusd"
    target: "tlbusd-runtime"
  tlbusnet:
    image: "ghcr.io/<owner>/tlbusnet"
    target: "tlbusnet-runtime"
  tlbusnet_obs:
    image: "ghcr.io/<owner>/tlbusnet-obs"
    target: "tlbusnet-runtime-obs"
  client:
    image: "ghcr.io/<owner>/tlbus-client"
    target: "client-runtime"
  pyclient:
    image: "ghcr.io/<owner>/tlbus-pyclient"
    target: "pyclient-runtime"
  worker:
    image: "ghcr.io/<owner>/tlbus-worker"
    target: "worker-runtime"
```

## 6. CI contract

```yaml
workflows:
  tests: ".github/workflows/tests.yml"
  publish: ".github/workflows/ghcr-images.yml"
gitlab:
  pipeline: ".gitlab-ci.yml"
  images:
    - ".gitlab/images/tlbusnet-runtime-obs.yml"
requirements:
  - "publish job must depend on tests success"
  - "images publish for linux/amd64 and linux/arm64"
  - "release tags follow 20* pattern (2026.0.1)"
  - "latest is published from default branch"
  - "use Node 24-compatible actions/runtime"
```

## 7. Required validation before final output

Run all applicable commands:

```bash
cargo fmt --all --check
cargo test --workspace --all-targets
python3 -m py_compile clients/pyclient/tlbus_pyclient.py
```

Then verify scope cleanliness against external stack references.

## 8. Change checklist

1. Keep EN and IT README aligned.
2. Keep READMEAI aligned with actual workflow/tooling.
3. Preserve txn_id and reply traceability in code and logs.
4. Ensure new Docker/CI/documentation names match exactly.
5. Report changed files and validation results.

## 9. Minimal delegation prompt

```text
Read READMEAI.md first. Work only in TL-Bus core scope, keep txn_id propagation and structured logs intact, update EN/IT docs when behavior changes, run required checks, and report changed files with validation results.
```
