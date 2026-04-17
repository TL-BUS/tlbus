# TL-Bus Repository AI Guide

This file is designed to be machine-readable and action-oriented for AI agents that must create or extend projects in this repository.

## AI_SPEC

```yaml
spec_version: "1.0"
repository:
  name: "tlbus"
  primary_language: "rust"
  secondary_languages: ["python", "bash", "yaml", "html"]
  build_systems: ["cargo", "docker compose"]
documentation_policy:
  readme_default_language: "en"
  localized_readme_pattern: "READMEIT.md"
critical_rules:
  - "Do not use monkey patches."
  - "Prefer robust solutions over quick hacks."
  - "Run automatic tests for affected modules."
  - "Do not invent unknown libraries."
  - "Never expose secrets."
protocol_requirements:
  txn_id:
    required: true
    behavior: "propagate unchanged across request chain and replies"
  headers:
    required_minimum: ["txn_id", "protocol"]
    recommended: ["transport", "protocol_version", "content_type", "reply_to"]
  addressing_pattern: "<pool>.<service>.<action>"
```

## REPO_MAP

```yaml
paths:
  core:
    - "crates/core"
    - "crates/daemon"
    - "crates/bridge"
    - "crates/sidecar"
    - "crates/send"
  plugins:
    - "crates/plugins/auth"
    - "crates/plugins/lineage"
    - "crates/plugins/hmac"
    - "crates/plugins/manifest"
  demo_rust:
    - "examples/compose-demo"
    - "examples/openclaw-tlbusnet-demo"
  demo_python:
    - "examples/python-client"
    - "examples/billing-demo/python"
  analysis:
    - "analisi"
```

## AGENT_WORKFLOW

```yaml
workflow:
  - step: "Read relevant README.md and domain data under analisi/ when present."
  - step: "Identify service boundaries: gateway, workflow/orchestrator, domain services."
  - step: "Keep message contracts explicit (payload schema + headers)."
  - step: "Ensure all outbound messages preserve inbound txn_id."
  - step: "Register services with manifest capabilities and modes."
  - step: "Wire demo stack via docker compose (sidecars + services + clients)."
  - step: "Run tests and config validation commands."
  - step: "Update README.md in English and add READMEIT.md only when keeping Italian docs."
```

## SERVICE_TEMPLATE

Use this pattern when adding a new TL-Bus service in Rust demo code:

```yaml
service_template:
  register:
    env_required: ["TLB_SERVICE_NAME", "TLB_SERVICE_SECRET"]
    helper: "bootstrap_service(...) in examples/compose-demo/src/lib.rs"
  listener:
    function: "bind_service_listener(service_socket)"
    loop: "accept -> read_envelope -> parse target -> decode payload -> handle -> reply"
  reply_rules:
    must_copy_headers:
      - "txn_id"
      - "workflow_id (if present)"
      - "command/operation index (if present)"
    must_use_reply_to_header: true
  logging:
    use_trace_fields: true
    include_txn_id: true
```

## TXN_ID_CONTRACT

```yaml
txn_id_contract:
  ingress:
    - "If missing at boundary, generate once (lineage/plugin or boundary service)."
  internal_calls:
    - "Always copy txn_id from inbound envelope to outbound headers."
  async_workflows:
    - "Store txn_id inside workflow state."
    - "Use same txn_id for final batch/aggregate reply."
  failure_paths:
    - "Fallback responses must keep txn_id."
  forbidden:
    - "Do not regenerate txn_id mid-workflow."
    - "Do not drop txn_id in timeout/error branches."
```

## EXAMPLE_CREATION_PLAYBOOK

```yaml
create_new_demo:
  inputs:
    - "domain scenario"
    - "data source (csv/json/db)"
    - "pool topology (local/federated)"
  output_structure:
    rust_code:
      - "examples/compose-demo/src/<new_module>.rs"
      - "examples/compose-demo/src/main.rs (new commands)"
      - "examples/compose-demo/src/lib.rs (exports)"
    infra:
      - "examples/<new-demo>/docker-compose.yml"
      - "examples/<new-demo>/test.sh"
      - "examples/<new-demo>/README.md"
      - "examples/<new-demo>/READMEIT.md (optional)"
  required_capabilities:
    - "one gateway/orchestrator capability"
    - "one or more domain capabilities"
    - "one client receive_result capability"
  validation:
    - "cargo fmt --all"
    - "cargo test -p tlbus-compose-demo"
    - "docker compose -f <demo>/docker-compose.yml config"
```

## COMMAND_REFERENCE

```yaml
commands:
  rust_tests:
    - "cargo test -p tlbus-compose-demo"
    - "cargo test -p tlbus-daemon"
  formatting:
    - "cargo fmt --all"
  compose_validation:
    - "docker compose -f examples/openclaw-tlbusnet-demo/docker-compose.yml --env-file examples/openclaw-tlbusnet-demo/.env.example config"
  demo_run:
    - "bash examples/openclaw-tlbusnet-demo/test.sh all"
```

## DOC_POLICY

```yaml
documentation:
  rules:
    - "README.md must be English."
    - "If preserving an Italian version, store it as READMEIT.md."
    - "Keep operational commands executable as written."
    - "Include expected output examples where useful."
```

## ACCEPTANCE_CHECKLIST

```yaml
acceptance_checklist:
  - "All touched README.md files are in English."
  - "Italian versions (if any) are available as READMEIT.md."
  - "txn_id propagation verified in request, reply, and fallback paths."
  - "Tests for touched modules pass."
  - "Compose config validates for touched demos."
  - "No secrets added to code, docs, or compose files."
```

## MINIMAL_AGENT_PROMPT

Use this snippet when delegating work to another AI:

```text
Read READMEAI.md first. Build a TL-Bus project following AI_SPEC, TXN_ID_CONTRACT, and EXAMPLE_CREATION_PLAYBOOK. Keep README.md in English, keep optional Italian docs in READMEIT.md, run required tests, and report changed files plus validation results.
```
