# About TL-Bus

TL-Bus is designed around a simple idea: keep the bus visible.

Instead of hiding routing and message metadata inside a big framework, the project exposes the important pieces directly:

- envelopes carry the routing and trace metadata
- plugins add lineage, auth, HMAC, and protocol behavior
- manifests describe what a service can do
- the bridge keeps federated delivery separate from local delivery

## Design principles

| Principle | Meaning |
| --- | --- |
| Explicit routing | Service names resolve to concrete socket paths or pool targets |
| Traceability | `txn_id` stays visible from ingress to reply |
| Small components | Daemon, bridge, sidecar, and sender each have a narrow job |
| Testability | The workspace ships with unit tests and demo scripts |

## Project focus

The project is centered on the message path, not on framework ceremony.
That makes TL-Bus a good fit when you want to:

- keep control over routing decisions
- observe message headers and lineage
- compose local and federated delivery
- plug in policy without rewriting the core bus

