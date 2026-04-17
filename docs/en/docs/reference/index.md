# Reference

## Binaries

| Binary | Purpose |
| --- | --- |
| `tlbus-send` | Send one envelope to a daemon socket |
| `tlbus-daemon` | Validate, route, and deliver envelopes |
| `tlbus-bridge` | Forward federated traffic between pools |
| `tlbus-sidecar` | Run daemon and bridge together for one pool |

## Core headers

| Header | Meaning |
| --- | --- |
| `txn_id` | Transaction correlation across the whole message path |
| `reply_to` | Return address for replies and protocol responses |
| `protocol` | Logical protocol name carried by the envelope |
| `transport` | Transport used by the selected delivery mode |
| `content_type` | Payload media type when the protocol needs it |

## Useful environment variables

| Variable | Role |
| --- | --- |
| `TLBUS_PLUGINS` | Selects the active plugin pipeline |
| `TLB_SERVICE_SECRET` | Enables service registration handshake |
| `TLB_POOL_SECRET` | Enables federation handshake between pools |
| `TLB_HMAC_KEY` | Enables HMAC signing |
| `TLB_BUS_MODE` | Selects the sidecar mode |
| `TLB_BUS_POOL` | Names the local pool |

## Message model

- `Envelope` carries the payload and metadata
- `ServiceManifest` describes service capabilities
- `ServiceDescriptor` is the registered view of a service
- `PoolManifest` aggregates the services available in one pool

