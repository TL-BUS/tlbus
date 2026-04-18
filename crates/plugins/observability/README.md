# TL-Bus Observability Plugin

Prometheus observability plugin for TL-Bus.

## Features

- Prometheus `/metrics` endpoint
- Message counter and processing latency histogram

## Usage

Enable the plugin in `TLBUS_PLUGINS` and optionally override the bind address.

```sh
export TLBUS_PLUGINS="lineage,auth,protocol,observability"
export TLB_METRICS_ADDR="0.0.0.0:9090"
```

Then scrape:

```sh
curl -sS http://127.0.0.1:9090/metrics
```
