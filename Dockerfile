FROM rust:1.92-bookworm AS builder
WORKDIR /app

COPY Cargo.toml ./
COPY crates ./crates
COPY clients ./clients

RUN cargo generate-lockfile
RUN cargo build --release \
    -p tlbus-daemon \
    -p tlbus-bridge \
    -p tlbus-sidecar \
    -p tlbus-send \
    -p tlbus-client \
    -p tlbus-worker

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

FROM runtime-base AS tlbusd-runtime
COPY --from=builder /app/target/release/tlbus-daemon /usr/local/bin/tlbus-daemon
CMD ["tlbus-daemon"]

FROM runtime-base AS tlbusnet-runtime
COPY --from=builder /app/target/release/tlbus-bridge /usr/local/bin/tlbus-bridge
COPY --from=builder /app/target/release/tlbus-sidecar /usr/local/bin/tlbus-sidecar
CMD ["tlbus-sidecar"]

FROM runtime-base AS client-runtime
COPY --from=builder /app/target/release/tlbus-client /usr/local/bin/tlbus-client
WORKDIR /workspace
CMD ["tlbus-client", "--help"]

FROM runtime-base AS worker-runtime
COPY --from=builder /app/target/release/tlbus-worker /usr/local/bin/tlbus-worker
WORKDIR /workspace
CMD ["tlbus-worker", "--help"]

FROM python:3.12-slim AS pyclient-runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/tlbus-pyclient
COPY clients/pyclient/tlbus_pyclient.py /opt/tlbus-pyclient/tlbus_pyclient.py
ENV PYTHONPATH=/opt/tlbus-pyclient
CMD ["python3", "/opt/tlbus-pyclient/tlbus_pyclient.py", "--help"]
