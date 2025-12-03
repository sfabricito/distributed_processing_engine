# syntax=docker/dockerfile:1

# Use a bookworm image to align with libssl3
FROM rust:1-bookworm AS builder
WORKDIR /app

# Dependencies for native crates (e.g., openssl/sqlite)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
# Pre-fetch deps
RUN mkdir src && echo "fn main(){}" > src/main.rs && cargo build --release || true
RUN rm -rf src

COPY src src
COPY data data

RUN cargo build --release --bin distributed_processing_engine --bin mini-spark-cli

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl jq openssl && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=builder /app/target/release/distributed_processing_engine /usr/local/bin/distributed_processing_engine
COPY --from=builder /app/target/release/mini-spark-cli /usr/local/bin/mini-spark-cli
COPY data /app/data

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/distributed_processing_engine"]
