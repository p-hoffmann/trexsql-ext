# Stage 1: Build the trex binary
FROM debian:trixie-slim AS builder

ARG TREXSQL_VERSION=v1.4.0-trex

RUN apt-get update && apt-get install -y curl unzip wget gcc libc6-dev && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.84.0
ENV PATH="/root/.cargo/bin:${PATH}"

# Download libtrexsql from GitHub release
RUN mkdir -p /opt/trexsql && \
    wget -O /tmp/libtrexsql.zip \
      https://github.com/p-hoffmann/trexsql-rs/releases/download/${TREXSQL_VERSION}/libtrexsql-linux-amd64.zip && \
    unzip /tmp/libtrexsql.zip -d /opt/trexsql && \
    rm /tmp/libtrexsql.zip

ENV TREXSQL_LIB_DIR=/opt/trexsql
ENV TREXSQL_INCLUDE_DIR=/opt/trexsql

# Cache dependency build: copy manifests first, build with dummy src, then replace
COPY Cargo.toml /usr/src/trexsql/
WORKDIR /usr/src/trexsql
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/trex target/release/deps/trexsql-* target/release/.fingerprint/trexsql-*

COPY src/ /usr/src/trexsql/src/
RUN cargo build --release

# Stage 2: Minimal runtime image
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      libssl3 libgomp1 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/trexsql/libtrexsql.so /usr/lib/
COPY --from=builder /usr/src/trexsql/target/release/trex /usr/bin/

RUN mkdir -p /usr/lib/trexsql/extensions
COPY extensions/*.trex /usr/lib/trexsql/extensions/

RUN ldconfig

ENTRYPOINT ["trex"]
