# Stage 1: Build the trex binary
FROM debian:trixie-slim AS builder

ARG TREXSQL_VERSION=v1.4.4-trex
ARG CHDB_VERSION=v3.6.0

RUN apt-get update && apt-get install -y curl unzip wget gcc libc6-dev && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.85.1
ENV PATH="/root/.cargo/bin:${PATH}"

# Download libtrexsql from GitHub release
RUN mkdir -p /opt/trexsql && \
    wget -O /tmp/libtrexsql.zip \
      https://github.com/p-hoffmann/trexsql-rs/releases/download/${TREXSQL_VERSION}/libtrexsql-linux-amd64.zip && \
    unzip /tmp/libtrexsql.zip -d /opt/trexsql && \
    rm /tmp/libtrexsql.zip

ENV TREXSQL_LIB_DIR=/opt/trexsql
ENV TREXSQL_INCLUDE_DIR=/opt/trexsql

# Download libchdb from GitHub release
RUN cd /tmp && \
    wget -O libchdb.tar.gz \
      https://github.com/chdb-io/chdb/releases/download/${CHDB_VERSION}/linux-x86_64-libchdb.tar.gz && \
    tar -xzf libchdb.tar.gz && \
    mv libchdb.so /opt/ && \
    rm -f libchdb.tar.gz chdb.h

# Cache dependency build: copy manifests first, build with dummy src, then replace
COPY Cargo.toml Cargo.lock /usr/src/trexsql/
WORKDIR /usr/src/trexsql
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/trex target/release/deps/trexsql-* target/release/.fingerprint/trexsql-*

COPY src/ /usr/src/trexsql/src/
RUN cargo build --release

# Stage 2: Minimal runtime image
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      libssl3 libgomp1 ca-certificates libvulkan1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/trexsql/libtrexsql.so /usr/lib/
COPY --from=builder /opt/libchdb.so /usr/lib/
COPY --from=builder /usr/src/trexsql/target/release/trex /usr/bin/

RUN mkdir -p /usr/lib/trexsql/extensions
COPY extensions/*.trex /usr/lib/trexsql/extensions/

RUN ldconfig

ENTRYPOINT ["trex"]
