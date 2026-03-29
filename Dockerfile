# Stage 1: Build the trex binary
FROM debian:trixie-slim AS builder

ARG TREXSQL_VERSION=v1.4.4-trex
ARG CHDB_VERSION=v3.6.0
ARG TARGETARCH

RUN apt-get update && apt-get install -y curl unzip wget gcc libc6-dev && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.88.0
ENV PATH="/root/.cargo/bin:${PATH}"

# Download libtrexsql from GitHub release (arch-specific)
RUN mkdir -p /opt/trexsql && \
    wget -O /tmp/libtrexsql.zip \
      https://github.com/p-hoffmann/trexsql-rs/releases/download/${TREXSQL_VERSION}/libtrexsql-linux-${TARGETARCH}.zip && \
    unzip /tmp/libtrexsql.zip -d /opt/trexsql && \
    rm /tmp/libtrexsql.zip && \
    ln -sf /opt/trexsql/libtrexsql.so /opt/trexsql/libduckdb.so

ENV TREXSQL_LIB_DIR=/opt/trexsql
ENV TREXSQL_INCLUDE_DIR=/opt/trexsql

# Download libchdb from GitHub release (amd64 only — no ARM build available)
RUN mkdir -p /opt/chdb && \
    if [ "$TARGETARCH" = "amd64" ]; then \
      cd /tmp && \
      wget -O libchdb.tar.gz \
        https://github.com/chdb-io/chdb/releases/download/${CHDB_VERSION}/linux-x86_64-libchdb.tar.gz && \
      tar -xzf libchdb.tar.gz && \
      mv libchdb.so /opt/chdb/ && \
      rm -f libchdb.tar.gz chdb.h; \
    fi

# Cache dependency build: copy manifests first, build with dummy src, then replace
COPY Cargo.toml Cargo.lock /usr/src/trexsql/
WORKDIR /usr/src/trexsql
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs && \
    cargo build --release && \
    rm -rf src target/release/trex target/release/libtrexsql_engine* \
      target/release/deps/trexsql* target/release/.fingerprint/trexsql-*

COPY src/ /usr/src/trexsql/src/
RUN cargo build --release

# Stage 2: Build devx frontend
FROM node:20-trixie-slim AS devx-builder
WORKDIR /build
COPY plugins/devx/package.json plugins/devx/package-lock.json plugins/devx/tsconfig*.json plugins/devx/vite.config.ts plugins/devx/index.html ./
COPY plugins/devx/src/ ./src/
RUN npm install && npm run build

# Stage 3: Build web frontend
FROM node:20-trixie-slim AS web-builder
WORKDIR /build
COPY plugins/web/package.json plugins/web/package-lock.json plugins/web/tsconfig*.json plugins/web/vite.config.ts plugins/web/index.html plugins/web/components.json ./
COPY plugins/web/src/ ./src/
RUN npm install && npm run build

# Stage 4: Build notebook frontend
FROM node:20-trixie-slim AS notebook-builder
WORKDIR /build
COPY plugins/notebook/package.json plugins/notebook/package-lock.json plugins/notebook/tsconfig*.json plugins/notebook/vite.config.ts plugins/notebook/index.html ./
COPY plugins/notebook/src/ ./src/
COPY plugins/notebook/public/ ./public/
RUN npm install && npm run build

# Stage 5: Build docs site
FROM node:20-trixie-slim AS docs-builder
WORKDIR /build
COPY plugins/docs/package.json plugins/docs/package-lock.json plugins/docs/tsconfig.json plugins/docs/docusaurus.config.ts plugins/docs/sidebars.ts ./
COPY plugins/docs/docs/ ./docs/
COPY plugins/docs/src/ ./src/
COPY plugins/docs/static/ ./static/
RUN npm install && npm run build

# Stage 6: Runtime
FROM node:20-trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      libssl3 libgomp1 ca-certificates libvulkan1 curl && \
    rm -rf /var/lib/apt/lists/*

# Copy trex binary, libtrexsql, and libtrexsql_engine
COPY --from=builder /usr/src/trexsql/target/release/trex /usr/bin/
COPY --from=builder /opt/trexsql/libtrexsql.so /usr/lib/
COPY --from=builder /opt/chdb/ /usr/lib/
COPY --from=builder /usr/src/trexsql/target/release/libtrexsql_engine.so /usr/lib/
RUN ldconfig

WORKDIR /usr/src

# Install extensions via npm
COPY package.json package-lock.json .npmrc deno.json ./
RUN npm install

# Collect extension files from node_modules into extensions dir
RUN mkdir -p /usr/lib/trexsql/extensions && \
    find node_modules/@trex -name "*.trex" -exec cp {} /usr/lib/trexsql/extensions/ \; && \
    find node_modules/@trex -name "*.duckdb_extension" -exec cp {} /usr/lib/trexsql/extensions/ \;

# Download official trexsql extensions for offline use (arch-specific)
ARG TARGETARCH
ENV DUCKDB_VERSION=1.4.4
RUN DUCKDB_PLATFORM="linux_${TARGETARCH}" && \
    mkdir -p /usr/share/trexsql/extensions/v${DUCKDB_VERSION}/${DUCKDB_PLATFORM} && \
    cd /usr/share/trexsql/extensions/v${DUCKDB_VERSION}/${DUCKDB_PLATFORM} && \
    for lib in avro aws delta ducklake fts httpfs icu iceberg inet json mysql_scanner parquet postgres_scanner spatial sqlite sqlite_scanner vss; do \
        curl -sfO https://extensions.duckdb.org/v${DUCKDB_VERSION}/${DUCKDB_PLATFORM}/${lib}.duckdb_extension.gz && \
        gzip -d ${lib}.duckdb_extension.gz; \
    done && \
    for lib in bigquery; do \
        curl -sfO https://community-extensions.duckdb.org/v${DUCKDB_VERSION}/${DUCKDB_PLATFORM}/${lib}.duckdb_extension.gz && \
        gzip -d ${lib}.duckdb_extension.gz; \
    done

# Override npm extensions with CI-built ones
# Supports both flat layout (local builds) and arch-specific layout (CI multi-arch builds)
COPY extensions/ /tmp/all-extensions/
RUN if [ -d "/tmp/all-extensions/${TARGETARCH}" ]; then \
      cp -f /tmp/all-extensions/${TARGETARCH}/*.trex /usr/lib/trexsql/extensions/ 2>/dev/null || true; \
      cp -f /tmp/all-extensions/${TARGETARCH}/*.duckdb_extension /usr/lib/trexsql/extensions/ 2>/dev/null || true; \
    else \
      cp -f /tmp/all-extensions/*.trex /usr/lib/trexsql/extensions/ 2>/dev/null || true; \
      cp -f /tmp/all-extensions/*.duckdb_extension /usr/lib/trexsql/extensions/ 2>/dev/null || true; \
    fi && rm -rf /tmp/all-extensions

# Download and extract Shinylive assets for analytics dashboards
ARG SHINYLIVE_VERSION=0.10.7
RUN curl -sLO https://github.com/posit-dev/shinylive/releases/download/v${SHINYLIVE_VERSION}/shinylive-${SHINYLIVE_VERSION}.tar.gz && \
    tar -xzf shinylive-${SHINYLIVE_VERSION}.tar.gz && \
    mv shinylive-${SHINYLIVE_VERSION} shinylive && \
    rm shinylive-${SHINYLIVE_VERSION}.tar.gz

# Create plugins directory and symlink @trex npm packages for plugin scanner
RUN mkdir -p ./plugins && \
    ln -sf $(pwd)/node_modules/@trex ./plugins/@trex

# Copy core package manifests first and install dependencies (cache-friendly)
COPY core/server/package.json core/server/package-lock.json ./core/server/
COPY core/event/package.json core/event/package-lock.json ./core/event/
RUN cd /usr/src/core/server && npm install --omit=dev && \
    cd /usr/src/core/event && npm install --omit=dev

# Copy remaining core source
COPY core/ ./core/

# Install Playwright with headless Chromium only for QA/design review tools
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright-browsers
ENV NODE_PATH=/usr/lib/node_modules
RUN npm install -g playwright@latest && \
    npx playwright install --with-deps chromium && \
    rm -rf /tmp/* /root/.cache/ms-playwright-*

# Copy functions
COPY functions/ ./functions/

# Create plugins/runtime workspace member stub (referenced by deno.json workspace)
RUN mkdir -p ./plugins/runtime && echo '{"nodeModulesDir":"auto"}' > ./plugins/runtime/deno.json

# Copy dev plugins (use pre-built dist from builder stages)
COPY plugins/devx/ ./plugins-dev/devx/
COPY --from=devx-builder /build/dist/ ./plugins-dev/devx/dist/
COPY plugins/web/ ./plugins-dev/web/
COPY --from=web-builder /build/dist/ ./plugins-dev/web/dist/
COPY plugins/notebook/ ./plugins-dev/notebook/
COPY --from=notebook-builder /build/dist/ ./plugins-dev/notebook/dist/
COPY plugins/docs/ ./plugins-dev/docs/
COPY --from=docs-builder /build/build/ ./plugins-dev/docs/build/
COPY plugins/storage/ ./plugins-dev/storage/

# Generate self-signed TLS cert for HTTPS
RUN openssl req -new -x509 -days 3650 -nodes \
      -out /usr/src/server.crt -keyout /usr/src/server.key \
      -subj '/CN=localhost' && \
    chown node:node /usr/src/server.crt /usr/src/server.key && \
    chmod 644 /usr/src/server.crt && chmod 600 /usr/src/server.key

ENV SCHEMA_DIR=/usr/src/core/schema
ENV DUCKDB_EXTENSION_DIRECTORY=/usr/share/trexsql/extensions

# Ensure workspace directory exists and is writable by node user
RUN mkdir -p /tmp/devx-workspaces && chown node:node /tmp/devx-workspaces

EXPOSE 8001 8000
USER node
ENTRYPOINT ["trex"]
