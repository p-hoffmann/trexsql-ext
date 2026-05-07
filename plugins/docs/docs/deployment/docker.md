---
sidebar_position: 1
---

# Docker Deployment

Trex is published as a single multi-arch Docker image
(`ghcr.io/p-hoffmann/trexsql:latest`) that bundles the `trex` Rust binary, the
auto-loaded extensions, the Deno-based core management application, and the web
frontend. The repository ships three compose files for different scenarios.

## Compose Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Default stack: Postgres 16 + Trex + PostgREST. Uses the published image. |
| `docker-compose.dev.yml` | Development overlay. Live-mounts `core/server`, `functions`, `plugins-dev`, and the web/docs `dist/` directories so changes hot-reload into the running container. |
| `docker-compose.pg-trex.yml` | Replaces vanilla Postgres with the `pg-trex` image (Postgres + the Trex extensions co-located in one process). |

## Quick Start

```bash
docker compose up -d
```

This starts:

- **postgres** (`postgres:16`) — application metadata + the auth schema. Published
  on host port `65433` so it doesn't collide with the Trex pgwire endpoint.
- **trex** — the Trex container. Publishes the web/MCP/REST/GraphQL HTTP endpoints
  on `8001`, the TLS variant on `8000`, and the pgwire endpoint on `5433`.
- **postgrest** (`postgrest:v12.2.3`) — auto-generated REST API over Postgres. It
  is reverse-proxied through Trex at `${BASE_PATH}/rest/v1/*`.

## Published Ports

| Host | Container | Service |
|------|-----------|---------|
| `8001` | `8001` | HTTP — Web UI, GraphQL, REST proxy, MCP, edge functions, auth. |
| `8000` | `8000` | HTTPS — TLS-terminated variant of the same surface. |
| `5433` | `5432` | pgwire — Postgres-compatible wire protocol into the analytical engine. |
| `65433` | `5432` | Postgres metadata DB (only `docker-compose.yml`). |

The gossip / cluster-membership port (default `4200`) is **not** published by the
default compose file. Enable it explicitly when running multi-node deployments.

## Default `docker-compose.yml`

```yaml
volumes:
  core-pgdata:

services:
  postgres:
    image: postgres:16
    ports:
      - 65433:5432
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: mypass
      POSTGRES_DB: testdb
    volumes:
      - core-pgdata:/var/lib/postgresql/data
      - ./core/seed.sql:/docker-entrypoint-initdb.d/seed.sql
      - ./core/schema:/docker-entrypoint-initdb.d/schema
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s

  trex:
    image: ghcr.io/p-hoffmann/trexsql:latest
    platform: linux/amd64
    ports:
      - 8000:8000
      - 8001:8001
      - 5433:5432
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      DATABASE_URL: postgres://postgres:mypass@postgres:5432/testdb
      BETTER_AUTH_SECRET: dev-secret-at-least-32-characters-long!!
      BASE_PATH: /trex
      BETTER_AUTH_URL: http://localhost:8001/trex
      PLUGINS_PATH: /usr/src/plugins
      PLUGINS_DEV_PATH: /usr/src/plugins-dev
      SCHEMA_DIR: /usr/src/core/schema
      POSTGREST_HOST: postgrest
      POSTGREST_PORT: "3000"
      SWARM_CONFIG: >-
        {"cluster_id":"local","nodes":{"local":{
          "gossip_addr":"0.0.0.0:4200",
          "extensions":[
            {"name":"trexas","config":{...}},
            {"name":"pgwire","config":{"host":"0.0.0.0","port":5432}}
          ]}}}
      SWARM_NODE: local

  postgrest:
    image: postgrest/postgrest:v12.2.3
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      PGRST_DB_URI: postgres://authenticator:authenticator_pass@postgres:5432/testdb
      PGRST_DB_SCHEMAS: public
      PGRST_DB_ANON_ROLE: anon
      PGRST_JWT_SECRET: dev-secret-at-least-32-characters-long!!
```

## Development Overlay

For live source mounts during plugin / server development:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```

The overlay bind-mounts `./core/server`, `./functions`, `./plugins-dev`, and the
prebuilt `./plugins/web/dist` and `./plugins/docs` into the container so edits on
the host take effect without rebuilding.

## Services Started by Default

The Rust `trex` binary boots the engine, loads every `*.trex` / `*.duckdb_extension`
in `EXTENSION_DIR` (default `/usr/lib/trexsql/extensions`), then iterates the
`extensions` array in `SWARM_CONFIG` to start service extensions. The default
compose starts:

- **trexas** — the core HTTP server (Express + Deno). Mounts the web UI, GraphQL,
  GraphiQL, MCP, edge functions, REST proxy, and auth on `:8001` (HTTP) and
  `:8000` (HTTPS).
- **pgwire** — Postgres wire protocol on `:5432` (published as host `:5433`).

The Arrow Flight SQL service (`flight`) and gossip cluster membership are
**not** started by the default compose file. Add them to `SWARM_CONFIG` when
running distributed deployments.

## Dockerfile

The image is built in five stages:

1. **builder** (`debian:trixie-slim`) — Installs Rust 1.88, downloads the pinned
   `libtrexsql.so` (and `libchdb.so` on amd64) from GitHub release artifacts, then
   builds the `trex` binary with cached dependency layers.
2. **web-builder** (`node:22-trixie-slim`) — Builds the admin web UI (`plugins/web`).
3. **notebook-builder** (`node:22-trixie-slim`) — Builds the React notebook bundle
   (`plugins/notebook`).
4. **docs-builder** (`node:22-trixie-slim`) — Builds the Docusaurus docs site.
5. **runtime** (`node:22-trixie-slim`) — Installs the runtime dependencies, copies
   the artefacts from the previous stages, fetches the npm-distributed Trex
   extensions into `/usr/lib/trexsql/extensions/`, and sets `trex` as the entry
   point.

The `TREXSQL_VERSION` and `CHDB_VERSION` build args pin the upstream native
libraries.

## Accessing Services

| Service | URL |
|---------|-----|
| Web UI | http://localhost:8001/trex/ |
| GraphiQL | http://localhost:8001/trex/graphiql (set `ENABLE_GRAPHIQL=true`) |
| Documentation | http://localhost:8001/trex/docs/ |
| MCP | http://localhost:8001/trex/mcp |
| REST (PostgREST proxy) | http://localhost:8001/trex/rest/v1 |
| Postgres metadata | `postgresql://postgres:mypass@localhost:65433/testdb` |
| pgwire (analytical engine) | `postgresql://localhost:5433/main` |
| HTTPS (self-signed) | https://localhost:8000/trex/ |

For cloud-managed deployments, see [`deploy/`](https://github.com/p-hoffmann/trexsql/tree/main/deploy)
which uses Pulumi to provision Trex on AWS ECS Fargate or Azure Container Apps.
