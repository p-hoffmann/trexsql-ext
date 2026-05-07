# Trex

A self-hosted backend platform with an analytical column-store engine and federation built in, packaged as a single binary.

Trex is **Supabase-compatible** at the wire level — the auth (GoTrue), REST (PostgREST), and Functions APIs preserve the Supabase contracts — and includes forks of several Supabase open-source components (Storage, Edge Runtime, CLI, pg-meta, ETL). On top of that base it adds an embedded analytical engine (a DuckDB fork) you can point at Parquet on S3, BigQuery, MySQL, SAP HANA, ClickHouse, or another Postgres in one SQL statement; a GraphQL layer (PostGraphile); a Model Context Protocol server; a plugin system; and domain extensions for healthcare and observational research (FHIR, OHDSI Atlas, Clinical Quality Language, in-process LLM inference).

## Quick start

```bash
git clone --recurse-submodules <repo>
cd trex
docker compose up -d
```

For live source mounts during plugin development, use `docker-compose.dev.yml`.

## What you get out of the box

`docker compose up` brings up three services:

- **postgres**: vanilla Postgres 16, used for application data, the auth schema, and PostgREST's source of truth.
- **postgrest**: auto-generated REST API over Postgres.
- **trex**: the analytical core. Speaks four protocols:
  - `:8001`: HTTP for the web UI, edge functions, GraphQL, MCP server, auth
  - `:8000`: TLS-terminated HTTPS variant of the above
  - `:5433`: Postgres wire protocol (connect with `psql`, JDBC, `pg_dump`, etc.)

In multi-node deployments the `db` extension also opens a UDP gossip port and an Arrow Flight SQL port; these are configured per deployment and not exposed by the default compose.

Out of the box you have:

- **Identity provider**: user signup, sign-in, magic links, password reset, email verification, role-based access, session management, an OAuth/OIDC consent flow, and machine-to-machine API keys.
- **GraphQL API** auto-generated over Postgres, served at `/trex/graphql` with a GraphiQL explorer at `/trex/graphiql`.
- **Subscriptions**: GraphQL subscriptions and live queries over Postgres `LISTEN`/`NOTIFY` push change streams to clients.
- **REST API** auto-generated over Postgres, served at `/trex/rest/v1`.
- **Edge function secrets**: encrypted secrets store with REST endpoints to list / set / unset. Secrets are loaded into edge function environments at invocation time, so functions read them as plain env vars without shipping them in code or in the image.
- **Vector search**: HNSW indexes on embedding columns inside the analytical engine, queryable in the same SQL as the rest of your data.

## Analytical database

The `trex` binary embeds a column-store engine that auto-loads a curated set of extensions on startup. Out of the box you can:

- **Federate across heterogeneous sources**: query Postgres, MySQL, SQLite, BigQuery, SAP HANA, ClickHouse, S3, and HTTP-served files in a single SQL statement.
- **Read open table formats**: Apache Iceberg, Delta Lake, ducklake, Parquet, Avro, JSON. Write support varies by format and tracks the upstream DuckDB extensions.
- **Run search workloads**: full-text search (BM25), vector similarity (HNSW), GIS / spatial queries.
- **Speak Postgres wire**: clients connect with `psql`, JDBC, or `pg_dump` and see Trex as just another Postgres.
- **Run distributed**: multi-node cluster coordination over Arrow Flight SQL, Postgres CDC replication into Trex, schema migrations, declarative data transforms, runtime extension installation.
- **Use domain extensions**: FHIR server, OHDSI Atlas cohort-to-SQL, Clinical Quality Language to ELM, and in-process LLM inference (Vulkan in the standard image; CUDA and Metal supported via custom builds).

## Relationship to Supabase

Trex's control-plane surface reuses several Supabase open-source components and implements wire compatibility with others. Specifically:

**Forks of Supabase repositories** (vendored and modified):

- **Storage**: `plugins/storage/supabase-storage/` — fork of `supabase/storage` (Apache-2.0)
- **CLI**: `plugins/cli/` — fork of `supabase/cli` (MIT)
- **pg-meta**: `plugins/pg-meta/postgres-meta/` — fork of `supabase/postgres-meta` (Apache-2.0)
- **Edge Runtime**: `plugins/runtime/trex-runtime/` — fork of `supabase/edge-runtime` (MIT)

**Wire-compatible reimplementations** (own code, matching the upstream HTTP contract):

- **Auth**: `core/server/auth/auth-router.ts` implements the Supabase GoTrue REST API. Existing clients written against GoTrue work without modification.
- **Functions API**: matches the Supabase Functions HTTP contract.

**Library dependencies**:

- **ETL**: `plugins/etl/` depends on Supabase's open-source ETL library at a pinned commit (Apache-2.0).

**Used unmodified**:

- **PostgREST**: runs as an unmodified upstream container (sidecar), reverse-proxied at `/rest/v1/*`.

What Trex adds on top of that base:

- An embedded analytical column-store engine (a DuckDB fork) reachable via Postgres wire on port 5433
- Federation extensions for SAP HANA, embedded ClickHouse (chDB), and peer Trex nodes
- A custom DataFusion-based distributed query scheduler with chitchat gossip and Arrow Flight SQL transport
- In-process LLM inference (llama.cpp) composed with HNSW vector search
- A FHIR R4 server, OHDSI Atlas cohort translator, and Clinical Quality Language compiler as plugins
- A Model Context Protocol (MCP) server exposing the management surface to LLM clients
- GraphQL via PostGraphile (rather than Supabase's pg_graphql) with a custom plugin for cross-cutting admin operations
- A plugin system that ships database extensions, UI surfaces, edge functions, scheduled flows, and dbt-style transformation projects as one npm package
- Runs against stock Postgres 16 — no custom Postgres extensions required, so any managed Postgres works

Notable Supabase components Trex does **not** include: Realtime, Studio (Trex has its own admin UI), Logflare/Analytics, and the SMS/email infrastructure that Supabase ships for delivered messages.

The architectural difference relative to Supabase is mostly a deployment-shape choice. Supabase runs its services as separate containers, which is the right default for many deployments — independent scaling, mature operational tooling, and a polished hosted offering. Trex collapses the same surface into a single binary by loading each capability as a DuckDB extension and embedding the Express server inside the SQL engine. The single-binary shape is suited to small and medium self-hosted deployments and to scenarios where colocating analytics with the application backend matters.

## Other upstreams

Components Trex reuses or integrates beyond the Supabase base. Each fork is maintained as a submodule (or separate repo) and retains its upstream copyright and license; see the `LICENSE` file inside each submodule.

- **DuckDB** (fork, MIT): analytical engine and C-API extension framework
- **Postgres** (PostgreSQL License): application data, auth schema, and the wire protocol Trex speaks
- **PostgREST** (MIT): REST API layer over Postgres (used unmodified as a sidecar)
- **PostGraphile** (MIT): GraphQL API layer over Postgres
- **Better Auth** (MIT): identity provider, session management, OAuth/OIDC flows (coexists with the GoTrue-compatible router)
- **chitchat** (MIT, Quickwit): cluster gossip protocol
- **Apache Arrow / DataFusion** (Apache-2.0): in-memory columnar format and distributed query planner
- **llama.cpp** (MIT): in-process LLM inference engine

## Building from source

Most extensions follow the same Makefile pattern:

```bash
make configure      # one-time setup (Python venv, platform detection)
make debug          # debug build
make release        # release build
make test_debug     # run tests
make clean          # remove build/
make clean_all      # remove build/ and configure/
```

To rebuild the trex container locally:

```bash
docker compose -f docker-compose.dev.yml build trex
```

## License

Apache-2.0.
