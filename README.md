# Trex

A self-hosted backend platform with an analytical column-store engine and federation built in, packaged as a single binary.

Trex is Supabase-compatible at the wire level — the Auth, REST (PostgREST), and Functions APIs preserve the Supabase contracts — and includes forks of several Supabase open-source components (Storage, Edge Runtime, CLI, pg-meta, ETL). On top of that base it adds an embedded analytical engine (a DuckDB fork) you can point at Parquet on S3, BigQuery, MySQL, SAP HANA, ClickHouse, or another Postgres in one SQL statement; a GraphQL layer (PostGraphile); a Model Context Protocol server; a plugin system; and domain extensions for healthcare and observational research (FHIR, OHDSI Atlas, Clinical Quality Language, in-process LLM inference).

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

## Upstreams

Components Trex reuses or integrates. Each fork is maintained as a submodule (or separate repo) and retains its upstream copyright and license; see the `LICENSE` file inside each submodule.

- **Supabase Storage** (fork, Apache-2.0): object storage service
- **Supabase CLI** (fork, MIT): project tooling
- **Supabase pg-meta** (fork, Apache-2.0): Postgres metadata API
- **Supabase Edge Runtime** (fork, MIT): edge function runtime
- **Supabase ETL** (Apache-2.0): used as a library at a pinned commit
- **Supabase Auth and Functions** (HTTP contracts): reimplemented in own code, wire-compatible
- **DuckDB** (fork, MIT): analytical engine and C-API extension framework
- **Postgres** (PostgreSQL License): application data, auth schema, and the wire protocol Trex speaks
- **PostgREST** (MIT): REST API layer over Postgres (used unmodified as a sidecar)
- **PostGraphile** (MIT): GraphQL API layer over Postgres
- **Better Auth** (MIT): identity provider, session management, OAuth/OIDC flows (coexists with the Supabase Auth-compatible router)
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
