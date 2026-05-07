---
sidebar_position: 6
---

# etl — PostgreSQL CDC Replication

The `etl` extension continuously replicates a Postgres database into the Trex
analytical engine. It reads Postgres' logical replication stream, converts each
INSERT/UPDATE/DELETE into Arrow batches, and applies them to mirrored Trex
tables in real time.

Use it when you want analytical queries over operational data **without** going
back to the OLTP Postgres on every read. A federated `ATTACH` (see
[hana](hana) / Postgres scanner) hits the source on each query; a CDC pipeline
maintains a local materialized copy that can serve queries from columnar
storage at sub-second latency.

## How it works

```mermaid
flowchart LR
    PG["Source PostgreSQL"]
    PG -->|wal2json / pgoutput| Pipeline["etl pipeline<br/>(bao task)"]
    Pipeline -->|Arrow batches| Engine["Trex Engine<br/>(mirrored tables)"]
    Engine --> Query["SELECT, JOIN, AGG..."]
```

Each pipeline is a long-running async task scheduled by the `bao` orchestrator
(see [Concepts → Connection Pool](../concepts/connection-pool)). It uses
Postgres' built-in logical replication, so the source needs:

- `wal_level = logical` in `postgresql.conf`.
- A `REPLICATION` privilege for the connection user.
- A publication declaring which tables to replicate (`CREATE PUBLICATION
  trex_pub FOR TABLE …`). The pipeline auto-creates a replication slot but
  **not** the publication — that's a deliberate Postgres-side decision.

## Replication modes

| Mode | What it does | When to use |
|------|--------------|-------------|
| `snapshot` | Bulk-copy current table contents once, then stop. | One-shot loads, dev/test. |
| `cdc` | Tail the logical replication slot for INSERT/UPDATE/DELETE indefinitely. | Standing replication. Default for production. |
| `snapshot_then_cdc` | Bulk-copy, then switch to CDC streaming with no gap. | Initial bootstrap of a long-lived pipeline. |

## Typical workflow

```sql
-- 1. On the source Postgres (one-time setup):
--      ALTER SYSTEM SET wal_level = logical;       -- requires restart
--      CREATE PUBLICATION trex_pub FOR ALL TABLES;
--      CREATE ROLE trex_repl WITH REPLICATION LOGIN PASSWORD '...';

-- 2. In Trex, start the pipeline:
SELECT trex_etl_start(
  'orders_pipeline',
  'postgres://trex_repl:pass@source-db:5432/app',
  'snapshot_then_cdc',
  1000,    -- batch size
  5000,    -- batch timeout (ms)
  3000,    -- retry delay (ms)
  5        -- max retries
);

-- 3. Verify it's running:
SELECT * FROM trex_etl_status();

-- 4. Query mirrored data:
SELECT customer_id, COUNT(*)
  FROM orders
 GROUP BY customer_id;

-- 5. Stop when done (e.g. before draining the source):
SELECT trex_etl_stop('orders_pipeline');
```

## Functions

### `trex_etl_start(name, connection_string, ...)`

Start a CDC replication pipeline. Three signatures, picked by argument count:

**Minimal** — defaults to `cdc` mode, batch size 500, 1s timeout, 1s retry,
3 retries:

```sql
SELECT trex_etl_start('my_pipeline', 'postgres://user:pass@host:5432/db');
```

**With mode**:

```sql
SELECT trex_etl_start('my_pipeline', 'postgres://...', 'snapshot');
```

**Full configuration**:

| Parameter | Type | Description |
|-----------|------|-------------|
| name | VARCHAR | Pipeline name. Used as the replication slot name. |
| connection_string | VARCHAR | Postgres URL with `REPLICATION` privilege. |
| mode | VARCHAR | `snapshot`, `cdc`, or `snapshot_then_cdc`. |
| batch_size | INTEGER | Rows per Arrow batch flushed to the engine. |
| batch_timeout | INTEGER | Maximum ms to wait before flushing a partial batch. |
| retry_delay | INTEGER | Initial delay between retry attempts on connection failure. |
| retry_max | INTEGER | Maximum retry attempts before the pipeline goes to `error` state. |

```sql
SELECT trex_etl_start(
  'my_pipeline',
  'postgres://user:pass@host:5432/db',
  'cdc',
  1000, 5000, 3000, 5
);
```

Tuning: bigger `batch_size` improves throughput but increases peak memory and
end-to-end latency. `batch_timeout` is the *maximum* a row sits in the buffer
before being applied — set it to your latency target.

### `trex_etl_stop(name)`

Stop a pipeline. The replication slot is **not** dropped — call it again with
the same name to resume. Drop the slot manually on the source if you're
permanently retiring the pipeline (`pg_drop_replication_slot('my_pipeline')`).

```sql
SELECT trex_etl_stop('my_pipeline');
```

### `trex_etl_status()`

Show every pipeline known to this node.

**Returns:** TABLE

| Column | Description |
|--------|-------------|
| name | Pipeline name. |
| state | `running`, `stopped`, `error`, `connecting`, `snapshotting`. |
| mode | The mode the pipeline was started with. |
| connection | Connection string (password redacted). |
| publication | The Postgres publication being read. |
| snapshot | Snapshot LSN if applicable. |
| rows_replicated | Cumulative count since pipeline start. |
| last_activity | Last time a batch was flushed. |
| error | Last error string if state is `error`. |

```sql
SELECT name, state, rows_replicated, last_activity, error
  FROM trex_etl_status();
```

## Operational notes

- **Replication slots accumulate WAL on the source** until the pipeline reads
  them. A stopped or stuck pipeline can fill the source's disk — set up
  alerting on `pg_replication_slots.confirmed_flush_lsn` lag.
- **Schema changes are not replicated.** The pipeline reads data, not DDL. If
  you `ALTER TABLE` on the source, restart the pipeline and Trex will adopt
  the new schema on the next snapshot.
- **One pipeline = one slot = one publication.** Don't attempt to point two
  pipelines at the same slot name; results are undefined.
- **TLS**: append `?sslmode=require` to the connection string and set
  `DB_TLS_CA_PATH` / `DB_TLS_INSECURE` on the Trex container as needed.
- The pipeline runs on `bao` workers — a single Trex node can host many
  pipelines without dedicated processes.

## Next steps

- [Concepts → Connection Pool](../concepts/connection-pool) — how `bao`
  schedules ETL work alongside other async tasks.
- [SQL Reference → db](db) — distributing replicated tables across cluster
  nodes via `trex_db_partition_table`.
- [Quickstart: Federate a Postgres database](../quickstarts/federate-postgres)
  — when you don't need replication and a live federated query suffices.
