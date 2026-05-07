---
sidebar_position: 5
---

# chdb — Embedded ClickHouse

The `chdb` extension embeds [chDB](https://github.com/chdb-io/chdb)
(ClickHouse-as-a-library) inside the Trex process. It runs ClickHouse SQL
against a ClickHouse-format catalog and surfaces results back as Trex tables
— so you can mix ClickHouse-native features (Materialized Views,
`ReplacingMergeTree`, ClickHouse-flavored windowing, the very deep
analytics function library) into Trex queries without standing up a separate
ClickHouse server.

## When to use it

- You have ClickHouse skills / queries / SQL snippets and want to keep them
  on a Trex deployment.
- You need a ClickHouse-specific feature DuckDB doesn't have — `Distributed`
  tables, `ReplicatedMergeTree`, ClickHouse JSON ops, etc.
- You're migrating from ClickHouse to Trex incrementally and want to keep
  read-only queries working during the transition.

If you only need to *query* an existing ClickHouse cluster, you don't need
this extension — use a ClickHouse JDBC client against the upstream cluster
directly. `chdb` is for embedding ClickHouse *into* Trex.

## How it works

```mermaid
flowchart LR
    Caller["SQL caller"]
    Caller -->|trex_chdb_execute| Chdb["chdb session"]
    Chdb --> ChStorage["ClickHouse-format storage"]
    Caller -->|SELECT * FROM trex_chdb_scan| ResultBuffer["Last query result buffer"]
    Chdb -->|after each execute| ResultBuffer
```

`chdb` runs as an in-process session. You start it once per node, execute
ClickHouse SQL with `trex_chdb_execute`, and read results back via
`trex_chdb_scan` (or its alias `trex_chdb_query`). The result buffer holds
**only** the most recent query — there is no streaming or cursor model.
Capture results into a Trex table (`CREATE TABLE … AS SELECT * FROM
trex_chdb_scan()`) if you need to keep them.

## Typical workflow

```sql
-- 1. Start chdb (once per node, persistent on disk)
SELECT trex_chdb_start('/data/chdb');

-- 2. Define a ClickHouse table
SELECT trex_chdb_execute(
  'CREATE TABLE events (
     ts DateTime,
     user_id UInt64,
     event String
   ) ENGINE = MergeTree ORDER BY (user_id, ts)'
);

-- 3. Bulk-load (e.g. from a Trex query)
COPY (SELECT * FROM memory.main.events_source)
  TO '/tmp/events.parquet' (FORMAT PARQUET);

SELECT trex_chdb_execute(
  'INSERT INTO events SELECT * FROM file(''/tmp/events.parquet'', ''Parquet'')'
);

-- 4. Run a ClickHouse-flavored aggregation
SELECT trex_chdb_execute(
  'SELECT user_id, count() AS n, anyLast(event) AS last_event
     FROM events
    GROUP BY user_id
    ORDER BY n DESC LIMIT 10'
);

-- 5. Pull results back into the Trex query plan
SELECT * FROM trex_chdb_scan();
```

## Functions

### `trex_chdb_start()`

Start the chDB session in ephemeral mode (data is held in memory and
discarded on stop). Useful for tests and short-lived workloads.

```sql
SELECT trex_chdb_start();
```

### `trex_chdb_start(path)`

Start chDB with a persistent on-disk catalog. Tables and data survive
restarts at the given path.

```sql
SELECT trex_chdb_start('/data/chdb');
```

The path is a regular ClickHouse data directory; `chdb` initializes it on
first use.

### `trex_chdb_stop()`

Stop the session. Persistent storage at `/data/chdb` (if used) remains on
disk; ephemeral data is lost.

```sql
SELECT trex_chdb_stop();
```

### `trex_chdb_execute(query)`

Execute any ClickHouse SQL — DDL, DML, or SELECT. The result of a SELECT
is buffered for retrieval via `trex_chdb_scan()`. DDL/DML returns a status
string.

```sql
SELECT trex_chdb_execute('CREATE TABLE t (id UInt32) ENGINE = MergeTree ORDER BY id');
SELECT trex_chdb_execute('INSERT INTO t VALUES (1), (2), (3)');
SELECT trex_chdb_execute('SELECT * FROM t');
SELECT * FROM trex_chdb_scan();   -- → 1, 2, 3
```

The buffer is overwritten on every execute, so chain `execute` and `scan`
without intervening calls if you need both halves.

### `trex_chdb_scan()` / `trex_chdb_query()`

Read the most recent query result as a Trex table. Columns are inferred
from the ClickHouse result schema. The two names are aliases.

```sql
SELECT * FROM trex_chdb_scan();
```

## Operational notes

- **Single session per process.** `chdb` is a singleton — calling
  `trex_chdb_start` twice without `trex_chdb_stop` in between is a no-op.
- **Memory pressure.** chDB allocates separately from the Trex engine; large
  in-memory MergeTree tables count against process RSS, not Trex's normal
  memory accounting.
- **No CDC into chdb.** The `etl` extension replicates Postgres → Trex
  catalog. If you want continuous data into chDB tables, write a transform
  plugin or use ClickHouse's own table functions (e.g. `postgresql()`).
- **AMD64 only.** The chDB native library currently ships only for amd64.
  ARM builds load the extension but `trex_chdb_start` will fail.

## Next steps

- [SQL Reference → etl](etl) — replicate Postgres into Trex (and from there
  optionally pivot into chDB tables).
- [Concepts → Query Pipeline](../concepts/query-pipeline) — how `chdb`
  scans interact with the rest of a query plan.
