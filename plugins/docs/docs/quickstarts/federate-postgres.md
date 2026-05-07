---
sidebar_position: 2
---

# Federate a Postgres database

Trex can query a remote Postgres as if it were a local catalog. This walkthrough
attaches a Postgres database, runs a federated query that joins remote and
local data, and shows how to push filters down to the remote source.

It assumes you've completed [Quickstart: Deploy](deploy) and have a `psql`
session against the pgwire endpoint:

```bash
psql -h localhost -p 5433 -U trex -d main
```

You also need a reachable Postgres database. The default Trex stack ships one
on host port `65433` — we'll use it as the remote source.

## 1. Attach the remote Postgres

```sql
ATTACH 'postgresql://postgres:mypass@host.docker.internal:65433/testdb'
  AS pg (TYPE postgres);
```

`host.docker.internal` resolves the host machine from inside the Trex
container; substitute the actual hostname/IP for production setups.

The catalog `pg` now exists alongside Trex's local `memory` catalog. List
schemas:

```sql
SELECT * FROM information_schema.schemata WHERE catalog_name = 'pg';
```

## 2. Run a federated query

Create a small local table for the join:

```sql
CREATE TABLE memory.main.regions (
  region_id INT PRIMARY KEY,
  region_name TEXT
);

INSERT INTO memory.main.regions VALUES
  (1, 'EMEA'), (2, 'APAC'), (3, 'NAM');
```

Now join it with a Postgres-side table (assume `pg.public.users` has a
`region_id` column):

```sql
SELECT u.email, r.region_name, COUNT(*) AS n
  FROM pg.public.users u
  JOIN memory.main.regions r USING (region_id)
 GROUP BY u.email, r.region_name
 ORDER BY n DESC
 LIMIT 10;
```

The Postgres scan happens on the remote side; the join and aggregation run
in the engine.

## 3. Verify pushdown

Trex's planner pushes `WHERE` filters and `SELECT` projections into the
remote scan when it can. Inspect the plan:

```sql
EXPLAIN
SELECT email FROM pg.public.users WHERE region_id = 1;
```

The plan should show a `POSTGRES_SCAN` node with `Filters: region_id = 1` and
`Projections: email` — confirming the filter and projection were sent to
Postgres rather than streamed in full.

## 4. Hide credentials with the database registry

For production, you don't want connection strings in SQL. Register the
database with the management API:

```bash
TOKEN=trex_…   # an admin API key

curl -X POST http://localhost:8001/trex/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc":"2.0","id":1,
    "method":"tools/call",
    "params":{
      "name":"database-create",
      "arguments":{
        "name":"prod-pg",
        "type":"postgres",
        "host":"prod-db.internal",
        "port":5432,
        "database":"app",
        "user":"readonly"
      }
    }
  }'

# Then store credentials separately:
curl -X POST http://localhost:8001/trex/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{
    "name":"database-credential-save",
    "arguments":{"databaseId":"prod-pg","password":"…"}
  }}'
```

`pgwire` reads credentials from this registry at attach time, so subsequent
sessions can `ATTACH 'prod-pg' (TYPE postgres)` without inlining secrets.

## What just happened

Trex used DuckDB's Postgres scanner extension (loaded automatically) to
register `pg` as a remote catalog. Every reference to `pg.<schema>.<table>`
issues a `SELECT` against the remote Postgres at execution time. The pgwire
session that ran your `ATTACH` was promoted to a persistent session (see
[Concepts → Connection Pool](../concepts/connection-pool)), so the attach
survives across statements.

For a deeper explanation of what gets pushed down vs. streamed, see
[Concepts → Query Pipeline → Federation](../concepts/query-pipeline).

## Next steps

- **Other sources**: the same `ATTACH ... TYPE <kind>` pattern works for
  MySQL, SQLite, BigQuery, ClickHouse ([SQL Reference → chdb](../sql-reference/chdb)),
  SAP HANA ([SQL Reference → hana](../sql-reference/hana)), and S3/HTTP.
- **CDC into Trex**: continuously replicate a Postgres into Trex with
  [SQL Reference → etl](../sql-reference/etl).
- **Distributed**: scale these federated joins across multiple nodes —
  [Quickstart: Run a distributed cluster](distributed-cluster).
