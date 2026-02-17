---
sidebar_position: 8
---

# migration — Schema Migrations

The `migration` extension provides SQL schema migration management with version tracking, checksum verification, and multi-database support (trexsql and PostgreSQL).

## Functions

### `trex_migration_run(path)`

Discover and execute pending migrations from a directory. Migrations are SQL files named with a numeric version prefix (e.g., `001_create_tables.sql`).

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Path to migrations directory |

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| version | INTEGER | Migration version number |
| name | VARCHAR | Migration file name |
| status | VARCHAR | Execution status |

```sql
SELECT * FROM trex_migration_run('./migrations');
```

### `trex_migration_status(path)`

Show status of all discovered migrations (applied, pending, or checksum mismatch).

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Path to migrations directory |

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| version | INTEGER | Migration version number |
| name | VARCHAR | Migration file name |
| status | VARCHAR | applied, pending, or checksum_mismatch |
| applied_on | VARCHAR | Application timestamp |
| checksum | VARCHAR | File checksum |

```sql
SELECT * FROM trex_migration_status('./migrations');
```

### `trex_migration_run_schema(path, schema, database)`

Run migrations in a specific schema and database. Supports both trexsql and PostgreSQL databases.

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Path to migrations directory |
| schema | VARCHAR | Target schema name |
| database | VARCHAR | Target database name |

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| version | INTEGER | Migration version number |
| name | VARCHAR | Migration file name |
| status | VARCHAR | Execution status |

```sql
SELECT * FROM trex_migration_run_schema('./migrations', 'my_schema', 'my_database');
```

### `trex_migration_status_schema(path, schema, database)`

Show migration status in a specific schema and database.

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Path to migrations directory |
| schema | VARCHAR | Target schema name |
| database | VARCHAR | Target database name |

**Returns:** TABLE

| Column | Type | Description |
|--------|------|-------------|
| version | INTEGER | Migration version number |
| name | VARCHAR | Migration file name |
| status | VARCHAR | applied, pending, or checksum_mismatch |
| applied_on | VARCHAR | Application timestamp |
| checksum | VARCHAR | File checksum |

```sql
SELECT * FROM trex_migration_status_schema('./migrations', 'my_schema', 'my_database');
```
