---
sidebar_position: 10
---

# atlas — OHDSI Cohort SQL

The `atlas` extension renders [OHDSI Atlas](https://www.ohdsi.org/atlas/)
cohort definitions into executable SQL. It wraps the
[Circe-BE](https://github.com/OHDSI/circe-be) Java library (compiled via
GraalVM to a native image) and exposes the core operations as DuckDB
functions: render JSON to SQL, translate dialects, and run the two as one
step.

Use it when you have OMOP CDM data in Trex and want to evaluate cohort
definitions exported from Atlas. Render the cohort once, translate to your
target dialect, and run the resulting SQL against the CDM tables. The
resulting SQL inserts rows into a results table with each cohort entry's
`subject_id`, `cohort_start_date`, and `cohort_end_date`.

## Concepts

### Cohort definition JSON

OHDSI Atlas cohort definitions are a structured JSON describing inclusion
criteria, observation windows, exit rules, etc. Atlas itself produces this
JSON; the `atlas` extension takes that JSON and emits SQL that, when
executed, populates the standard OMOP `cohort` results table.

### Render options

The render-options JSON (second arg to `*_render*` functions) uses
camelCase keys:

```json
{
  "cdmSchema": "cdm",
  "vocabularySchema": "cdm",
  "resultSchema": "results",
  "targetTable": "cohort",
  "cohortId": 1,
  "generateStats": false
}
```

`targetTable` defaults to `cohort` — the standard OMOP results table.
`cohortId` distinguishes multiple cohorts that share one results
table. `generateStats` controls whether Circe emits the optional
inclusion-rule statistics SQL alongside the cohort INSERT.

### Base64-encoded cohort JSON

`trex_atlas_json_to_sql` and `trex_atlas_check_cohort` accept the cohort
JSON only in **base64-encoded** form. Either encode client-side
(`base64 -w0 cohort.json`) or wrap the value in
`encode(<json>::bytea, 'base64')` at the SQL level. Passing raw JSON
will fail.

## Cohort rendering workflow

The correct composition for turning a cohort JSON into executable
dialect-specific SQL is `json_to_sql` → `sql_translate`:

```sql
SELECT trex_atlas_sql_translate(
  trex_atlas_json_to_sql(
    encode(cohort_def::bytea, 'base64'),
    '{
      "cdmSchema": "cdm",
      "vocabularySchema": "cdm",
      "resultSchema": "results",
      "targetTable": "cohort",
      "cohortId": 1,
      "generateStats": false
    }'
  ),
  'postgresql'
) AS rendered_sql
FROM (SELECT readfile('/data/cohort.json') AS cohort_def) t;
```

`trex_atlas_sql_render_translate` is **not** the cohort shortcut for
this — it's a SqlRender helper for already-templated SQL strings (see
its entry below).

### SQL dialects

Atlas / OHDSI tools generate SQL using SqlRender's annotation syntax.
`atlas` translates this to a target dialect at the SQL level — supported
dialects include `postgresql`, `sql server`, `oracle`, `redshift`,
`bigquery`, `spark`, `impala`, `netezza`, `snowflake`. For Trex's
analytical engine, use `postgresql` (it's the closest dialect Trex
understands).

## Typical workflow

```sql
-- 1. Validate the cohort JSON (base64-encoded)
SELECT trex_atlas_check_cohort('<base64-cohort-json>');

-- 2. Render the cohort JSON to annotated SqlRender SQL, then translate
--    to the target dialect.
SELECT trex_atlas_sql_translate(
  trex_atlas_json_to_sql(
    '<base64-cohort-json>',
    '{"cdmSchema":"cdm","resultSchema":"results","cohortId":1}'
  ),
  'postgresql'
);

-- 3. Execute the returned SQL against your CDM
--    (typically copy/paste into a transform plugin or directly run via SQL)
```

## Functions

### `trex_atlas_check_cohort(base64_cohort_json)`

Validate a cohort JSON definition without rendering. The cohort JSON
must be **base64-encoded** (the same encoding `trex_atlas_json_to_sql`
expects). Returns a JSON result with `valid: true/false` and an error
message if invalid. Run this before render to surface bad inputs early.

```sql
-- Encode at the SQL level
SELECT trex_atlas_check_cohort(
  encode(readfile('/data/cohort.json')::bytea, 'base64')
);

-- Or pass an already-base64'd string
SELECT trex_atlas_check_cohort('<base64-cohort-json>');
```

`circe_check_cohort` is a deprecated alias.

### `trex_atlas_json_to_sql(base64_definition, options)`

Render a cohort JSON to SQL using OHDSI SqlRender annotation syntax. The
first argument must be **base64-encoded** cohort JSON. The output is
*not* yet dialect-specific — feed it to `trex_atlas_sql_translate`.

The `options` JSON uses camelCase keys: `cdmSchema`, `vocabularySchema`,
`resultSchema`, `targetTable`, `cohortId`, `generateStats`.

```sql
SELECT trex_atlas_json_to_sql(
  '<base64-cohort-json>',
  '{
    "cdmSchema": "cdm",
    "vocabularySchema": "cdm",
    "resultSchema": "results",
    "targetTable": "cohort",
    "cohortId": 1,
    "generateStats": false
  }'
);
```

`circe_json_to_sql` is a deprecated alias.

### `trex_atlas_sql_render(cohort_definition, render_options)`

Same as `json_to_sql` but accepts the structured render-options block.
Prefer this when you have non-default schemas/tables. The cohort JSON
must be base64-encoded; the render-options JSON uses the camelCase keys
documented in [Render options](#render-options): `cdmSchema`,
`vocabularySchema`, `resultSchema`, `targetTable`, `cohortId`,
`generateStats`.

```sql
SELECT trex_atlas_sql_render(
  '<base64-cohort-json>',
  '{
    "cdmSchema": "cdm",
    "vocabularySchema": "cdm",
    "resultSchema": "results",
    "targetTable": "cohort",
    "cohortId": 1
  }'
);
```

### `trex_atlas_sql_translate(sql, dialect)`

Translate annotated SQL into a specific database dialect. Use after
`json_to_sql` / `sql_render`, or directly on hand-written annotated SQL.

| Dialect | Notes |
|---------|-------|
| `postgresql` | Closest match for Trex's engine. |
| `sql server` | T-SQL. |
| `oracle` | Oracle-specific syntax. |
| `bigquery` | Google BigQuery. |
| `spark` | Spark SQL. |
| Others | `redshift`, `impala`, `netezza`, `snowflake`. |

```sql
SELECT trex_atlas_sql_translate(
  'SELECT @cdm_schema.person.* FROM @cdm_schema.person',
  'postgresql'
);
```

### `trex_atlas_sql_render_translate(sql_template, target_dialect, parameters_json)`

A **SqlRender helper** — renders an annotated SQL template by
substituting the parameters in `parameters_json`, then translates the
result to `target_dialect`. This is the OHDSI SqlRender
`render(...)` + `translate(...)` two-step exposed as a single function.

It does **not** take a cohort JSON. For cohort rendering, use
`trex_atlas_json_to_sql` followed by `trex_atlas_sql_translate` — see
[Cohort rendering workflow](#cohort-rendering-workflow).

```sql
SELECT trex_atlas_sql_render_translate(
  'SELECT * FROM @cdm_schema.person WHERE person_id = @person_id',
  'postgresql',
  '{"cdm_schema":"cdm","person_id":42}'
);
```

:::tip
Don't reach for this function to render a cohort. The cohort path is
`trex_atlas_sql_translate(trex_atlas_json_to_sql(<base64-cohort>, <opts>), <dialect>)`.
:::

`circe_sql_render_translate` is a deprecated alias.

### `trex_atlas_generate_and_translate(specification, target_dialect)`

Variant accepting a wrapping "specification" object (used by some Atlas
exports that bundle cohorts with descriptive metadata). Returns translated
SQL ready to execute.

```sql
SELECT trex_atlas_generate_and_translate(
  '<atlas-specification-json>',
  'postgresql'
);
```

### `trex_atlas_hello(name)` and `trex_atlas_openssl_version(version_type)`

Diagnostic functions: `hello` confirms the extension loaded; `openssl_version`
reports the linked OpenSSL build (relevant for FIPS / OpenSSL 3.x compliance
tracking).

```sql
SELECT trex_atlas_hello('world');
SELECT trex_atlas_openssl_version('full');
```

## Operational notes

- **Native image, not JVM.** The Circe library is compiled to a GraalVM
  native image — there is no JVM startup cost on extension load, but you
  also can't drop in alternative Atlas tools without recompiling.
- **The output is SQL, not a result.** These functions don't run the
  rendered SQL against your CDM — that's your job. Pipe the output through
  `trex_etl_*`, a transform plugin, or run it directly via the engine.
- **Schema substitution is naive.** Render-options keys (`cdmSchema`,
  `resultSchema`, `vocabularySchema`) are token-replaced into the rendered
  SQL. Make sure your schema names are valid SQL identifiers.
- **Pair with `cql2elm`** for measure evaluation: Atlas defines populations
  in cohort JSON; CQL defines clinical logic over those populations.

## Next steps

- [SQL Reference → cql2elm](cql2elm) — Clinical Quality Language → ELM,
  paired with FHIR for measure evaluation.
- [SQL Reference → fhir](fhir) — the FHIR server. Cohorts identified in
  Atlas can be pulled out via FHIR `$everything` operations.
- The OHDSI [Atlas docs](https://www.ohdsi.org/atlas/) for the cohort
  definition format.
