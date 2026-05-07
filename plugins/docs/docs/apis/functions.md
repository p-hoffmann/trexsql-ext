---
sidebar_position: 4
---

# Edge Functions & Management API

Trex bundles the Supabase Edge Runtime for running Deno-based edge functions and
implements the Supabase management API surface so the standard `supabase` CLI can
deploy / manage them.

## Endpoints

### Invocation

| Pattern | Description |
|---------|-------------|
| `${BASE_PATH}/functions/v1/:slug` | Invoke a built-in function by slug (any HTTP method). |
| `${BASE_PATH}/functions/v1/:slug/*` | Subpath under the same worker. |
| `${BASE_PATH}/plugins${scopePrefix}/:source/*` | Invoke a function registered by a plugin. |

`BASE_PATH` defaults to `/trex`. `scopePrefix` is the plugin scope (e.g. `/@trex`)
and is omitted for non-scoped plugins.

The auth context middleware injects `x-user-id` and `x-user-role` headers into the
worker request based on the session JWT.

### Management API (Supabase CLI compatible)

These mirror Supabase's REST API so `supabase login`, `supabase functions deploy`,
`supabase secrets set`, `supabase gen types`, and `supabase config push` work
against a Trex deployment. Every endpoint requires admin auth: a Bearer JWT with
`trex_role = "admin"`, an `sbp_…` personal access token, or a `trex_…` API key.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/organizations` | Returns `[{ id: "trex-org", name: "trex" }]`. |
| GET | `/v1/projects` | Returns a single synthetic project (used by `supabase login` to validate tokens). |
| GET | `/v1/projects/:ref` | Project metadata with the resolved DB host (`EXTERNAL_DB_URL` → `POOLER_URL` → `DATABASE_URL`). |
| GET | `/v1/projects/:ref/api-keys?reveal=true` | Anon and service-role keys (`reveal=true` returns plaintext for admins). |
| POST | `/v1/projects/:ref/cli/login-role` | Allocates a temporary `cli_login_*` Postgres role with login privilege, valid for 1 hour. |
| GET / PATCH / PUT | `/v1/projects/:ref/config/auth` | GoTrue config. PATCH/PUT updates persist into `trex.setting`. |
| GET / PATCH / PUT | `/v1/projects/:ref/config/database/postgres` | Postgres tunables (advisory; `trex.setting` only). |
| GET / PATCH / PUT | `/v1/projects/:ref/config/storage` | Storage config (file-size limit, image-transformation flag). |
| GET | `/v1/projects/:ref/config/database/pooler` | Connection pool / pooler info derived from env DB URLs. |
| GET / PATCH / PUT | `/v1/projects/:ref/postgrest` | PostgREST config. |
| GET | `/v1/projects/:ref/network-restrictions` | Stub (returns `0.0.0.0/0`). |
| GET | `/v1/projects/:ref/ssl-enforcement` | Stub. |
| GET | `/v1/projects/:ref/billing/addons` | Empty addon list (required by `supabase config push`). |

#### Functions

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/projects/:ref/functions` | List all functions discovered under `FUNCTIONS_DIR` (default `./functions`). |
| GET | `/v1/projects/:ref/functions/:slug` | Function metadata. `entrypoint_path` is rewritten to a `file:///` URL for CLI compatibility. |
| GET | `/v1/projects/:ref/functions/:slug/body` | Returns the ESZIP bundle (`esbuild.esz`, optionally `EZBR`-prefixed Brotli) if present, otherwise the entrypoint source. |
| POST | `/v1/projects/:ref/functions?slug=…&entrypoint_path=…&verify_jwt=…` | Deploy. Body is the raw ESZIP bundle. Stored at `FUNCTIONS_DIR/:slug/esbuild.esz`. Bumps `version`. |
| POST | `/v1/projects/:ref/functions/deploy` | Legacy JSON deploy: `{ slug, name?, body, verify_jwt?, entrypoint_path?, import_map? }`. |
| DELETE | `/v1/projects/:ref/functions/:slug` | Remove the function directory. |

#### Secrets

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/projects/:ref/secrets` | List secrets (name + hash, no plaintext). `SUPABASE_*` names are filtered out. |
| POST | `/v1/projects/:ref/secrets` | Body: `[{ name, value }, …]`. Values are stored encrypted in `trex.secret`. |
| DELETE | `/v1/projects/:ref/secrets` | Body: `[name, …]`. |

Secrets are decrypted and injected as env vars for every edge-function worker. The
server caches decrypted secrets for 30 seconds; mutations invalidate the cache.

#### Types

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/projects/:ref/types/typescript?included_schemas=public,trex` | Generate Supabase-style `Database` TypeScript types from `information_schema`. Used by `supabase gen types typescript`. |

## Function Metadata (`function.json`)

Each function directory may contain a `function.json`:

```json
{
  "slug": "hello-world",
  "name": "hello-world",
  "status": "ACTIVE",
  "version": 3,
  "verify_jwt": true,
  "entrypoint_path": "index.ts",
  "import_map_path": null,
  "created_at": 1730000000,
  "updated_at": 1730500000
}
```

If a function directory has only an `index.ts` and no `function.json`, defaults are
synthesized at read time (status `ACTIVE`, version `1`, `verify_jwt: true`).

## Writing a Function

Workers use the standard Supabase Edge Runtime Fetch API:

```typescript
Deno.serve(async (req: Request) => {
  if (req.method === "GET") {
    return Response.json({ status: "ok" });
  }
  return new Response("Method not allowed", { status: 405 });
});
```

### Auth Context

Every invocation receives:

| Header | Source |
|--------|--------|
| `x-user-id` | `app.user_id` from `pgSettings` |
| `x-user-role` | `app.user_role` from `pgSettings` |
| `Authorization` | Forwarded from the inbound request |

`accept-encoding` is stripped before the worker runs to avoid double-encoded
responses.

### Environment Injection

Workers receive:

- All decrypted entries from `trex.secret` (refreshed every 30s).
- The plugin's `_shared` env block (with `${VAR}`/`${VAR:-default}` substitution).
- The plugin's per-`NODE_ENV` env block (e.g. `production`).
- `TREX_FUNCTION_PATH` — absolute path to the plugin directory.

## CLI Login Flow

`POST /api/cli/sessions` (admin only) and `GET /platform/cli/login/:session_id`
implement an ECDH-sealed device-code login used by `supabase login` against a Trex
instance.

```
CLI                        Browser / Web UI               Server
 │                             │                             │
 │  generate ECDH P-256 ──────▶│                             │
 │  open browser w/ pubkey     │                             │
 │                             │  POST /api/cli/sessions ───▶│
 │                             │  { session_id, public_key,  │
 │                             │    token_name }             │
 │                             │                             │  generate sbp_ key
 │                             │                             │  ECDH → AES-GCM
 │                             │  ◀────── { device_code } ───│  store encrypted
 │  GET /platform/cli/login/   │                             │
 │     :session_id?device_code=│ ───────────────────────────▶│
 │  ◀───────────────────────── { encrypted_access_token,     │
 │                              public_key, nonce } ─────────│
 │  derive shared key, decrypt │                             │
```

Sessions live in memory for 5 minutes and are deleted after a single successful
retrieval.

## Built-in Functions

Built-in functions live in `FUNCTIONS_DIR` (default `./functions`) outside any
plugin. They use the same metadata, deployment endpoints, and runtime as plugin
functions, and are invoked at `${BASE_PATH}/functions/v1/:slug`.
