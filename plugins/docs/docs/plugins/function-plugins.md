---
sidebar_position: 4
---

# Function Plugins

Function plugins register HTTP API endpoints powered by Deno EdgeRuntime workers.
Each function runs in an isolated worker with configurable permissions, secrets
injection, and optional ESZIP bundles.

## Configuration

```json
{
  "trex": {
    "functions": {
      "env": {
        "_shared": {
          "DATABASE_URL": "${DATABASE_URL}",
          "API_KEY": "${MY_API_KEY:-}"
        },
        "production": {
          "FEATURE_FLAG": "on"
        }
      },
      "roles": {
        "my-plugin-admin": ["my-plugin:read", "my-plugin:write"],
        "my-plugin-viewer": ["my-plugin:read"]
      },
      "scopes": [
        { "path": "/plugins/my-plugin/admin/*", "scopes": ["my-plugin:write"] },
        { "path": "/plugins/my-plugin/*", "scopes": ["my-plugin:read"] }
      ],
      "api": [
        {
          "source": "/my-plugin",
          "function": "/functions",
          "env": "production",
          "imports": "/functions/import_map.json",
          "eszip": "/dist/bundle.eszip",
          "allowHostFsAccess": false,
          "permissions": { "net": ["api.example.com"] }
        }
      ],
      "init": [
        {
          "function": "/functions/setup.ts",
          "waitfor": "http://localhost:5432",
          "delay": 1000
        }
      ]
    }
  }
}
```

## API Routes

Each entry in `api` registers an Express handler at
`${PLUGINS_BASE_PATH}{scopePrefix}{source}/*` (default
`/plugins/<scope>/<source>/*`):

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | URL path. Mounted under `${PLUGINS_BASE_PATH}` plus the plugin's scope prefix. |
| `function` | string | Path to the worker **directory** (containing `index.ts`), relative to the plugin directory. The Deno EdgeRuntime resolves the entrypoint inside the directory. Pointing at a specific `.ts` file fails with `could not find an appropriate entrypoint`. |
| `env` | string | Selects the per-environment env block to merge with `_shared`. Typically `production`, `development`, or omitted. |
| `imports` | string | Path to a Deno import map. Absolute paths and URLs are passed through; relative paths are resolved against the plugin directory. |
| `eszip` | string | Path to a prebuilt ESZIP bundle (e.g. produced by `deno bundle` / `esbuild` + `eszip`). When set, the worker loads from the bundle instead of source. |
| `allowHostFsAccess` | bool | When `true`, the worker may read/write the host filesystem outside its sandbox. Default `false`. |
| `permissions` | object | Deno permissions object passed straight to the worker (e.g. `{ net: [...], read: [...] }`). |

All HTTP methods (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`) route to the worker
under the registered path.

## Init Hooks

`functions.init[]` runs one-shot setup workers at server startup:

| Field | Description |
|-------|-------------|
| `function` | Path to the init script. |
| `env` | Environment block to merge. |
| `imports` | Import map path (same rules as `api[].imports`). |
| `eszip` | Optional ESZIP bundle for the init worker. |
| `waitfor` | URL to poll until reachable before running. |
| `waitforEnvVar` | Name of an env var whose value to use as `waitfor` URL. |
| `delay` | Milliseconds to wait after the worker exits. |

Init workers run sequentially in declaration order.

## Roles & Scopes

Plugins ship their own authorization model. The loader merges plugin roles into
the global `ROLE_SCOPES` map and prepends `scopes[].path` patterns into the
URL-scope check list.

- Roles are auto-created in `trex.role` at startup (via `ensureRolesExist`).
- Admin users bypass every scope check.
- Non-admin callers must hold a role whose scope set covers all scopes required
  by the matched URL pattern.
- The first matching path pattern wins. Order entries from most specific to least
  specific.

## Environment Variables

Workers receive a merged env map composed of:

1. `_shared` from the plugin config (with `${VAR}` substitution).
2. The block named by the `api[].env` field, if any.
3. All decrypted secrets from `trex.secret` (refreshed every 30s).
4. `TREX_FUNCTION_PATH` — absolute path to the plugin directory.

Substitution syntax (in the plugin config — not at runtime in the worker):

| Pattern | Behavior |
|---------|----------|
| `${VAR}` | Value of env var (empty string if unset). |
| `${VAR:-default}` | Value or default if unset/empty. |
| `${VAR-default}` | Value or default if unset. |
| `${VAR:?error}` | Throw if unset/empty. |
| `${VAR:+alternate}` | Alternate if set and non-empty. |

## Auth Context

The Express middleware injects auth metadata into every plugin request before it
reaches the worker:

| Header | Source |
|--------|--------|
| `x-user-id` | `pgSettings["app.user_id"]` |
| `x-user-role` | `pgSettings["app.user_role"]` |

The original `Authorization` header is forwarded. `accept-encoding` is stripped
to avoid double-encoded responses.

## Worker Limits

Each worker is bounded by:

- 1000 MB memory limit.
- 30-minute wall clock timeout per request.
- 1 000 000 ms CPU soft limit / 2 000 000 ms hard limit.

Limits are not currently configurable per-plugin.
