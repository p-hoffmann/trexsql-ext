// @ts-ignore
import { STATUS_CODE } from "https://deno.land/std/http/status.ts";
import { join } from "jsr:@std/path@^1.0";
import express from "express";
import { createServer, request as httpRequest } from "node:http";
import { grafserv } from "postgraphile/grafserv/express/v4";
import cors from "cors";
import { BASE_PATH } from "./config.ts";
import { pool } from "./db.ts";
import { authRouter } from "./auth/auth-router.ts";
import { ensureAuthKeys } from "./auth/api-keys.ts";
import { verifyAccessToken } from "./auth/jwt.ts";
import { createPostGraphile } from "./postgraphile.ts";
import { authContext } from "./middleware/auth-context.ts";
import { Plugins } from "./plugin/plugin.ts";
import { addPluginRoutes } from "./routes/plugin.ts";
import { functionsRouter } from "./routes/functions.ts";
import { cliLoginRouter } from "./routes/cli-login.ts";
import { fnmap } from "./plugin/function.ts";

console.log("main function started");
console.log(Deno.version);

addEventListener("beforeunload", () => {
  console.log("main worker exiting");
});

addEventListener("unhandledrejection", (ev) => {
  console.log(ev);
  ev.preventDefault();
});

const app = express();
const server = createServer(app);

const trustedOrigins = (Deno.env.get("BETTER_AUTH_TRUSTED_ORIGINS") || "").split(",").filter(Boolean);
app.use(cors({
  origin: trustedOrigins.length > 0 ? trustedOrigins : false,
  credentials: true,
}));

// Public settings endpoint — no auth required, only whitelisted keys
const PUBLIC_SETTING_KEYS = ["auth.selfRegistration", "auth.anonKey"];

app.get(`${BASE_PATH}/api/settings/public`, async (_req, res) => {
  try {
    const result = await pool.query(
      `SELECT key, value FROM trex.setting WHERE key = ANY($1)`,
      [PUBLIC_SETTING_KEYS]
    );
    const settings: Record<string, any> = {};
    for (const row of result.rows) {
      settings[row.key] = row.value;
    }
    res.json(settings);
  } catch {
    // Table may not exist yet — return safe defaults
    res.json({ "auth.selfRegistration": false });
  }
});

// Mount GoTrue-compatible auth router
app.use(`${BASE_PATH}/auth/v1`, authRouter);

// Deno doesn't have `global` — polyfill for npm packages that expect Node.js
if (typeof (globalThis as any).global === "undefined") {
  (globalThis as any).global = globalThis;
}

// MCP server (before authContext — uses its own API key auth)
try {
  const { mountMcpServer } = await import("./mcp/index.ts");
  mountMcpServer(app);
  console.log(`MCP server mounted on ${BASE_PATH}/mcp`);
} catch (err) {
  console.error("MCP server failed to initialize:", err);
}

// Helper: extract user from Bearer token (for session-based admin endpoints)
async function getAuthUser(req: any): Promise<{ id: string; role: string } | null> {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) return null;
  const token = authHeader.slice(7);
  const claims = await verifyAccessToken(token);
  if (!claims) return null;
  return { id: claims.sub, role: claims.app_metadata?.trex_role || "user" };
}

// API key management endpoint (Bearer-token authenticated)
app.post(`${BASE_PATH}/api/api-keys`, express.json(), async (req, res) => {
  try {
    const user = await getAuthUser(req);
    if (!user || user.role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    const { generateApiKey } = await import("./mcp/auth.ts");
    const { name, expiresAt } = req.body || {};
    if (!name) {
      res.status(400).json({ error: "name is required" });
      return;
    }
    let expiresDate: Date | undefined;
    if (expiresAt) {
      expiresDate = new Date(expiresAt);
      if (isNaN(expiresDate.getTime())) {
        res.status(400).json({ error: "Invalid expiresAt date" });
        return;
      }
    }
    const result = await generateApiKey(
      user.id,
      name,
      expiresDate,
    );
    res.json(result);
  } catch (err) {
    console.error("API key creation error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get(`${BASE_PATH}/api/api-keys`, async (req, res) => {
  try {
    const user = await getAuthUser(req);
    if (!user || user.role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    const result = await pool.query(
      `SELECT id, name, key_prefix, "lastUsedAt", "expiresAt", "revokedAt", "createdAt" FROM trex.api_key WHERE "userId" = $1 ORDER BY "createdAt" DESC`,
      [user.id]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("API key list error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete(`${BASE_PATH}/api/api-keys/:id`, async (req, res) => {
  try {
    const user = await getAuthUser(req);
    if (!user || user.role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    const result = await pool.query(
      `UPDATE trex.api_key SET "revokedAt" = NOW() WHERE id = $1 AND "userId" = $2 AND "revokedAt" IS NULL RETURNING id`,
      [req.params.id, user.id]
    );
    if (result.rows.length === 0) {
      res.status(404).json({ error: "Key not found or already revoked" });
      return;
    }
    res.json({ ok: true });
  } catch (err) {
    console.error("API key revoke error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Generate Supabase CLI compatible access token (sbp_ format)
app.post(`${BASE_PATH}/api/cli-token`, express.json(), async (req, res) => {
  try {
    const user = await getAuthUser(req);
    if (!user || user.role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    const { generateApiKey } = await import("./mcp/auth.ts");
    const name = req.body?.name || "supabase-cli";
    const result = await generateApiKey(user.id, name, undefined, "sbp_");
    res.json({ access_token: result.key });
  } catch (err) {
    console.error("CLI token creation error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Dynamic plugin registration — register functions from a given directory.
// Internal endpoint: called by devx edge function to register D2E app functions.
// Security: only allows paths within known workspace directories.
app.post(`${BASE_PATH}/api/plugins/register`, express.json(), async (req, res) => {
  try {
    const { path: dirPath } = req.body || {};
    if (!dirPath || typeof dirPath !== "string") {
      res.status(400).json({ error: "path is required" });
      return;
    }
    // Only allow registration from devx workspace directories
    const allowedPrefixes = [
      Deno.env.get("DEVX_WORKSPACE_DIR") || "/tmp/devx-workspaces",
      "/var/devx-workspaces",
    ];
    const normalized = dirPath.replace(/\/+$/, "");
    if (!allowedPrefixes.some((p) => normalized.startsWith(p + "/"))) {
      res.status(403).json({ error: "Path not in allowed workspace directory" });
      return;
    }
    const result = await Plugins.registerFromPath(app, normalized);
    if (result.ok) {
      res.json(result);
    } else {
      res.status(400).json(result);
    }
  } catch (err) {
    console.error("Plugin registration error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Admin-only: get auth keys
app.get(`${BASE_PATH}/api/settings/auth-keys`, async (req, res) => {
  try {
    const user = await getAuthUser(req);
    if (!user || user.role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    const result = await pool.query(
      `SELECT key, value FROM trex.setting WHERE key IN ('auth.anonKey', 'auth.serviceRoleKey')`,
    );
    const keys: Record<string, string> = {};
    for (const row of result.rows) {
      keys[row.key] = typeof row.value === "string" ? row.value : JSON.parse(row.value);
    }
    res.json(keys);
  } catch (err) {
    console.error("Auth keys error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// PostgREST proxy — before authContext since PostgREST handles its own JWT verification
const POSTGREST_HOST = Deno.env.get("POSTGREST_HOST") || "postgrest";
const POSTGREST_PORT = Deno.env.get("POSTGREST_PORT") || "3000";

app.all(`${BASE_PATH}/rest/v1/*`, (req, res) => {
  const targetPath = req.originalUrl.replace(`${BASE_PATH}/rest/v1`, "") || "/";

  // Build headers to forward
  const headers: Record<string, string> = {};
  const forwardHeaders = [
    "authorization", "apikey", "prefer", "range", "content-type",
    "accept", "content-profile", "accept-profile", "x-client-info",
  ];
  for (const h of forwardHeaders) {
    if (req.headers[h]) {
      headers[h] = Array.isArray(req.headers[h]) ? req.headers[h].join(", ") : req.headers[h] as string;
    }
  }

  // supabase-js sends apikey header + Authorization header.
  // If no Authorization header, use apikey as Bearer token so PostgREST can determine the role.
  if (!headers["authorization"] && headers["apikey"]) {
    headers["authorization"] = `Bearer ${headers["apikey"]}`;
  }

  const proxyReq = httpRequest(
    {
      hostname: POSTGREST_HOST,
      port: parseInt(POSTGREST_PORT),
      path: targetPath,
      method: req.method,
      headers,
    },
    (proxyRes) => {
      res.status(proxyRes.statusCode || 500);
      // Forward response headers
      const skipHeaders = new Set(["transfer-encoding"]);
      for (const [key, value] of Object.entries(proxyRes.headers)) {
        if (!skipHeaders.has(key) && value !== undefined) {
          res.setHeader(key, value);
        }
      }
      proxyRes.pipe(res);
    },
  );

  proxyReq.on("error", (err) => {
    console.error("[postgrest-proxy] Error:", err.message);
    if (!res.headersSent) {
      res.status(502).json({ error: "PostgREST unavailable", details: err.message });
    }
  });

  // Pipe request body
  req.pipe(proxyReq);
});

// Supabase CLI subdomain routing — the CLI hits https://{ref}.trex.local/storage/v1/...
// without the BASE_PATH prefix. Rewrite to include the prefix so routes match.
if (BASE_PATH && BASE_PATH !== "/") {
  const supabasePaths = ["/storage/v1/", "/auth/v1/", "/rest/v1/", "/functions/v1/"];
  app.use((req, _res, next) => {
    if (!req.url.startsWith(BASE_PATH) && supabasePaths.some((p) => req.url.startsWith(p))) {
      req.url = `${BASE_PATH}${req.url}`;
      req.originalUrl = req.url;
    }
    next();
  });
}

// CLI login polling endpoint — no auth required (before authContext)
app.use(cliLoginRouter);

app.use(authContext);

try {
  await Plugins.initPlugins(app);
  addPluginRoutes(app);
  console.log("Plugin system initialized");
} catch (err) {
  console.error("Plugin system failed to initialize:", err);
}

// Supabase-compatible /storage/v1/* route — calls storage worker directly.
// Bypasses pluginAuthz because Supabase Storage handles its own JWT auth
// (required for public bucket access without a Bearer token).
// Use express.raw() to capture the raw body before any middleware consumes it.
app.all(`${BASE_PATH}/storage/v1/*`, express.raw({ type: "*/*", limit: "50mb" }), async (req, res) => {
  const handler = fnmap["@trex/storage/supabase-storage/functions"];
  if (!handler) {
    res.status(503).json({ error: "Storage plugin not loaded" });
    return;
  }
  try {
    const host = req.get("host") || "localhost";
    const protocol = req.protocol || "http";
    // Rewrite /trex/storage/v1/... to /storage-api/...
    const storagePath = req.originalUrl.replace(`${BASE_PATH}/storage/v1`, "/storage-api");
    const requestUrl = `${protocol}://${host}${storagePath}`;

    const headers = new Headers();
    for (const [key, val] of Object.entries(req.headers)) {
      if (val) {
        const lower = key.toLowerCase();
        if (lower === "accept-encoding" || lower === "content-length") continue;
        headers.set(key, Array.isArray(val) ? val.join(", ") : String(val));
      }
    }

    let body: Blob | undefined;
    if (req.method !== "GET" && req.method !== "HEAD") {
      if (req.body && req.body.length > 0) {
        body = new Blob([req.body]);
      } else if (storagePath.startsWith("/storage-api/object/list/")) {
        // Supabase CLI sends POST to /object/list/ with empty body — inject defaults
        body = new Blob([JSON.stringify({ prefix: "", limit: 100, offset: 0 })], { type: "application/json" });
        headers.set("content-type", "application/json");
      } else if (headers.get("content-type")?.includes("application/json")) {
        body = new Blob(["{}"], { type: "application/json" });
      }
    }

    const webReq = new globalThis.Request(requestUrl, { method: req.method, headers, body });
    const workerResponse = await handler(webReq);

    res.status(workerResponse.status);
    workerResponse.headers.forEach((value: string, key: string) => {
      const lower = key.toLowerCase();
      if (lower === "content-encoding" || lower === "content-length" || lower === "transfer-encoding") return;
      res.setHeader(key, value);
    });
    const responseBody = await workerResponse.text();
    res.send(responseBody);
  } catch (err) {
    console.error("[storage-proxy] Error:", err);
    res.status(500).json({ error: String(err) });
  }
});

// Run core schema migrations (SCHEMA_DIR) via direct PostgreSQL connection
try {
  const schemaDir = Deno.env.get("SCHEMA_DIR");
  const databaseUrl = Deno.env.get("DATABASE_URL");
  if (schemaDir && databaseUrl) {
    const { Pool } = await import("pg");
    const migrationPool = new Pool({ connectionString: databaseUrl });
    try {
      // Ensure trex schema exists
      await migrationPool.query("CREATE SCHEMA IF NOT EXISTS trex");
      await migrationPool.query(`CREATE TABLE IF NOT EXISTS trex._migrations (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ DEFAULT NOW()
      )`);

      // Read and apply migration files in order
      const files: string[] = [];
      for await (const entry of Deno.readDir(schemaDir)) {
        if (entry.isFile && entry.name.endsWith(".sql")) {
          files.push(entry.name);
        }
      }
      files.sort();

      let applied = 0;
      for (const file of files) {
        const version = file.replace(".sql", "");
        const check = await migrationPool.query(
          "SELECT 1 FROM trex._migrations WHERE version = $1",
          [version],
        );
        if (check.rows.length > 0) continue;

        const sql = await Deno.readTextFile(`${schemaDir}/${file}`);
        try {
          await migrationPool.query(sql);
          applied++;
          console.log(`Core schema: applied migration ${version}`);
        } catch (migErr: any) {
          // If migration fails with "already exists" errors, mark as applied
          // This handles the case where migrations were partially applied before tracking existed
          const code = migErr?.code;
          if (code === "42710" || code === "42P07" || code === "42P06") {
            // 42710 = duplicate object, 42P07 = duplicate table, 42P06 = duplicate schema
            console.log(`Core schema: migration ${version} already applied (objects exist)`);
          } else {
            console.error(`Core schema: migration ${version} failed:`, migErr);
            // Still mark as applied to avoid retrying broken migrations endlessly
          }
        }
        await migrationPool.query(
          "INSERT INTO trex._migrations (version) VALUES ($1) ON CONFLICT DO NOTHING",
          [version],
        );
      }
      console.log(applied > 0 ? `Core schema migrations applied (${applied})` : "Core schema migrations up to date");
    } finally {
      await migrationPool.end();
    }
  }
} catch (err) {
  console.error("Core schema migration failed:", err);
}


// Run plugin migrations after plugin discovery
try {
  const { runAllPluginMigrations } = await import("./plugin/migration.ts");
  await runAllPluginMigrations();
} catch (err) {
  console.error("Plugin migration execution failed:", err);
}

// Auto-create roles declared by plugins in the PostgreSQL role table
try {
  const { ensureRolesExist } = await import("./plugin/function.ts");
  await ensureRolesExist();
} catch (err) {
  console.error("Role auto-creation failed:", err);
}

const databaseUrl = Deno.env.get("DATABASE_URL");
if (databaseUrl) {
  try {
    const schemas = (Deno.env.get("PG_SCHEMA") || "trex").split(",");
    const pgl = createPostGraphile(databaseUrl, schemas);
    const serv = pgl.createServ(grafserv);
    await serv.addTo(app, server);
    console.log(`PostGraphile mounted on ${BASE_PATH}/graphql and ${BASE_PATH}/graphiql`);
  } catch (err) {
    console.error("PostGraphile failed to initialize:", err);
  }
} else {
  console.warn("DATABASE_URL not set — PostGraphile disabled");
}

// WebSocket upgrade handler for devx dev server proxy (Vite HMR)
server.on("upgrade", (req, socket, head) => {
  const urlPath = req.url || "";
  const proxyMatch = urlPath.match(/\/plugins\/\w+\/devx-api\/apps\/([^/]+)\/proxy(\/.*)?$/);
  if (!proxyMatch) return; // Not a devx proxy path — let other handlers (e.g. PostGraphile) handle it

  const appId = proxyMatch[1];
  const statusUrl = `http://localhost:8000/plugins/trex/devx-api/apps/${appId}/server/status`;

  const statusReq = httpRequest(statusUrl, {
    headers: { cookie: req.headers.cookie || "" },
  }, (statusRes) => {
    let data = "";
    statusRes.on("data", (chunk: string) => { data += chunk; });
    statusRes.on("end", () => {
      try {
        const status = JSON.parse(data);
        if (status.status !== "running") { socket.destroy(); return; }
        const port = status.url ? new URL(status.url).port : String(status.port);

        // Make upgrade request to the dev server
        const proxyReq = httpRequest(`http://localhost:${port}${urlPath}`, {
          method: "GET",
          headers: { ...req.headers, host: `localhost:${port}` },
        });
        proxyReq.on("upgrade", (_proxyRes, proxySocket, proxyHead) => {
          socket.write(
            "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            `Sec-WebSocket-Accept: ${_proxyRes.headers["sec-websocket-accept"]}\r\n` +
            ((_proxyRes.headers["sec-websocket-protocol"]) ? `Sec-WebSocket-Protocol: ${_proxyRes.headers["sec-websocket-protocol"]}\r\n` : "") +
            "\r\n"
          );
          if (proxyHead.length > 0) socket.write(proxyHead);
          proxySocket.pipe(socket);
          socket.pipe(proxySocket);
          proxySocket.on("error", () => socket.destroy());
          socket.on("error", () => proxySocket.destroy());
        });
        proxyReq.on("error", () => socket.destroy());
        proxyReq.end();
      } catch { socket.destroy(); }
    });
  });
  statusReq.on("error", () => socket.destroy());
  statusReq.end();
});

app.get(`${BASE_PATH}/_internal/health`, (_req, res) => {
  res.status(STATUS_CODE.OK).json({ message: "ok" });
});

app.get(`${BASE_PATH}/_internal/metric`, async (req, res) => {
  const user = await getAuthUser(req);
  if (!user || user.role !== "admin") {
    res.status(401).json({ error: "Admin authentication required" });
    return;
  }
  const metric = await EdgeRuntime.getRuntimeMetrics();
  res.json(metric);
});

app.put(`${BASE_PATH}/_internal/upload`, async (req, res) => {
  try {
    const user = await getAuthUser(req);
    if (!user || user.role !== "admin") {
      res.status(401).json({ error: "Admin authentication required" });
      return;
    }
    let body = "";
    for await (const chunk of req) {
      body += chunk;
    }
    const dir = await Deno.makeTempDir();
    const path = join(dir, "index.ts");
    await Deno.writeTextFile(path, body);
    res.json({ path: dir });
  } catch (err) {
    res.status(STATUS_CODE.BadRequest).json(err);
  }
});

// Supabase-compatible edge function invocation: /functions/v1/:function_name
const FUNCTIONS_DIR = Deno.env.get("FUNCTIONS_DIR") || "./functions";

// Cached Supabase-compatible env vars (populated after ensureAuthKeys)
let supabaseEnvVars: [string, string][] = [];

async function getSupabaseEnvVars(): Promise<[string, string][]> {
  const envVarsObj = Deno.env.toObject();
  const envVars: [string, string][] = Object.keys(envVarsObj).map((k) => [k, envVarsObj[k]]);
  // Inject Supabase-compatible vars if not already in process env
  for (const [key, value] of supabaseEnvVars) {
    if (!envVarsObj[key]) {
      envVars.push([key, value]);
    }
  }

  // Load user-defined secrets from DB
  try {
    const { loadSecretsForEnv } = await import("./routes/functions.ts");
    const secrets = await loadSecretsForEnv();
    for (const [key, value] of secrets) {
      if (!envVarsObj[key]) {
        envVars.push([key, value]);
      }
    }
  } catch { /* secrets table may not exist yet */ }

  return envVars;
}

async function invokeEdgeFunction(req: any, res: any) {
  const functionName = req.params.function_name;
  let servicePath: string;

  // Support /tmp/ paths for backward compat (runtime/bao plugins)
  if (functionName.startsWith("tmp")) {
    try {
      servicePath = await Deno.realPath(`/tmp/${functionName}`);
      if (!servicePath.startsWith("/tmp/")) {
        res.status(400).json({ error: "Invalid service path" });
        return;
      }
    } catch (err) {
      res.status(STATUS_CODE.BadRequest).json(err);
      return;
    }
  } else {
    servicePath = join(FUNCTIONS_DIR, functionName);
  }

  try {
    await Deno.stat(servicePath);
  } catch {
    res.status(404).json({ error: `Function ${functionName} not found` });
    return;
  }

  // Check verify_jwt from function metadata
  try {
    const metaPath = join(servicePath, "function.json");
    const metaContent = await Deno.readTextFile(metaPath);
    const meta = JSON.parse(metaContent);
    if (meta.verify_jwt !== false) {
      const authHeader = req.headers.authorization;
      const apikey = req.headers.apikey;
      const token = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : apikey;
      if (!token) {
        res.status(401).json({ error: "Invalid JWT" });
        return;
      }
      const claims = await verifyAccessToken(token);
      if (!claims) {
        res.status(401).json({ error: "Invalid JWT" });
        return;
      }
    }
  } catch {
    // No function.json or parse error — default to requiring auth
    const authHeader = req.headers.authorization;
    const apikey = req.headers.apikey;
    const token = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : apikey;
    if (token) {
      const claims = await verifyAccessToken(token);
      if (!claims) {
        res.status(401).json({ error: "Invalid JWT" });
        return;
      }
    }
  }

  // Check for import map
  let importMapPath: string | undefined;
  try {
    const denoJsonPath = join(servicePath, "deno.json");
    await Deno.stat(denoJsonPath);
    importMapPath = denoJsonPath;
  } catch { /* no import map */ }

  // Check for ESZIP bundle (deployed by Supabase CLI)
  // Format: EZBR magic (4 bytes) + Brotli-compressed ESZIP v2
  let maybeEszip: Uint8Array | undefined;
  let maybeEntrypoint: string | undefined;
  try {
    const eszipPath = join(servicePath, "esbuild.esz");
    const raw = await Deno.readFile(eszipPath);

    // Check for EZBR header and decompress
    const header = new TextDecoder().decode(raw.slice(0, 4));
    if (header === "EZBR") {
      const { brotliDecompressSync } = await import("node:zlib");
      maybeEszip = new Uint8Array(brotliDecompressSync(raw.slice(4)));
    } else {
      // Already raw eszip
      maybeEszip = raw;
    }

    // Read entrypoint from function.json metadata
    try {
      const metaContent = await Deno.readTextFile(join(servicePath, "function.json"));
      const meta = JSON.parse(metaContent);
      maybeEntrypoint = meta.entrypoint_path
        ? `file:///${meta.entrypoint_path}`
        : "file:///src/index.ts";
    } catch {
      maybeEntrypoint = "file:///src/index.ts";
    }
  } catch { /* no eszip bundle — use regular servicePath */ }

  const createWorker = async () => {
    const workerOpts: Record<string, unknown> = {
      servicePath,
      memoryLimitMb: 150,
      workerTimeoutMs: 5 * 60 * 1000,
      noModuleCache: false,
      envVars: await getSupabaseEnvVars(),
      forceCreate: false,
      cpuTimeSoftLimitMs: 10000,
      cpuTimeHardLimitMs: 20000,
      importMapPath,
      context: {
        useReadSyncFileAPI: true,
        unstableSloppyImports: true,
      },
    };

    // If ESZIP bundle exists, pass it to the worker
    if (maybeEszip) {
      workerOpts.maybeEszip = maybeEszip;
      workerOpts.maybeEntrypoint = maybeEntrypoint;
    }

    return await EdgeRuntime.userWorkers.create(workerOpts);
  };

  const host = req.get("host") || "localhost";
  const protocol = req.protocol || "http";
  const webUrl = `${protocol}://${host}${req.originalUrl}`;
  const webHeaders = new Headers();
  for (const [key, val] of Object.entries(req.headers)) {
    if (val) webHeaders.set(key, Array.isArray(val) ? val.join(", ") : val as string);
  }
  let reqBody: Blob | undefined;
  if (req.method !== "GET" && req.method !== "HEAD") {
    const chunks: Uint8Array[] = [];
    for await (const chunk of req) {
      chunks.push(typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk);
    }
    if (chunks.length > 0) reqBody = new Blob(chunks);
  }
  const webReq = new Request(webUrl, {
    method: req.method,
    headers: webHeaders,
    body: reqBody,
  });

  const MAX_RETRIES = 3;
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const worker = await createWorker();
      const controller = new AbortController();
      const workerResponse = await worker.fetch(webReq, { signal: controller.signal });

      res.status(workerResponse.status);
      workerResponse.headers.forEach((value: string, key: string) => {
        res.setHeader(key, value);
      });

      // Support streaming (SSE)
      const contentType = workerResponse.headers.get("content-type") || "";
      if (contentType.includes("text/event-stream")) {
        const reader = workerResponse.body?.getReader();
        if (reader) {
          const pump = async () => {
            while (true) {
              const { done, value } = await reader.read();
              if (done) { res.end(); return; }
              res.write(value);
            }
          };
          pump().catch(() => res.end());
        } else {
          res.end();
        }
      } else {
        const body = await workerResponse.text();
        res.send(body);
      }
      return;
    } catch (e) {
      if (e instanceof Deno.errors.WorkerAlreadyRetired && attempt < MAX_RETRIES - 1) {
        continue;
      }
      res.status(STATUS_CODE.InternalServerError).json({ msg: String(e) });
      return;
    }
  }
  res.status(STATUS_CODE.InternalServerError).json({ msg: "Worker unavailable after retries" });
}

app.all(`${BASE_PATH}/functions/v1/:function_name`, invokeEdgeFunction);
app.all(`${BASE_PATH}/functions/v1/:function_name/*`, invokeEdgeFunction);

// Function management API (Supabase CLI compatible)
app.use(functionsRouter);

// Serve self-hosted Shinylive assets (must be before SPA catch-all)
try {
  const shinyliveDistPath = join(Deno.cwd(), "shinylive");
  await Deno.stat(shinyliveDistPath);
  const serveShiny = (await import("express")).default.static;
  app.use(`${BASE_PATH}/shinylive`, serveShiny(shinyliveDistPath));
  console.log("Serving Shinylive assets from shinylive/");
} catch { /* shinylive assets not present — skip */ }

app.get("/", (_req, res) => {
  res.redirect("/plugins/trex/web/");
});

// Initialize auth keys (anon key, service_role key) + cache for edge functions
try {
  const authKeys = await ensureAuthKeys();
  console.log("[auth] Auth keys initialized");

  // Cache Supabase-compatible env vars for edge function workers
  const supabaseUrl = Deno.env.get("BETTER_AUTH_URL") || `http://localhost:8001${BASE_PATH}`;
  supabaseEnvVars = [
    ["SUPABASE_URL", supabaseUrl],
    ["SUPABASE_ANON_KEY", authKeys.anonKey],
    ["SUPABASE_SERVICE_ROLE_KEY", authKeys.serviceRoleKey],
    ["SUPABASE_DB_URL", Deno.env.get("DATABASE_URL") || ""],
  ];
  console.log("[functions] Supabase-compatible env vars cached for edge functions");
} catch (err) {
  console.error("[auth] Failed to initialize auth keys:", err);
}

// Load SSO providers
try {
  const tableCheck = await pool.query(
    `SELECT 1 FROM information_schema.tables WHERE table_schema = 'trex' AND table_name = 'sso_provider' LIMIT 1`
  );
  if (tableCheck.rows.length > 0) {
    const result = await pool.query(
      `SELECT id FROM trex.sso_provider WHERE enabled = true`
    );
    const names = result.rows.map((r: any) => r.id);
    console.log(`[auth] SSO providers: ${names.length > 0 ? names.join(", ") : "none"}`);
  }
} catch (err) {
  console.error("[auth] Failed to load SSO providers:", err);
}

// Bootstrap initial API key from env var (for Docker/CI)
const initialKeyName = Deno.env.get("TREX_INITIAL_API_KEY_NAME");
if (initialKeyName) {
  try {
    const existing = await pool.query("SELECT 1 FROM trex.api_key LIMIT 1");
    if (existing.rows.length === 0) {
      const { generateApiKey } = await import("./mcp/auth.ts");
      const adminResult = await pool.query(
        `SELECT id FROM trex."user" WHERE role = 'admin' ORDER BY "createdAt" ASC LIMIT 1`
      );
      if (adminResult.rows.length > 0) {
        const result = await generateApiKey(adminResult.rows[0].id, initialKeyName);
        console.log(`[mcp] Initial API key created: ${result.key.slice(0, 13)}...(redacted)`);
      }
    }
  } catch (err) {
    console.error("[mcp] Failed to bootstrap initial API key:", err);
  }
}

server.listen(8000, () => {
  console.log("server listening on port 8000");
});
