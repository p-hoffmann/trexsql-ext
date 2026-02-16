// @ts-ignore
import { STATUS_CODE } from "https://deno.land/std/http/status.ts";
import { join } from "jsr:@std/path@^1.0";
import express from "express";
import { createServer } from "node:http";
import { grafserv } from "postgraphile/grafserv/express/v4";
import cors from "cors";
import { BASE_PATH, FUNCTIONS_BASE_PATH } from "./config.ts";
import { auth } from "./auth.ts";
import { createPostGraphile } from "./postgraphile.ts";
import { authContext } from "./middleware/auth-context.ts";
import { Plugins } from "./plugin/plugin.ts";
import { addPluginRoutes } from "./routes/plugin.ts";

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

app.use(cors({ origin: true, credentials: true }));

// Better Auth handler — construct a web Request from Express req
// since toNodeHandler has body-parsing issues in the Deno runtime
app.all(`${BASE_PATH}/api/auth/*`, async (req, res) => {
  try {
    const host = req.get("host") || "localhost";
    const protocol = req.protocol || "http";
    const url = `${protocol}://${host}${req.originalUrl}`;

    const headers = new Headers();
    for (const [key, val] of Object.entries(req.headers)) {
      if (val) headers.set(key, Array.isArray(val) ? val.join(", ") : val);
    }

    let body: Blob | undefined;
    if (req.method !== "GET" && req.method !== "HEAD") {
      const chunks: Uint8Array[] = [];
      for await (const chunk of req) {
        chunks.push(typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk);
      }
      if (chunks.length > 0) body = new Blob(chunks);
    }

    const webReq = new Request(url, {
      method: req.method,
      headers,
      body,
    });

    const webRes = await auth.handler(webReq);

    res.status(webRes.status);
    webRes.headers.forEach((value, key) => {
      res.append(key, value);
    });
    const text = await webRes.text();
    res.send(text);
  } catch (err) {
    console.error("Auth handler error:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Auth context middleware for PostGraphile
app.use(authContext);

// Plugin system: discover and register plugins
try {
  await Plugins.initPlugins(app);
  addPluginRoutes(app);
  console.log("Plugin system initialized");
} catch (err) {
  console.error("Plugin system failed to initialize:", err);
}

// Test database connection endpoint
app.post(`${BASE_PATH}/api/db/test-connection`, express.json(), async (req, res) => {
  const pgSettings = (req as any).pgSettings || {};
  if (pgSettings["app.user_role"] !== "admin") {
    res.status(403).json({ success: false, message: "Forbidden" });
    return;
  }

  const { databaseId } = req.body;
  if (!databaseId) {
    res.status(400).json({ success: false, message: "databaseId is required" });
    return;
  }

  const mainPool = new (await import("pg")).default.Pool({
    connectionString: Deno.env.get("DATABASE_URL"),
  });

  try {
    const dbResult = await mainPool.query(
      `SELECT host, port, "databaseName", dialect FROM trex.database WHERE id = $1`,
      [databaseId]
    );
    if (dbResult.rows.length === 0) {
      res.status(404).json({ success: false, message: "Database not found" });
      return;
    }

    const db = dbResult.rows[0];

    const credResult = await mainPool.query(
      `SELECT username, password FROM trex.database_credential WHERE "databaseId" = $1 LIMIT 1`,
      [databaseId]
    );
    if (credResult.rows.length === 0) {
      res.status(400).json({ success: false, message: "No credentials configured" });
      return;
    }

    const cred = credResult.rows[0];

    const testPool = new (await import("pg")).default.Pool({
      host: db.host,
      port: db.port,
      database: db.databaseName,
      user: cred.username,
      password: cred.password,
      connectionTimeoutMillis: 5000,
      max: 1,
    });

    try {
      const client = await testPool.connect();
      await client.query("SELECT 1");
      client.release();
      res.json({ success: true, message: "Connection successful" });
    } catch (connErr: any) {
      res.json({ success: false, message: connErr.message || "Connection failed" });
    } finally {
      await testPool.end();
    }
  } catch (err: any) {
    console.error("Test connection error:", err);
    res.status(500).json({ success: false, message: "Internal server error" });
  } finally {
    await mainPool.end();
  }
});

// PostGraphile
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

// Internal endpoints
app.get(`${BASE_PATH}/_internal/health`, (_req, res) => {
  res.status(STATUS_CODE.OK).json({ message: "ok" });
});

app.get(`${BASE_PATH}/_internal/metric`, async (_req, res) => {
  const metric = await EdgeRuntime.getRuntimeMetrics();
  res.json(metric);
});

app.put(`${BASE_PATH}/_internal/upload`, async (req, res) => {
  try {
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

// Worker routing (must be before SPA catch-all)
app.use(`${FUNCTIONS_BASE_PATH}/:service_name`, async (req, res, next) => {
  const serviceName = req.params.service_name;

  if (
    serviceName === "_internal" ||
    serviceName === "graphql" ||
    serviceName === "graphiql" ||
    serviceName === "api"
  ) {
    return next();
  }

  let servicePath: string;
  if (serviceName.startsWith("tmp")) {
    try {
      servicePath = await Deno.realPath(`/tmp/${serviceName}`);
    } catch (err) {
      res.status(STATUS_CODE.BadRequest).json(err);
      return;
    }
  } else {
    servicePath = `./functions/${serviceName}`;
  }

  // Check if function directory exists before trying to create a worker
  try {
    await Deno.stat(servicePath);
  } catch {
    return next();
  }

  const createWorker = async () => {
    const memoryLimitMb = 150;
    const workerTimeoutMs = 5 * 60 * 1000;
    const noModuleCache = false;
    const envVarsObj = Deno.env.toObject();
    const envVars = Object.keys(envVarsObj).map((k) => [k, envVarsObj[k]]);
    const forceCreate = false;
    const cpuTimeSoftLimitMs = 10000;
    const cpuTimeHardLimitMs = 20000;

    return await EdgeRuntime.userWorkers.create({
      servicePath,
      memoryLimitMb,
      workerTimeoutMs,
      noModuleCache,
      envVars,
      forceCreate,
      cpuTimeSoftLimitMs,
      cpuTimeHardLimitMs,
      context: {
        useReadSyncFileAPI: true,
        unstableSloppyImports: true,
      },
    });
  };

  // Build a web-standard Request from the Express req
  const host = req.get("host") || "localhost";
  const protocol = req.protocol || "http";
  const webUrl = `${protocol}://${host}${req.originalUrl}`;
  const webHeaders = new Headers();
  for (const [key, val] of Object.entries(req.headers)) {
    if (val) webHeaders.set(key, Array.isArray(val) ? val.join(", ") : val);
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

  const callWorker = async (): Promise<Response> => {
    try {
      const worker = await createWorker();
      const controller = new AbortController();
      return await worker.fetch(webReq, { signal: controller.signal });
    } catch (e) {
      if (e instanceof Deno.errors.WorkerAlreadyRetired) {
        return await callWorker();
      }

      const error = { msg: e.toString() };
      return new Response(JSON.stringify(error), {
        status: STATUS_CODE.InternalServerError,
        headers: { "Content-Type": "application/json" },
      });
    }
  };

  try {
    const workerResponse = await callWorker();
    res.status(workerResponse.status);
    workerResponse.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });
    const body = await workerResponse.text();
    res.send(body);
  } catch (err) {
    res.status(STATUS_CODE.InternalServerError).json({ msg: String(err) });
  }
});

// Static file serving for production frontend build
try {
  const webDistPath = join(Deno.cwd(), "core", "web", "dist");
  await Deno.stat(webDistPath);
  const serveStatic = (await import("express")).default.static;
  app.use(BASE_PATH, serveStatic(webDistPath));
  app.get(`${BASE_PATH}/*`, (_req, res, next) => {
    if (_req.path.startsWith(`${BASE_PATH}/api/`) || _req.path.startsWith(`${BASE_PATH}/graphql`) || _req.path.startsWith(`${BASE_PATH}/graphiql`) || _req.path.startsWith(`${BASE_PATH}/_internal`)) {
      return next();
    }
    res.sendFile(join(webDistPath, "index.html"));
  });
  console.log("Serving static files from core/web/dist/");
} catch (e) {
  console.warn("Static file serving disabled:", e);
}

// Redirect root to BASE_PATH
app.get("/", (_req, res) => {
  res.redirect(`${BASE_PATH}/`);
});

server.listen(8000, () => {
  console.log("server listening on port 8000");
});
