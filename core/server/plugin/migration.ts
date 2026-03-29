declare const Trex: any;

export interface MigrationPluginInfo {
  pluginName: string;
  schema: string;
  database: string;
  migrationsPath: string;
}

const migrationRegistry: Map<string, MigrationPluginInfo> = new Map();

export function addMigrationPlugin(
  value: any,
  dir: string,
  shortName: string
) {
  const schema = value?.schema;
  if (!schema || typeof schema !== "string") {
    console.warn(
      `Plugin ${shortName} has invalid migration config (missing schema) — skipping`
    );
    return;
  }

  const database = value?.database || "_config";
  const migrationsPath = `${dir}/migrations`;

  migrationRegistry.set(shortName, {
    pluginName: shortName,
    schema,
    database,
    migrationsPath,
  });
  console.log(
    `Registered migration plugin ${shortName} (schema: ${schema}, database: ${database})`
  );
}

export function getMigrationPlugins(): MigrationPluginInfo[] {
  return Array.from(migrationRegistry.values());
}

import { escapeSql } from "../lib/sql.ts";

export async function runAllPluginMigrations(): Promise<void> {
  if (migrationRegistry.size === 0) {
    return;
  }

  console.log(
    `Running migrations for ${migrationRegistry.size} plugin(s)...`
  );

  const conn = new Trex.TrexDB("memory");

  // Plugin migrations run via direct PostgreSQL connection since DuckDB's
  // postgres extension can't be used inside the PG process (libpq conflict).
  const databaseUrl = Deno.env.get("DATABASE_URL") || "";
  if (!databaseUrl) {
    console.warn("DATABASE_URL not set — skipping plugin migrations");
    return;
  }

  try {
    const { Pool } = await import("pg");
    const sslRequired = databaseUrl.includes("sslmode=require") || databaseUrl.includes("sslmode=prefer");
    const pool = new Pool({ connectionString: databaseUrl, ...(sslRequired && { ssl: { rejectUnauthorized: false } }) });

    for (const [name, info] of migrationRegistry) {
      try {
        // Read migration files and execute them in order
        const files: string[] = [];
        for await (const entry of Deno.readDir(info.migrationsPath)) {
          if (entry.isFile && entry.name.endsWith(".sql")) {
            files.push(entry.name);
          }
        }
        files.sort((a, b) => {
          const va = parseInt(a.match(/^V(\d+)/)?.[1] || "0", 10);
          const vb = parseInt(b.match(/^V(\d+)/)?.[1] || "0", 10);
          return va - vb;
        });

        // Ensure schema exists
        await pool.query(`CREATE SCHEMA IF NOT EXISTS ${info.schema}`);

        // Track applied migrations
        await pool.query(`CREATE TABLE IF NOT EXISTS ${info.schema}._migrations (
          version TEXT PRIMARY KEY,
          applied_at TIMESTAMPTZ DEFAULT NOW()
        )`);

        for (const file of files) {
          const version = file.replace(".sql", "");
          const check = await pool.query(
            `SELECT 1 FROM ${info.schema}._migrations WHERE version = $1`,
            [version]
          );
          if (check.rows.length > 0) continue;

          const sql = await Deno.readTextFile(`${info.migrationsPath}/${file}`);
          await pool.query(sql);
          await pool.query(
            `INSERT INTO ${info.schema}._migrations (version) VALUES ($1)`,
            [version]
          );
          console.log(`Plugin ${name}: applied migration ${version}`);
        }
        console.log(`Plugin ${name}: migrations up to date`);
      } catch (err) {
        console.error(`Plugin ${name}: migration failed:`, err);
      }
    }

    await pool.end();
  } catch (err) {
    console.error("Plugin migration runner failed:", err);
  }
}
