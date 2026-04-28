import { Pool } from "pg";
import { buildSslConfig } from "./lib/db-ssl.ts";

const databaseUrl = Deno.env.get("DATABASE_URL");
if (!databaseUrl) {
  throw new Error("DATABASE_URL environment variable is required");
}

const ssl = buildSslConfig(databaseUrl);

export const pool = new Pool({
  connectionString: databaseUrl,
  options: "-c search_path=trex,public",
  ...(ssl && { ssl }),
});
