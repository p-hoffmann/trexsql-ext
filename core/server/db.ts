import { Pool } from "pg";

const databaseUrl = Deno.env.get("DATABASE_URL");
if (!databaseUrl) {
  throw new Error("DATABASE_URL environment variable is required");
}

const needsSsl = databaseUrl.includes("sslmode=require") || databaseUrl.includes("sslmode=prefer");

export const pool = new Pool({
  connectionString: databaseUrl,
  options: "-c search_path=trex,public",
  ...(needsSsl && { ssl: { rejectUnauthorized: false } }),
});
