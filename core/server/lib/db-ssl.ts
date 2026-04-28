// Builds the `ssl` option for `pg.Pool` from a Postgres connection string.
//
// Default is strict cert validation (`rejectUnauthorized: true`).
// Override knobs (in order of precedence):
//   DB_TLS_INSECURE=true     — disable cert validation. Dev/local only. Logged on use.
//   DB_TLS_CA_PATH=/path/ca  — load a custom CA bundle (PEM) for self-signed/private CAs.
//
// Returns undefined when the URL doesn't request SSL — pg falls back to plaintext.
export function buildSslConfig(
  databaseUrl: string,
): { rejectUnauthorized: boolean; ca?: string } | undefined {
  const needsSsl = databaseUrl.includes("sslmode=require") ||
    databaseUrl.includes("sslmode=prefer");
  if (!needsSsl) return undefined;

  if (Deno.env.get("DB_TLS_INSECURE") === "true") {
    console.warn(
      "DB_TLS_INSECURE=true — accepting any DB cert. Do not use in production.",
    );
    return { rejectUnauthorized: false };
  }

  const caPath = Deno.env.get("DB_TLS_CA_PATH");
  if (caPath) {
    try {
      return { rejectUnauthorized: true, ca: Deno.readTextFileSync(caPath) };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      throw new Error(`DB_TLS_CA_PATH=${caPath} could not be read: ${msg}`);
    }
  }

  return { rejectUnauthorized: true };
}
