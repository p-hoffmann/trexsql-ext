// Shared TLS configuration for pg Pool clients.
//
// Default policy: when sslmode=require/prefer is in the connection string
// (i.e. SSL is needed), verify the server certificate. Insecure pass-through
// (rejectUnauthorized: false) is opt-in via DB_TLS_INSECURE=1, and a custom
// CA bundle path can be supplied via DB_TLS_CA_PATH.

export interface SslOptions {
  rejectUnauthorized: boolean;
  ca?: string;
}

let cachedCa: string | undefined;
let cachedCaLoaded = false;

function loadCa(): string | undefined {
  if (cachedCaLoaded) return cachedCa;
  cachedCaLoaded = true;
  const caPath = Deno.env.get("DB_TLS_CA_PATH");
  if (!caPath) return undefined;
  try {
    cachedCa = Deno.readTextFileSync(caPath);
  } catch (err) {
    console.warn(`[db-ssl] DB_TLS_CA_PATH=${caPath} could not be read:`, err);
    cachedCa = undefined;
  }
  return cachedCa;
}

function isTruthy(val: string | undefined): boolean {
  if (!val) return false;
  const v = val.trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

export function needsSsl(connectionString: string): boolean {
  return (
    connectionString.includes("sslmode=require") ||
    connectionString.includes("sslmode=prefer") ||
    connectionString.includes("sslmode=verify-ca") ||
    connectionString.includes("sslmode=verify-full")
  );
}

export function getSslOptions(): SslOptions {
  const insecure = isTruthy(Deno.env.get("DB_TLS_INSECURE"));
  const ca = loadCa();
  const opts: SslOptions = { rejectUnauthorized: !insecure };
  if (ca) opts.ca = ca;
  return opts;
}

// Convenience: build the {ssl: ...} fragment for a pg Pool config based on
// the connection string. Returns {} when no SSL is needed.
export function poolSsl(connectionString: string | undefined): { ssl?: SslOptions } {
  if (!connectionString) return {};
  if (!needsSsl(connectionString)) return {};
  return { ssl: getSslOptions() };
}
