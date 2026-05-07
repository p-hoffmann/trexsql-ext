import { BASE_PATH } from "../config.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function base64url(data: Uint8Array): string {
  return btoa(String.fromCharCode(...data))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function base64urlEncode(str: string): string {
  return base64url(encoder.encode(str));
}

function base64urlDecode(str: string): string {
  let s = str.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  const binary = atob(s);
  const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0));
  return decoder.decode(bytes);
}

function base64urlDecodeBytes(str: string): Uint8Array {
  let s = str.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  const binary = atob(s);
  return Uint8Array.from(binary, (c) => c.charCodeAt(0));
}

async function hmacSign(data: string, secret: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, encoder.encode(data));
  return new Uint8Array(sig);
}

async function hmacVerify(
  data: string,
  signature: Uint8Array,
  secret: string,
): Promise<boolean> {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  );
  return crypto.subtle.verify("HMAC", key, signature, encoder.encode(data));
}

/**
 * Return all JWT signing keys, in priority order.
 * The first entry is the active key (used for signing); all entries are
 * accepted for verification.
 *
 * Keys are sourced from:
 *   - BETTER_AUTH_SECRETS (comma-separated; first entry is active)
 *   - BETTER_AUTH_SECRET / AUTH_JWT_SECRET (single secret, current behavior)
 *
 * If both are set, BETTER_AUTH_SECRETS entries come first, then any
 * single-secret value not already present. This lets operators define a
 * rotation list without having to also clear the legacy single-secret env
 * var. See plugins/docs/docs/operations/secret-rotation.md.
 */
export function getJwtSecrets(): string[] {
  const list = (Deno.env.get("BETTER_AUTH_SECRETS") || "")
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  const single =
    Deno.env.get("AUTH_JWT_SECRET") ||
    Deno.env.get("BETTER_AUTH_SECRET") ||
    "";
  if (single && !list.includes(single)) {
    list.push(single);
  }
  return list;
}

/**
 * Return the active signing key (first in BETTER_AUTH_SECRETS, or
 * BETTER_AUTH_SECRET if the list is unset). Empty string if neither is set.
 */
export function getJwtSecret(): string {
  const secrets = getJwtSecrets();
  return secrets[0] || "";
}

export interface AccessTokenClaims {
  sub: string;
  role: string;
  aud: string;
  iss: string;
  exp: number;
  iat: number;
  email: string;
  app_metadata: { provider: string; providers: string[]; trex_role: string };
  user_metadata: Record<string, unknown>;
  session_id: string;
}

export async function signAccessToken(
  user: {
    id: string;
    email: string;
    role: string;
    app_metadata?: Record<string, unknown>;
    user_metadata?: Record<string, unknown>;
  },
  sessionId: string,
): Promise<string> {
  const secret = getJwtSecret();
  const now = Math.floor(Date.now() / 1000);
  const rawUrl = Deno.env.get("BETTER_AUTH_URL") || "http://localhost:8000";
  let issOrigin: string;
  try {
    issOrigin = new URL(rawUrl).origin;
  } catch {
    issOrigin = rawUrl;
  }

  const payload: AccessTokenClaims = {
    sub: user.id,
    role: "authenticated",
    aud: "authenticated",
    iss: `${issOrigin}${BASE_PATH}/auth/v1`,
    exp: now + 3600, // 1 hour
    iat: now,
    email: user.email,
    app_metadata: {
      provider: "email",
      providers: ["email"],
      trex_role: user.role,
      ...(user.app_metadata || {}),
    },
    user_metadata: user.user_metadata || {},
    session_id: sessionId,
  };

  const header = base64urlEncode(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = base64urlEncode(JSON.stringify(payload));
  const data = `${header}.${body}`;
  const sig = await hmacSign(data, secret);
  return `${data}.${base64url(sig)}`;
}

export async function verifyAccessToken(
  token: string,
): Promise<AccessTokenClaims | null> {
  const secrets = getJwtSecrets();
  if (secrets.length === 0 || !token) return null;

  const parts = token.split(".");
  if (parts.length !== 3) return null;

  const [header, body, sig] = parts;
  const data = `${header}.${body}`;

  try {
    const sigBytes = base64urlDecodeBytes(sig);

    // Try the active key first, then each older key in turn. Any successful
    // verification accepts the token. This supports overlap windows during
    // signing-key rotation. See operations/secret-rotation.md.
    let valid = false;
    for (const secret of secrets) {
      if (await hmacVerify(data, sigBytes, secret)) {
        valid = true;
        break;
      }
    }
    if (!valid) return null;

    const payload = JSON.parse(base64urlDecode(body));

    // Check expiry
    if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) {
      return null;
    }

    return payload as AccessTokenClaims;
  } catch {
    return null;
  }
}

export function generateRefreshToken(): string {
  return crypto.randomUUID();
}

export async function hashRefreshToken(token: string): Promise<string> {
  const hash = await crypto.subtle.digest("SHA-256", encoder.encode(token));
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

export async function generateAnonKey(): Promise<string> {
  const secret = getJwtSecret();
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    role: "anon",
    iss: "supabase",
    iat: now,
    exp: now + 100 * 365 * 24 * 60 * 60, // 100 years
  };
  const header = base64urlEncode(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = base64urlEncode(JSON.stringify(payload));
  const data = `${header}.${body}`;
  const sig = await hmacSign(data, secret);
  return `${data}.${base64url(sig)}`;
}

export async function generateServiceRoleKey(): Promise<string> {
  const secret = getJwtSecret();
  const now = Math.floor(Date.now() / 1000);
  const payload = {
    role: "service_role",
    iss: "supabase",
    iat: now,
    exp: now + 100 * 365 * 24 * 60 * 60, // 100 years
  };
  const header = base64urlEncode(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = base64urlEncode(JSON.stringify(payload));
  const data = `${header}.${body}`;
  const sig = await hmacSign(data, secret);
  return `${data}.${base64url(sig)}`;
}

// Lazy import inside the function bodies to avoid a circular import via
// ../db.ts (which imports config); we want jwt.ts to stay leaf-level.

/**
 * Generate a new anon key signed with the active signing key, persist it
 * to trex.setting under 'auth.anonKey', and return the new value.
 *
 * All clients holding the previous anon key will be rejected on next use.
 * See plugins/docs/docs/operations/secret-rotation.md.
 */
export async function rotateAnonKey(): Promise<string> {
  const { pool } = await import("../db.ts");
  const newKey = await generateAnonKey();
  await pool.query(
    `INSERT INTO trex.setting (key, value) VALUES ('auth.anonKey', $1::jsonb)
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
    [JSON.stringify(newKey)],
  );
  return newKey;
}

/**
 * Generate a new service_role key signed with the active signing key,
 * persist it to trex.setting under 'auth.serviceRoleKey', and return the
 * new value.
 *
 * All clients holding the previous service_role key will be rejected on
 * next use. See plugins/docs/docs/operations/secret-rotation.md.
 */
export async function rotateServiceRoleKey(): Promise<string> {
  const { pool } = await import("../db.ts");
  const newKey = await generateServiceRoleKey();
  await pool.query(
    `INSERT INTO trex.setting (key, value) VALUES ('auth.serviceRoleKey', $1::jsonb)
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
    [JSON.stringify(newKey)],
  );
  return newKey;
}
