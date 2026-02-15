-- Better Auth core tables in trex schema
-- Column names use camelCase to match Better Auth's default Kysely adapter

CREATE SCHEMA IF NOT EXISTS trex;
SET search_path TO trex;

CREATE TABLE IF NOT EXISTS "user" (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  "emailVerified" BOOLEAN DEFAULT false,
  image TEXT,
  role TEXT DEFAULT 'user',
  "deletedAt" TIMESTAMPTZ,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS session (
  id TEXT PRIMARY KEY,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  token TEXT NOT NULL UNIQUE,
  "expiresAt" TIMESTAMPTZ NOT NULL,
  "ipAddress" TEXT,
  "userAgent" TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS account (
  id TEXT PRIMARY KEY,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  "accountId" TEXT NOT NULL,
  "providerId" TEXT NOT NULL,
  "accessToken" TEXT,
  "refreshToken" TEXT,
  "accessTokenExpiresAt" TIMESTAMPTZ,
  "refreshTokenExpiresAt" TIMESTAMPTZ,
  scope TEXT,
  "idToken" TEXT,
  password TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE ("providerId", "accountId")
);

CREATE TABLE IF NOT EXISTS verification (
  id TEXT PRIMARY KEY,
  identifier TEXT NOT NULL,
  value TEXT NOT NULL,
  "expiresAt" TIMESTAMPTZ NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

-- JWT plugin table
CREATE TABLE IF NOT EXISTS jwks (
  id TEXT PRIMARY KEY,
  "publicKey" TEXT NOT NULL,
  "privateKey" TEXT NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW()
);

-- OIDC Provider plugin tables
CREATE TABLE IF NOT EXISTS oauth_application (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  icon TEXT,
  metadata TEXT,
  "clientId" TEXT NOT NULL UNIQUE,
  "clientSecret" TEXT NOT NULL,
  "redirectURLs" TEXT NOT NULL,
  type TEXT DEFAULT 'web',
  disabled BOOLEAN DEFAULT false,
  "userId" TEXT REFERENCES "user"(id) ON DELETE SET NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oauth_access_token (
  id TEXT PRIMARY KEY,
  "accessToken" TEXT NOT NULL,
  "refreshToken" TEXT,
  "accessTokenExpiresAt" TIMESTAMPTZ NOT NULL,
  "refreshTokenExpiresAt" TIMESTAMPTZ,
  "clientId" TEXT NOT NULL,
  "userId" TEXT REFERENCES "user"(id) ON DELETE CASCADE,
  scopes TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oauth_consent (
  id TEXT PRIMARY KEY,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  "clientId" TEXT NOT NULL,
  scopes TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oauth_authorization_code (
  id TEXT PRIMARY KEY,
  code TEXT NOT NULL,
  "clientId" TEXT NOT NULL,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  scopes TEXT,
  "redirectURI" TEXT NOT NULL,
  "codeChallenge" TEXT,
  "codeChallengeMethod" TEXT,
  "expiresAt" TIMESTAMPTZ NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);
