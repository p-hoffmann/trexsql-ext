-- TrexSQL Core Schema
-- Combines: auth tables, extensions, RLS, functions, smart comments,
--           database management, roles, SSO providers

CREATE SCHEMA IF NOT EXISTS trex;
SET search_path TO trex;

-- ── Auth tables (Better Auth) ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS "user" (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  "emailVerified" BOOLEAN DEFAULT false,
  image TEXT,
  role TEXT DEFAULT 'user',
  banned BOOLEAN DEFAULT false,
  "banReason" TEXT,
  "banExpires" TIMESTAMPTZ,
  "mustChangePassword" BOOLEAN DEFAULT false,
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

CREATE TABLE IF NOT EXISTS jwks (
  id TEXT PRIMARY KEY,
  "publicKey" TEXT NOT NULL,
  "privateKey" TEXT NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW()
);

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

-- ── Indexes ──────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_user_email ON "user"(email);
CREATE INDEX IF NOT EXISTS idx_user_deleted_at ON "user"("deletedAt") WHERE "deletedAt" IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_account_user_id ON account("userId");
CREATE UNIQUE INDEX IF NOT EXISTS idx_account_provider ON account("providerId", "accountId");
CREATE INDEX IF NOT EXISTS idx_session_user_id ON session("userId");
CREATE INDEX IF NOT EXISTS idx_session_token ON session(token);
CREATE INDEX IF NOT EXISTS idx_session_expires_at ON session("expiresAt");

-- ── Row Level Security ───────────────────────────────────────────────────────

ALTER TABLE "user" ENABLE ROW LEVEL SECURITY;
ALTER TABLE account ENABLE ROW LEVEL SECURITY;
ALTER TABLE session ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_users ON "user"
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_row ON "user"
  FOR ALL
  USING (id = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK (id = nullif(current_setting('app.user_id', true), ''));

CREATE POLICY admin_all_accounts ON account
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_accounts ON account
  FOR SELECT
  USING ("userId" = nullif(current_setting('app.user_id', true), ''));

CREATE POLICY admin_all_sessions ON session
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_sessions ON session
  FOR ALL
  USING ("userId" = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK ("userId" = nullif(current_setting('app.user_id', true), ''));

-- ── Functions ────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION current_user_id() RETURNS TEXT AS $$
  SELECT nullif(current_setting('app.user_id', true), '');
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION current_user_is_admin() RETURNS BOOLEAN AS $$
  SELECT current_setting('app.user_role', true) = 'admin';
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION soft_delete_user(target_user_id TEXT) RETURNS "user" AS $$
  UPDATE "user"
  SET "deletedAt" = NOW(), banned = true, "updatedAt" = NOW()
  WHERE id = target_user_id AND "deletedAt" IS NULL
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER;

CREATE OR REPLACE FUNCTION restore_user(target_user_id TEXT) RETURNS "user" AS $$
  UPDATE "user"
  SET "deletedAt" = NULL, banned = false, "banReason" = NULL, "updatedAt" = NOW()
  WHERE id = target_user_id AND "deletedAt" IS NOT NULL
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER;

CREATE OR REPLACE FUNCTION search_users(query TEXT)
RETURNS SETOF "user" AS $$
  SELECT * FROM "user"
  WHERE "deletedAt" IS NULL
    AND (name ILIKE '%' || query || '%' OR email ILIKE '%' || query || '%')
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION purge_deleted_users() RETURNS INTEGER AS $$
  WITH deleted AS (
    DELETE FROM "user"
    WHERE "deletedAt" IS NOT NULL
      AND "deletedAt" < NOW() - INTERVAL '30 days'
    RETURNING id
  )
  SELECT COUNT(*)::INTEGER FROM deleted;
$$ LANGUAGE SQL VOLATILE;

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW."updatedAt" = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ── Smart comments ───────────────────────────────────────────────────────────

COMMENT ON COLUMN "user"."mustChangePassword" IS E'@omit update';
COMMENT ON COLUMN account.password IS E'@omit';
COMMENT ON COLUMN account."accessToken" IS E'@omit';
COMMENT ON COLUMN account."refreshToken" IS E'@omit';
COMMENT ON COLUMN account."idToken" IS E'@omit';
COMMENT ON TABLE verification IS E'@omit';
COMMENT ON TABLE jwks IS E'@omit';
COMMENT ON TABLE oauth_access_token IS E'@omit';
COMMENT ON TABLE oauth_authorization_code IS E'@omit';
COMMENT ON TABLE oauth_consent IS E'@omit';
COMMENT ON COLUMN oauth_application."clientSecret" IS E'@omit';

-- ── Database management ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS database (
  id TEXT PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_]+$'),
  host TEXT NOT NULL,
  port INTEGER NOT NULL DEFAULT 5432,
  "databaseName" TEXT NOT NULL,
  dialect TEXT NOT NULL DEFAULT 'postgresql',
  description TEXT,
  enabled BOOLEAN DEFAULT true,
  "vocabSchemas" JSONB,
  extra JSONB,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS database_credential (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  "databaseId" TEXT NOT NULL REFERENCES database(id) ON DELETE CASCADE,
  username TEXT NOT NULL,
  password TEXT NOT NULL,
  "userScope" TEXT,
  "serviceScope" TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_database_credential_database_id ON database_credential("databaseId");

ALTER TABLE database ENABLE ROW LEVEL SECURITY;
ALTER TABLE database_credential ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_databases ON database
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY admin_all_database_credentials ON database_credential
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

COMMENT ON COLUMN database_credential.password IS E'@omit';

CREATE OR REPLACE FUNCTION search_databases(query TEXT)
RETURNS SETOF database AS $$
  SELECT * FROM database
  WHERE id ILIKE '%' || query || '%'
    OR host ILIKE '%' || query || '%'
    OR "databaseName" ILIKE '%' || query || '%'
    OR description ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION save_database_credential(
  p_database_id TEXT,
  p_username TEXT,
  p_password TEXT,
  p_user_scope TEXT DEFAULT NULL,
  p_service_scope TEXT DEFAULT NULL
) RETURNS database_credential AS $$
  INSERT INTO database_credential ("databaseId", username, password, "userScope", "serviceScope")
  VALUES (p_database_id, p_username, p_password, p_user_scope, p_service_scope)
  ON CONFLICT (id) DO UPDATE SET
    username = EXCLUDED.username,
    password = EXCLUDED.password,
    "userScope" = EXCLUDED."userScope",
    "serviceScope" = EXCLUDED."serviceScope",
    "updatedAt" = NOW()
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER;

DROP TRIGGER IF EXISTS trg_database_updated_at ON database;
CREATE TRIGGER trg_database_updated_at
  BEFORE UPDATE ON database
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS trg_database_credential_updated_at ON database_credential;
CREATE TRIGGER trg_database_credential_updated_at
  BEFORE UPDATE ON database_credential
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ── Roles ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS role (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_role (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  "roleId" TEXT NOT NULL REFERENCES role(id) ON DELETE CASCADE,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE ("userId", "roleId")
);

CREATE INDEX IF NOT EXISTS idx_user_role_user_id ON user_role("userId");
CREATE INDEX IF NOT EXISTS idx_user_role_role_id ON user_role("roleId");

ALTER TABLE role ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_role ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_roles ON role
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY admin_all_user_roles ON user_role
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_user_roles ON user_role
  FOR SELECT
  USING ("userId" = nullif(current_setting('app.user_id', true), ''));

CREATE OR REPLACE FUNCTION search_roles(query TEXT)
RETURNS SETOF role AS $$
  SELECT * FROM role
  WHERE name ILIKE '%' || query || '%'
    OR description ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

DROP TRIGGER IF EXISTS trg_role_updated_at ON role;
CREATE TRIGGER trg_role_updated_at
  BEFORE UPDATE ON role
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ── SSO providers ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS sso_provider (
  id TEXT PRIMARY KEY CHECK (id ~ '^[a-z][a-z0-9_]*$'),
  "displayName" TEXT NOT NULL,
  "clientId" TEXT NOT NULL,
  "clientSecret" TEXT NOT NULL,
  enabled BOOLEAN DEFAULT false,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE sso_provider ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_sso_providers ON sso_provider
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

COMMENT ON COLUMN sso_provider."clientSecret" IS E'@omit';

CREATE OR REPLACE FUNCTION save_sso_provider(
  p_id TEXT,
  p_display_name TEXT,
  p_client_id TEXT,
  p_client_secret TEXT,
  p_enabled BOOLEAN DEFAULT false
) RETURNS sso_provider AS $$
  INSERT INTO trex.sso_provider (id, "displayName", "clientId", "clientSecret", enabled)
  VALUES (p_id, p_display_name, p_client_id, p_client_secret, p_enabled)
  ON CONFLICT (id) DO UPDATE SET
    "displayName" = EXCLUDED."displayName",
    "clientId" = EXCLUDED."clientId",
    "clientSecret" = CASE
      WHEN EXCLUDED."clientSecret" = '' THEN trex.sso_provider."clientSecret"
      ELSE EXCLUDED."clientSecret"
    END,
    enabled = EXCLUDED.enabled,
    "updatedAt" = NOW()
  RETURNING *;
$$ LANGUAGE SQL VOLATILE SECURITY DEFINER;

CREATE OR REPLACE FUNCTION enabled_sso_providers()
RETURNS TABLE(id TEXT, "displayName" TEXT) AS $$
  SELECT id, "displayName" FROM trex.sso_provider WHERE enabled = true ORDER BY id;
$$ LANGUAGE SQL STABLE SECURITY DEFINER;

CREATE OR REPLACE FUNCTION search_sso_providers(query TEXT)
RETURNS SETOF sso_provider AS $$
  SELECT * FROM trex.sso_provider
  WHERE id ILIKE '%' || query || '%'
    OR "displayName" ILIKE '%' || query || '%'
    OR "clientId" ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

DROP TRIGGER IF EXISTS trg_sso_provider_updated_at ON sso_provider;
CREATE TRIGGER trg_sso_provider_updated_at
  BEFORE UPDATE ON sso_provider
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ── Seed admin user ──────────────────────────────────────────────────────────
-- email: admin@local.com / password: password (scrypt N=16384 r=16 p=1 dkLen=64)

INSERT INTO "user" (id, name, email, "emailVerified", role, "mustChangePassword")
VALUES ('00000000-0000-0000-0000-000000000001', 'Admin', 'admin@local.com', true, 'admin', true)
ON CONFLICT (id) DO NOTHING;

INSERT INTO account (id, "userId", "accountId", "providerId", password)
VALUES (
  '00000000-0000-0000-0000-000000000002',
  '00000000-0000-0000-0000-000000000001',
  '00000000-0000-0000-0000-000000000001',
  'credential',
  'a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6:0c219b9e9260518faacd40f6bbaf04d86622631a745d5a177a0f8ff18363b52ed0f56a940c02a8bf0bb314b293d8ccfd5383e537bd18f445bb31f5bc979a020f'
)
ON CONFLICT (id) DO NOTHING;
