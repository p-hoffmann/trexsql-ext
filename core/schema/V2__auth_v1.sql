-- GoTrue-compatible auth extensions

SET search_path TO trex;

-- Refresh token storage (GoTrue-style sessions)
CREATE TABLE IF NOT EXISTS refresh_token (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  token_hash TEXT NOT NULL UNIQUE,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  session_id UUID NOT NULL,
  revoked BOOLEAN DEFAULT false,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_refresh_token_hash ON refresh_token(token_hash);
CREATE INDEX IF NOT EXISTS idx_refresh_token_user ON refresh_token("userId");

-- Add GoTrue-compatible columns to user table
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS password_hash TEXT;
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS email_confirmed_at TIMESTAMPTZ;
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS last_sign_in_at TIMESTAMPTZ;
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS phone TEXT;
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS user_metadata JSONB DEFAULT '{}';
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS app_metadata JSONB DEFAULT '{"provider":"email","providers":["email"]}';

-- Backfill email_confirmed_at from emailVerified
UPDATE "user" SET email_confirmed_at = "createdAt" WHERE "emailVerified" = true AND email_confirmed_at IS NULL;

-- Migrate password hashes from Better Auth account table to user.password_hash
UPDATE "user" u SET password_hash = a.password
FROM account a
WHERE a."userId" = u.id
  AND a."providerId" = 'credential'
  AND a.password IS NOT NULL
  AND u.password_hash IS NULL;

-- GoTrue-compatible auth helper functions
CREATE SCHEMA IF NOT EXISTS auth;

CREATE OR REPLACE FUNCTION auth.uid() RETURNS TEXT AS $$
  SELECT COALESCE(
    nullif(current_setting('request.jwt.claims', true)::json->>'sub', ''),
    nullif(current_setting('app.user_id', true), '')
  );
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION auth.jwt() RETURNS JSON AS $$
  SELECT current_setting('request.jwt.claims', true)::json;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION auth.role() RETURNS TEXT AS $$
  SELECT COALESCE(
    nullif(current_setting('request.jwt.claims', true)::json->>'role', ''),
    nullif(current_setting('app.user_role', true), '')
  );
$$ LANGUAGE SQL STABLE;

-- RLS on refresh_token
ALTER TABLE refresh_token ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS admin_all_refresh_tokens ON refresh_token;
CREATE POLICY admin_all_refresh_tokens ON refresh_token
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

DROP POLICY IF EXISTS user_own_refresh_tokens ON refresh_token;
CREATE POLICY user_own_refresh_tokens ON refresh_token
  FOR ALL
  USING ("userId" = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK ("userId" = nullif(current_setting('app.user_id', true), ''));

COMMENT ON TABLE refresh_token IS E'@omit';

DROP TRIGGER IF EXISTS trg_refresh_token_updated_at ON refresh_token;
CREATE TRIGGER trg_refresh_token_updated_at
  BEFORE UPDATE ON refresh_token
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
