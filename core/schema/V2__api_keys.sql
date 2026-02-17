-- API Keys for MCP server authentication
SET search_path TO trex;

CREATE TABLE IF NOT EXISTS api_key (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name TEXT NOT NULL,
  key_hash TEXT NOT NULL UNIQUE,
  key_prefix TEXT NOT NULL,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  "lastUsedAt" TIMESTAMPTZ,
  "expiresAt" TIMESTAMPTZ,
  "revokedAt" TIMESTAMPTZ,
  "createdAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_key_user_id ON api_key("userId");
CREATE INDEX IF NOT EXISTS idx_api_key_key_hash ON api_key(key_hash);

ALTER TABLE api_key ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_api_keys ON api_key
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

COMMENT ON TABLE api_key IS E'@omit';
