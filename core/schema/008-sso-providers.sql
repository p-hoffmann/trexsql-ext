-- SSO provider configuration (DB-backed, hot-reloadable)

SET search_path TO trex;

CREATE TABLE IF NOT EXISTS sso_provider (
  id TEXT PRIMARY KEY CHECK (id ~ '^[a-z][a-z0-9_]*$'),
  "displayName" TEXT NOT NULL,
  "clientId" TEXT NOT NULL,
  "clientSecret" TEXT NOT NULL,
  enabled BOOLEAN DEFAULT false,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

-- RLS
ALTER TABLE sso_provider ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_sso_providers ON sso_provider
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

-- Hide clientSecret from GraphQL reads
COMMENT ON COLUMN sso_provider."clientSecret" IS E'@omit';

-- Upsert function (SECURITY DEFINER so it bypasses RLS and accepts clientSecret)
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

-- Public-safe function: returns only id + displayName for enabled providers (used by login page)
CREATE OR REPLACE FUNCTION enabled_sso_providers()
RETURNS TABLE(id TEXT, "displayName" TEXT) AS $$
  SELECT id, "displayName" FROM trex.sso_provider WHERE enabled = true ORDER BY id;
$$ LANGUAGE SQL STABLE SECURITY DEFINER;

-- Search function for admin
CREATE OR REPLACE FUNCTION search_sso_providers(query TEXT)
RETURNS SETOF sso_provider AS $$
  SELECT * FROM trex.sso_provider
  WHERE id ILIKE '%' || query || '%'
    OR "displayName" ILIKE '%' || query || '%'
    OR "clientId" ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

-- Auto-update updatedAt trigger (reuses function from 006)
DROP TRIGGER IF EXISTS trg_sso_provider_updated_at ON sso_provider;
CREATE TRIGGER trg_sso_provider_updated_at
  BEFORE UPDATE ON sso_provider
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
