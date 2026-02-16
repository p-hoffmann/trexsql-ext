-- Database management tables for external database connections

SET search_path TO trex;

-- External database connection info
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

-- Credentials per database (normalizes d2e JSONB array)
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

-- Indexes
CREATE INDEX IF NOT EXISTS idx_database_credential_database_id ON database_credential("databaseId");

-- RLS
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

-- Smart comments: hide password from GraphQL
COMMENT ON COLUMN database_credential.password IS E'@omit';

-- Search function for PostGraphile
CREATE OR REPLACE FUNCTION search_databases(query TEXT)
RETURNS SETOF database AS $$
  SELECT * FROM database
  WHERE id ILIKE '%' || query || '%'
    OR host ILIKE '%' || query || '%'
    OR "databaseName" ILIKE '%' || query || '%'
    OR description ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

-- Save credential function (accepts password, which @omit hides from auto mutations)
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

-- Auto-update updatedAt triggers
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW."updatedAt" = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_database_updated_at ON database;
CREATE TRIGGER trg_database_updated_at
  BEFORE UPDATE ON database
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS trg_database_credential_updated_at ON database_credential;
CREATE TRIGGER trg_database_credential_updated_at
  BEFORE UPDATE ON database_credential
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
