-- Application roles (separate from Better Auth platform roles)

SET search_path TO trex;

-- Application roles table
CREATE TABLE IF NOT EXISTS role (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

-- Many-to-many join between users and application roles
CREATE TABLE IF NOT EXISTS user_role (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  "roleId" TEXT NOT NULL REFERENCES role(id) ON DELETE CASCADE,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE ("userId", "roleId")
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_user_role_user_id ON user_role("userId");
CREATE INDEX IF NOT EXISTS idx_user_role_role_id ON user_role("roleId");

-- RLS
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

-- Search function for PostGraphile
CREATE OR REPLACE FUNCTION search_roles(query TEXT)
RETURNS SETOF role AS $$
  SELECT * FROM role
  WHERE name ILIKE '%' || query || '%'
    OR description ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

-- Auto-update updatedAt trigger (reuses update_updated_at_column from 006)
DROP TRIGGER IF EXISTS trg_role_updated_at ON role;
CREATE TRIGGER trg_role_updated_at
  BEFORE UPDATE ON role
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
