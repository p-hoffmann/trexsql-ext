-- Analytics dashboards (Shinylive editors)
SET search_path TO trex;

-- ── Dashboard table ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dashboard (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name TEXT NOT NULL,
  language TEXT NOT NULL CHECK (language IN ('python', 'r')),
  code TEXT NOT NULL DEFAULT '',
  "userId" TEXT NOT NULL DEFAULT nullif(current_setting('app.user_id', true), '') REFERENCES "user"(id) ON DELETE CASCADE,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dashboard_user_id ON dashboard("userId");

-- ── Row Level Security ──────────────────────────────────────────────────────

ALTER TABLE dashboard ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_dashboards ON dashboard
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_dashboards ON dashboard
  FOR ALL
  USING ("userId" = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK ("userId" = nullif(current_setting('app.user_id', true), ''));

COMMENT ON COLUMN dashboard."userId" IS E'@omit create,update';

-- ── Trigger ─────────────────────────────────────────────────────────────────

DROP TRIGGER IF EXISTS trg_dashboard_updated_at ON dashboard;
CREATE TRIGGER trg_dashboard_updated_at
  BEFORE UPDATE ON dashboard
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ── Search ──────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION search_dashboards(query TEXT)
RETURNS SETOF dashboard AS $$
  SELECT * FROM dashboard
  WHERE name ILIKE '%' || query || '%'
    OR language ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;
