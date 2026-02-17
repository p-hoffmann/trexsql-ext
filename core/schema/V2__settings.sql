SET search_path TO trex;

-- ── Settings table ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS setting (
  key TEXT PRIMARY KEY CHECK (key ~ '^[a-z][a-z0-9_.]*$'),
  value JSONB NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE setting ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_settings ON setting
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

DROP TRIGGER IF EXISTS trg_setting_updated_at ON setting;
CREATE TRIGGER trg_setting_updated_at
  BEFORE UPDATE ON setting
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ── Upsert function ────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION save_setting(
  p_key TEXT,
  p_value JSONB
) RETURNS setting AS $$
  INSERT INTO trex.setting (key, value)
  VALUES (p_key, p_value)
  ON CONFLICT (key) DO UPDATE SET
    value = EXCLUDED.value,
    "updatedAt" = NOW()
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER;

-- ── Getter with fallback ───────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION get_setting(
  p_key TEXT,
  p_default JSONB DEFAULT 'null'::JSONB
) RETURNS JSONB AS $$
  SELECT COALESCE(
    (SELECT value FROM trex.setting WHERE key = p_key),
    p_default
  );
$$ LANGUAGE SQL STABLE SECURITY DEFINER;

-- ── Seed defaults ──────────────────────────────────────────────────────────

INSERT INTO setting (key, value) VALUES
  ('auth.selfRegistration', 'false'::JSONB),
  ('runtime.functionLogging', '"console"'::JSONB)
ON CONFLICT (key) DO NOTHING;
