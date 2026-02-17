-- ── Transform Deployments ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trex.transform_deployment (
  plugin_name TEXT NOT NULL,
  dest_db TEXT NOT NULL,
  dest_schema TEXT NOT NULL,
  deployed_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (plugin_name)
);
