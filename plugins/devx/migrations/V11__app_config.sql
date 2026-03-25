-- Template-specific configuration (e.g. WebAPI URL for Atlas, dataset ID for D2E)
ALTER TABLE devx.apps ADD COLUMN IF NOT EXISTS config JSONB DEFAULT '{}'::jsonb;
