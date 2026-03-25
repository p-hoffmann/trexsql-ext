-- Supabase deployment configuration per app
ALTER TABLE devx.apps ADD COLUMN IF NOT EXISTS supabase_project_id TEXT;
ALTER TABLE devx.apps ADD COLUMN IF NOT EXISTS supabase_target VARCHAR(20) DEFAULT 'local';

-- Deployment history
CREATE TABLE IF NOT EXISTS devx.deployments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    app_id UUID NOT NULL REFERENCES devx.apps(id) ON DELETE CASCADE,
    user_id UUID NOT NULL,
    target VARCHAR(20) NOT NULL,
    target_project_id TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    steps JSONB DEFAULT '[]',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
