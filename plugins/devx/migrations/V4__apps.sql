-- Apps table: each user can have multiple apps/projects
CREATE TABLE IF NOT EXISTS devx.apps (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    name VARCHAR(500) NOT NULL,
    path VARCHAR(1000) NOT NULL,
    tech_stack VARCHAR(100),
    dev_command VARCHAR(500) DEFAULT 'npm run dev',
    install_command VARCHAR(500) DEFAULT 'npm install',
    build_command VARCHAR(500) DEFAULT 'npm run build',
    dev_port INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_devx_apps_user_id ON devx.apps(user_id);

-- Link chats to apps (nullable for backward compatibility)
ALTER TABLE devx.chats ADD COLUMN IF NOT EXISTS app_id UUID REFERENCES devx.apps(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_devx_chats_app_id ON devx.chats(app_id);
