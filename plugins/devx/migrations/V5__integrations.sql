-- Encrypted credential storage for integrations
CREATE TABLE IF NOT EXISTS devx.integrations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    provider VARCHAR(100) NOT NULL,
    name VARCHAR(200) NOT NULL,
    encrypted_token TEXT,
    token_iv TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, provider, name)
);
CREATE INDEX IF NOT EXISTS idx_devx_integrations_user ON devx.integrations(user_id, provider);

-- MCP server configurations
CREATE TABLE IF NOT EXISTS devx.mcp_servers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    name VARCHAR(200) NOT NULL,
    transport VARCHAR(20) NOT NULL CHECK (transport IN ('stdio', 'http')),
    command TEXT,
    args JSONB DEFAULT '[]',
    env JSONB DEFAULT '{}',
    url TEXT,
    headers JSONB DEFAULT '{}',
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, name)
);

-- MCP tool consent (separate from built-in tool consents)
CREATE TABLE IF NOT EXISTS devx.mcp_tool_consents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    server_name VARCHAR(200) NOT NULL,
    tool_name VARCHAR(200) NOT NULL,
    consent VARCHAR(20) NOT NULL DEFAULT 'ask'
        CHECK (consent IN ('always', 'ask', 'never')),
    UNIQUE(user_id, server_name, tool_name)
);

-- Git metadata on apps
ALTER TABLE devx.apps ADD COLUMN IF NOT EXISTS git_remote_url TEXT;
ALTER TABLE devx.apps ADD COLUMN IF NOT EXISTS git_default_branch VARCHAR(100) DEFAULT 'main';

-- Per-app database schemas
CREATE TABLE IF NOT EXISTS devx.app_databases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    app_id UUID NOT NULL REFERENCES devx.apps(id) ON DELETE CASCADE,
    schema_name VARCHAR(200) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
