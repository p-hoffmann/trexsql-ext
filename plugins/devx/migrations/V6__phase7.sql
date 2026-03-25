-- Plans (for Plan Mode)
CREATE TABLE IF NOT EXISTS devx.plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_id UUID NOT NULL REFERENCES devx.chats(id) ON DELETE CASCADE,
    content TEXT NOT NULL DEFAULT '',
    status VARCHAR(20) NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'accepted', 'rejected')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chat_id)
);
CREATE INDEX IF NOT EXISTS idx_devx_plans_chat ON devx.plans(chat_id);

-- Context compaction summaries
CREATE TABLE IF NOT EXISTS devx.compacted_contexts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_id UUID NOT NULL REFERENCES devx.chats(id) ON DELETE CASCADE,
    summary TEXT NOT NULL,
    messages_before INT NOT NULL,
    last_message_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_devx_compacted_chat ON devx.compacted_contexts(chat_id);

-- Custom prompt templates
CREATE TABLE IF NOT EXISTS devx.prompt_templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    name VARCHAR(200) NOT NULL,
    content TEXT NOT NULL,
    category VARCHAR(100) DEFAULT 'general',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_devx_prompts_user ON devx.prompt_templates(user_id);

-- Custom AI providers
CREATE TABLE IF NOT EXISTS devx.custom_providers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    name VARCHAR(200) NOT NULL,
    base_url TEXT NOT NULL,
    api_key TEXT,
    models JSONB DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(user_id, name)
);

-- File attachments
CREATE TABLE IF NOT EXISTS devx.attachments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id UUID NOT NULL REFERENCES devx.messages(id) ON DELETE CASCADE,
    chat_id UUID NOT NULL,
    filename VARCHAR(500) NOT NULL,
    content_type VARCHAR(200) NOT NULL,
    size_bytes BIGINT NOT NULL,
    storage_path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_devx_attachments_msg ON devx.attachments(message_id);

-- Token usage tracking on messages
ALTER TABLE devx.messages ADD COLUMN IF NOT EXISTS token_input INT;
ALTER TABLE devx.messages ADD COLUMN IF NOT EXISTS token_output INT;

-- Theme preference on settings
ALTER TABLE devx.settings ADD COLUMN IF NOT EXISTS theme VARCHAR(20) DEFAULT 'system';
