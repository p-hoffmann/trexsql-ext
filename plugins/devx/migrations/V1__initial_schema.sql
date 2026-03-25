-- DevX initial schema: chats, messages, settings

CREATE TABLE IF NOT EXISTS devx.chats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    title VARCHAR(500) NOT NULL DEFAULT 'New Chat',
    mode VARCHAR(20) NOT NULL DEFAULT 'build',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_devx_chats_user_id ON devx.chats(user_id);
CREATE INDEX IF NOT EXISTS idx_devx_chats_updated_at ON devx.chats(updated_at DESC);

CREATE TABLE IF NOT EXISTS devx.messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_id UUID NOT NULL REFERENCES devx.chats(id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    model VARCHAR(200),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_devx_messages_chat_id ON devx.messages(chat_id);
CREATE INDEX IF NOT EXISTS idx_devx_messages_created_at ON devx.messages(chat_id, created_at);

CREATE TABLE IF NOT EXISTS devx.settings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL UNIQUE,
    provider VARCHAR(100) NOT NULL DEFAULT 'anthropic',
    model VARCHAR(200) NOT NULL DEFAULT 'claude-sonnet-4-20250514',
    api_key TEXT,
    base_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
