-- Todos for agent task tracking

CREATE TABLE IF NOT EXISTS devx.todos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_id UUID NOT NULL REFERENCES devx.chats(id) ON DELETE CASCADE,
    todo_id VARCHAR(100) NOT NULL,
    content TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'in_progress', 'completed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chat_id, todo_id)
);

CREATE INDEX IF NOT EXISTS idx_devx_todos_chat_id ON devx.todos(chat_id);

-- Per-user tool consent preferences

CREATE TABLE IF NOT EXISTS devx.tool_consents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    tool_name VARCHAR(100) NOT NULL,
    consent VARCHAR(20) NOT NULL DEFAULT 'ask'
        CHECK (consent IN ('always', 'ask', 'never')),
    UNIQUE(user_id, tool_name)
);
