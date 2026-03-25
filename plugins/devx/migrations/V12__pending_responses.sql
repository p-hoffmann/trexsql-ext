-- Shared table for cross-worker blocking requests (questionnaire, consent).
-- The SSE stream worker polls for the answer; the answer endpoint writes it.
CREATE TABLE IF NOT EXISTS devx.pending_responses (
  request_id  TEXT PRIMARY KEY,
  chat_id     UUID NOT NULL REFERENCES devx.chats(id) ON DELETE CASCADE,
  user_id     TEXT NOT NULL,
  kind        TEXT NOT NULL,          -- 'questionnaire' or 'consent'
  answer      JSONB,                  -- NULL until answered
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for polling by request_id (primary key already covers this)
-- Index for cleanup by chat_id
CREATE INDEX IF NOT EXISTS idx_pending_responses_chat ON devx.pending_responses (chat_id);

-- Auto-expire old entries (cleanup via app logic, but also a safety net)
