-- Add ai_rules to settings, ensure mode CHECK covers all 4 modes

ALTER TABLE devx.settings ADD COLUMN IF NOT EXISTS ai_rules TEXT;

-- Ensure no NULL mode values before adding constraint
UPDATE devx.chats SET mode = 'build' WHERE mode IS NULL;

-- Update mode constraint to include agent and plan
ALTER TABLE devx.chats DROP CONSTRAINT IF EXISTS chats_mode_check;
ALTER TABLE devx.chats ADD CONSTRAINT chats_mode_check
  CHECK (mode IN ('build', 'ask', 'agent', 'plan'));
