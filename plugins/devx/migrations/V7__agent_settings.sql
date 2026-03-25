-- Phase 3: Agent configuration columns on settings table
ALTER TABLE devx.settings ADD COLUMN IF NOT EXISTS auto_approve BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE devx.settings ADD COLUMN IF NOT EXISTS max_steps INTEGER NOT NULL DEFAULT 25;
ALTER TABLE devx.settings ADD COLUMN IF NOT EXISTS max_tool_steps INTEGER NOT NULL DEFAULT 10;
ALTER TABLE devx.settings ADD COLUMN IF NOT EXISTS auto_fix_problems BOOLEAN NOT NULL DEFAULT false;
