-- Custom extensions to Better Auth base schema

SET search_path TO trex;

ALTER TABLE "user" ADD COLUMN IF NOT EXISTS banned BOOLEAN DEFAULT false;
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS "banReason" TEXT;
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS "banExpires" TIMESTAMPTZ;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_user_email ON "user"(email);
CREATE INDEX IF NOT EXISTS idx_user_deleted_at ON "user"("deletedAt") WHERE "deletedAt" IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_account_user_id ON account("userId");
CREATE UNIQUE INDEX IF NOT EXISTS idx_account_provider ON account("providerId", "accountId");
CREATE INDEX IF NOT EXISTS idx_session_user_id ON session("userId");
CREATE INDEX IF NOT EXISTS idx_session_token ON session(token);
CREATE INDEX IF NOT EXISTS idx_session_expires_at ON session("expiresAt");
