-- Encrypt trex.database_credential.password at rest.
--
-- Approach: an explicit decrypt step in the application reads the credential
-- via decryptSecret() (AES-256-GCM with HKDF over BETTER_AUTH_SECRET, salt
-- "trex-secrets"). We do NOT push decryption into Postgres or pgwire — pgwire
-- is given already-decrypted credentials by the application before it starts.
--
-- This migration adds password_encrypted, makes the plaintext password
-- nullable, and leaves save_database_credential() unchanged so existing GraphQL
-- callers continue to write the plaintext column. A bootstrap step in
-- core/server/index.ts runs on every startup and encrypts any rows where
-- password_encrypted IS NULL but password IS NOT NULL — moving freshly written
-- plaintext into ciphertext within milliseconds of insertion. Reader code
-- prefers password_encrypted and falls back to plaintext only during the
-- transient window before bootstrap runs.

SET search_path TO trex;

ALTER TABLE trex.database_credential
  ADD COLUMN IF NOT EXISTS password_encrypted TEXT;

ALTER TABLE trex.database_credential
  ALTER COLUMN password DROP NOT NULL;

COMMENT ON COLUMN trex.database_credential.password IS E'@omit\nLegacy plaintext column; the bootstrap encrypt step nulls this out after copying into password_encrypted. Do not write directly.';
COMMENT ON COLUMN trex.database_credential.password_encrypted IS E'@omit\nAES-256-GCM ciphertext (base64 iv||ct). Decrypt via core/server/auth/crypto.ts decryptSecret().';
