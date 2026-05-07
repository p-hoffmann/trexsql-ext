-- Add RLS policies to trex.secret and tighten trex.api_key.
--
-- trex.secret previously had no RLS at all. Any role with USAGE on the schema
-- and SELECT on the table could read AES ciphertext + value_hash. Encrypted at
-- rest is good but defense-in-depth says "admin only".
--
-- trex.api_key already has admin_all_api_keys; add user-own SELECT so users
-- can list/revoke their own keys without escalating to admin. PostgREST runs
-- under the `service_role` role which is BYPASSRLS, so its access is unaffected.

SET search_path TO trex;

-- ── trex.secret ──────────────────────────────────────────────────────────────

ALTER TABLE trex.secret ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS admin_all_secret ON trex.secret;
CREATE POLICY admin_all_secret ON trex.secret
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

-- ── trex.api_key ─────────────────────────────────────────────────────────────
-- admin_all_api_keys already exists from V1. Add a user-scoped policy so a
-- non-admin can manage their own keys.

DROP POLICY IF EXISTS user_own_api_key ON trex.api_key;
CREATE POLICY user_own_api_key ON trex.api_key
  USING ("userId" = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK ("userId" = nullif(current_setting('app.user_id', true), ''));
