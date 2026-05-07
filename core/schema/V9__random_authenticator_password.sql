-- Randomize the authenticator role password (phase 1 — store-only).
--
-- Phase 1 SCOPE: generate a deterministic-but-random-per-deploy password and
-- persist it in trex.setting under key auth.authenticator_password. The
-- PostgreSQL role itself is NOT yet rotated because PostgREST is an external
-- container that still consumes the V3 hardcoded password via PGRST_DB_URI.
-- Wiring docker-compose / PostgREST to read this value is a follow-up.
--
-- Once the operator has updated PGRST_DB_URI to read the new value, a follow-up
-- migration can switch the role password by ALTERing the authenticator role
-- to match. Until then, the V3 password remains authoritative for the role
-- but is dead from a "secret rotation" perspective: the value in trex.setting
-- is the canonical new password, ready to be activated.
--
-- This migration is idempotent: re-running it preserves the previously
-- generated password rather than rotating it on every server restart.

SET search_path TO trex;

DO $$
DECLARE
  v_pass TEXT;
BEGIN
  SELECT (value #>> '{}')::TEXT INTO v_pass
  FROM trex.setting WHERE key = 'auth.authenticator_password';

  IF v_pass IS NULL THEN
    -- gen_random_uuid() is core; concatenate two for ~256 bits of entropy.
    v_pass := replace(gen_random_uuid()::text, '-', '') ||
              replace(gen_random_uuid()::text, '-', '');
    INSERT INTO trex.setting (key, value)
    VALUES ('auth.authenticator_password', to_jsonb(v_pass))
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, "updatedAt" = NOW();
    RAISE NOTICE '[V9] Generated new authenticator password (stored in trex.setting). Update PGRST_DB_URI to consume it, then ALTER ROLE authenticator with the new value.';
  ELSE
    RAISE NOTICE '[V9] auth.authenticator_password already present; skipping generation.';
  END IF;
END $$;
