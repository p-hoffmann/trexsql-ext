-- Remove the seeded admin@trex.local user if it still has the default seed
-- credential and has never signed in. Production deployments should rely on
-- ADMIN_EMAIL or first-user promotion to bootstrap admin.

SET search_path TO trex;

-- The seed account row from V1 (id 00000000-0000-0000-0000-000000000002,
-- providerId 'credential') links to user 00000000-0000-0000-0000-000000000001.
-- Only delete the user if no session has ever been recorded for them.

DO $$
DECLARE
  v_user_id TEXT := '00000000-0000-0000-0000-000000000001';
  v_account_id TEXT := '00000000-0000-0000-0000-000000000002';
  v_seed_password TEXT := 'a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6:0c219b9e9260518faacd40f6bbaf04d86622631a745d5a177a0f8ff18363b52ed0f56a940c02a8bf0bb314b293d8ccfd5383e537bd18f445bb31f5bc979a020f';
  v_session_count INTEGER;
  v_password_unchanged BOOLEAN;
BEGIN
  SELECT COUNT(*) INTO v_session_count FROM trex.session WHERE "userId" = v_user_id;
  SELECT EXISTS (
    SELECT 1 FROM trex.account
    WHERE id = v_account_id AND "userId" = v_user_id AND password = v_seed_password
  ) INTO v_password_unchanged;

  IF v_session_count = 0 AND v_password_unchanged THEN
    DELETE FROM trex.account WHERE "userId" = v_user_id;
    DELETE FROM trex."user" WHERE id = v_user_id AND email = 'admin@trex.local';
    RAISE NOTICE '[V10] Removed seeded admin@trex.local (never used).';
  ELSE
    RAISE NOTICE '[V10] Seeded admin@trex.local kept (sessions=%, password_unchanged=%).',
      v_session_count, v_password_unchanged;
  END IF;
END $$;
