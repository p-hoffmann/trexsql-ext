-- Custom PostgreSQL functions for PostGraphile

SET search_path TO trex;

CREATE OR REPLACE FUNCTION current_user_id() RETURNS TEXT AS $$
  SELECT nullif(current_setting('app.user_id', true), '');
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION current_user_is_admin() RETURNS BOOLEAN AS $$
  SELECT current_setting('app.user_role', true) = 'admin';
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION soft_delete_user(target_user_id TEXT) RETURNS "user" AS $$
  UPDATE "user"
  SET "deletedAt" = NOW(), banned = true, "updatedAt" = NOW()
  WHERE id = target_user_id AND "deletedAt" IS NULL
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER;

CREATE OR REPLACE FUNCTION restore_user(target_user_id TEXT) RETURNS "user" AS $$
  UPDATE "user"
  SET "deletedAt" = NULL, banned = false, "banReason" = NULL, "updatedAt" = NOW()
  WHERE id = target_user_id AND "deletedAt" IS NOT NULL
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER;

CREATE OR REPLACE FUNCTION search_users(query TEXT)
RETURNS SETOF "user" AS $$
  SELECT * FROM "user"
  WHERE "deletedAt" IS NULL
    AND (name ILIKE '%' || query || '%' OR email ILIKE '%' || query || '%')
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION purge_deleted_users() RETURNS INTEGER AS $$
  WITH deleted AS (
    DELETE FROM "user"
    WHERE "deletedAt" IS NOT NULL
      AND "deletedAt" < NOW() - INTERVAL '30 days'
    RETURNING id
  )
  SELECT COUNT(*)::INTEGER FROM deleted;
$$ LANGUAGE SQL VOLATILE;
