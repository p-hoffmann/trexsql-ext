-- PostgREST role setup for Supabase-compatible REST API

-- Create roles (idempotent)
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'anon') THEN
    CREATE ROLE anon NOLOGIN NOINHERIT;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'authenticated') THEN
    CREATE ROLE authenticated NOLOGIN NOINHERIT;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'service_role') THEN
    CREATE ROLE service_role NOLOGIN NOINHERIT BYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'authenticator') THEN
    CREATE ROLE authenticator NOINHERIT LOGIN PASSWORD 'authenticator_pass';
  END IF;
END $$;

-- Ensure authenticator can login (idempotent fix for existing installations)
ALTER ROLE authenticator LOGIN PASSWORD 'authenticator_pass';

-- authenticator can switch to any of the API roles
GRANT anon, authenticated, service_role TO authenticator;

-- Schema access
GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;

-- public schema: anon gets SELECT, authenticated/service_role get full CRUD
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon;
GRANT ALL ON ALL TABLES IN SCHEMA public TO authenticated;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA public TO service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO service_role;

-- Default privileges for future tables created by postgres role
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO anon;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO authenticated;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO service_role;

-- Pre-request function: bridges PostgREST JWT claims into app.* session vars
-- so existing RLS policies (which check app.user_id / app.user_role) continue to work
CREATE OR REPLACE FUNCTION public.postgrest_pre_request() RETURNS void AS $$
DECLARE
  claims JSON;
BEGIN
  claims := current_setting('request.jwt.claims', true)::json;
  IF claims IS NOT NULL THEN
    PERFORM set_config('app.user_id', coalesce(claims->>'sub', ''), true);
    PERFORM set_config('app.user_role',
      CASE
        WHEN claims->>'role' = 'service_role' THEN 'admin'
        WHEN claims->'app_metadata'->>'trex_role' = 'admin' THEN 'admin'
        ELSE 'user'
      END, true);
  END IF;
END;
$$ LANGUAGE plpgsql;
