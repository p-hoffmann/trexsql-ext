-- Hide trex.setting from auto-generated PostGraphile schema.
-- The setting table holds auth.serviceRoleKey / auth.anonKey. The PostGraphile
-- pool currently runs as a role that bypasses RLS, so without @omit anyone
-- reachable to /graphql could read these via auto-CRUD.
COMMENT ON TABLE trex.setting IS E'@omit';
