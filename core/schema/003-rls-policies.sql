-- Row Level Security policies

SET search_path TO trex;

ALTER TABLE "user" ENABLE ROW LEVEL SECURITY;
ALTER TABLE account ENABLE ROW LEVEL SECURITY;
ALTER TABLE session ENABLE ROW LEVEL SECURITY;

-- User table policies
CREATE POLICY admin_all_users ON "user"
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_row ON "user"
  FOR ALL
  USING (id = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK (id = nullif(current_setting('app.user_id', true), ''));

-- Account table policies
CREATE POLICY admin_all_accounts ON account
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_accounts ON account
  FOR SELECT
  USING ("userId" = nullif(current_setting('app.user_id', true), ''));

-- Session table policies
CREATE POLICY admin_all_sessions ON session
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_sessions ON session
  FOR ALL
  USING ("userId" = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK ("userId" = nullif(current_setting('app.user_id', true), ''));
