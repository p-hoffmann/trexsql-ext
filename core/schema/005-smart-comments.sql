-- PostGraphile smart comments to hide sensitive columns/tables from GraphQL

SET search_path TO trex;

-- Hide sensitive account columns
COMMENT ON COLUMN account.password IS E'@omit';
COMMENT ON COLUMN account."accessToken" IS E'@omit';
COMMENT ON COLUMN account."refreshToken" IS E'@omit';
COMMENT ON COLUMN account."idToken" IS E'@omit';

-- Hide entire verification table
COMMENT ON TABLE verification IS E'@omit';

-- Hide JWT keys table
COMMENT ON TABLE jwks IS E'@omit';

-- Hide OAuth internal tables
COMMENT ON TABLE oauth_access_token IS E'@omit';
COMMENT ON TABLE oauth_authorization_code IS E'@omit';
COMMENT ON TABLE oauth_consent IS E'@omit';

-- Hide client_secret from oauth_application
COMMENT ON COLUMN oauth_application."clientSecret" IS E'@omit';
