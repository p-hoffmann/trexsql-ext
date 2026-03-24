-- Fix authenticator role LOGIN permission (may have been created without LOGIN by older migration path)
ALTER ROLE authenticator LOGIN PASSWORD 'authenticator_pass';
