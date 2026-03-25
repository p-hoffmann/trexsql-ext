-- Secrets table for edge function environment variables

SET search_path TO trex;

CREATE TABLE IF NOT EXISTS secret (
  name TEXT PRIMARY KEY CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_]*$'),
  value_encrypted TEXT NOT NULL,
  value_hash TEXT NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);
