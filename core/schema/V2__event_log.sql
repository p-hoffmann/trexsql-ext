-- Event log table for persisting runtime event logs

SET search_path TO trex;

CREATE TABLE IF NOT EXISTS event_log (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_type VARCHAR(50) NOT NULL DEFAULT 'Log',
  level VARCHAR(20) NOT NULL DEFAULT 'Info',
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_event_log_created_at ON event_log (created_at DESC);
CREATE INDEX idx_event_log_level ON event_log (level);
