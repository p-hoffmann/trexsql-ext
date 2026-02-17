-- PostGraphile LISTEN/NOTIFY subscriptions

SET search_path TO trex;

-- ── Subscription table ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS subscription (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name TEXT NOT NULL UNIQUE CHECK (name ~ '^[a-z][a-z0-9_]*$'),
  topic TEXT NOT NULL,
  "sourceTable" TEXT NOT NULL,
  events TEXT[] NOT NULL DEFAULT '{INSERT,UPDATE,DELETE}',
  description TEXT,
  enabled BOOLEAN DEFAULT true,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE subscription ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_subscriptions ON subscription
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

COMMENT ON TABLE subscription IS E'@name notifySubscription\n@omit create,update,delete';

DROP TRIGGER IF EXISTS trg_subscription_updated_at ON subscription;
CREATE TRIGGER trg_subscription_updated_at
  BEFORE UPDATE ON subscription
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ── Search ──────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION search_subscriptions(query TEXT)
RETURNS SETOF subscription AS $$
  SELECT * FROM trex.subscription
  WHERE name ILIKE '%' || query || '%'
    OR topic ILIKE '%' || query || '%'
    OR "sourceTable" ILIKE '%' || query || '%'
    OR description ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

-- ── Save (idempotent upsert + trigger management) ───────────────────────────

CREATE OR REPLACE FUNCTION save_subscription(
  p_name TEXT,
  p_topic TEXT,
  p_source_table TEXT,
  p_events TEXT[],
  p_description TEXT DEFAULT NULL,
  p_enabled BOOLEAN DEFAULT true
) RETURNS subscription AS $$
DECLARE
  v_event TEXT;
  v_func_name TEXT;
  v_trigger_name TEXT;
  v_event_clause TEXT;
  v_table_oid regclass;
  v_result trex.subscription;
BEGIN
  FOREACH v_event IN ARRAY p_events LOOP
    IF v_event NOT IN ('INSERT', 'UPDATE', 'DELETE') THEN
      RAISE EXCEPTION 'Invalid event: %. Must be INSERT, UPDATE, or DELETE.', v_event;
    END IF;
  END LOOP;

  -- Validates table exists; returns safe quoted identifier
  v_table_oid := p_source_table::regclass;

  v_func_name := 'trex.trex_sub_notify_' || p_name;
  v_trigger_name := 'trg_sub_' || p_name;

  BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger_name, v_table_oid::TEXT);
  EXCEPTION WHEN undefined_table THEN
    NULL;
  END;

  -- Drop trigger from previous source table if it changed
  BEGIN
    PERFORM 1 FROM trex.subscription WHERE name = p_name AND "sourceTable" != p_source_table;
    IF FOUND THEN
      DECLARE
        v_old_table regclass;
      BEGIN
        SELECT "sourceTable"::regclass INTO v_old_table FROM trex.subscription WHERE name = p_name;
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger_name, v_old_table::TEXT);
      EXCEPTION WHEN undefined_table OR invalid_text_representation THEN
        NULL;
      END;
    END IF;
  END;

  EXECUTE format('DROP FUNCTION IF EXISTS %s() CASCADE', v_func_name);

  IF p_enabled THEN
    EXECUTE format(
      $fn$
      CREATE OR REPLACE FUNCTION %s() RETURNS TRIGGER AS $trg$
      DECLARE
        v_payload JSONB;
        v_id TEXT;
      BEGIN
        IF TG_OP = 'DELETE' THEN
          v_id := OLD.id::TEXT;
        ELSE
          v_id := NEW.id::TEXT;
        END IF;

        v_payload := jsonb_build_object(
          'event', TG_OP,
          'table', TG_TABLE_NAME,
          'schema', TG_TABLE_SCHEMA,
          'id', v_id
        );

        PERFORM pg_notify('postgraphile:' || %L, v_payload::TEXT);

        IF TG_OP = 'DELETE' THEN
          RETURN OLD;
        ELSE
          RETURN NEW;
        END IF;
      END;
      $trg$ LANGUAGE plpgsql
      $fn$,
      v_func_name, p_topic
    );

    v_event_clause := array_to_string(p_events, ' OR ');

    EXECUTE format(
      'CREATE TRIGGER %I AFTER %s ON %s FOR EACH ROW EXECUTE FUNCTION %s()',
      v_trigger_name, v_event_clause, v_table_oid::TEXT, v_func_name
    );
  END IF;

  INSERT INTO trex.subscription (name, topic, "sourceTable", events, description, enabled)
  VALUES (p_name, p_topic, p_source_table, p_events, p_description, p_enabled)
  ON CONFLICT (name) DO UPDATE SET
    topic = EXCLUDED.topic,
    "sourceTable" = EXCLUDED."sourceTable",
    events = EXCLUDED.events,
    description = EXCLUDED.description,
    enabled = EXCLUDED.enabled,
    "updatedAt" = NOW()
  RETURNING * INTO v_result;

  RETURN v_result;
END;
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

-- ── Delete ──────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION remove_subscription(p_name TEXT) RETURNS subscription AS $$
DECLARE
  v_sub trex.subscription;
  v_func_name TEXT;
  v_trigger_name TEXT;
  v_table_oid regclass;
BEGIN
  SELECT * INTO v_sub FROM trex.subscription WHERE name = p_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Subscription "%" not found.', p_name;
  END IF;

  v_func_name := 'trex.trex_sub_notify_' || p_name;
  v_trigger_name := 'trg_sub_' || p_name;

  BEGIN
    v_table_oid := v_sub."sourceTable"::regclass;
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger_name, v_table_oid::TEXT);
  EXCEPTION WHEN undefined_table OR invalid_text_representation THEN
    NULL;
  END;

  EXECUTE format('DROP FUNCTION IF EXISTS %s() CASCADE', v_func_name);

  DELETE FROM trex.subscription WHERE name = p_name;

  RETURN v_sub;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER;
