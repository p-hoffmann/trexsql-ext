SET search_path TO trex;

-- ── Extend dashboard language constraint ──────────────────────────────────────

ALTER TABLE dashboard DROP CONSTRAINT dashboard_language_check;
ALTER TABLE dashboard ADD CONSTRAINT dashboard_language_check
  CHECK (language IN ('python', 'r', 'markdown'));
