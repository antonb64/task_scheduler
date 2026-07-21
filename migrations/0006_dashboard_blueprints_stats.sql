PRAGMA foreign_keys = ON;

INSERT OR IGNORE INTO settings_documents(document_key, document_json, revision, updated_at)
VALUES (
    'dashboard',
    '{"schedule_ids":[],"widgets":["cluster_capacity","active_batches","recent_failures","quarantined_nodes","connector_health","telemetry_health","selected_schedules"]}',
    1,
    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
);

CREATE TABLE IF NOT EXISTS blueprint_revisions (
    digest TEXT PRIMARY KEY,
    source_ref TEXT NOT NULL,
    source_version TEXT,
    loaded_at TEXT NOT NULL,
    executor_kind TEXT NOT NULL,
    required_labels_json TEXT NOT NULL,
    execution_policy_json TEXT NOT NULL,
    parameter_schema_json TEXT NOT NULL,
    binding_declarations_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedule_blueprint_revisions (
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    schedule_revision INTEGER NOT NULL,
    blueprint_digest TEXT NOT NULL REFERENCES blueprint_revisions(digest),
    is_current INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    PRIMARY KEY(schedule_id, schedule_revision)
);

CREATE INDEX IF NOT EXISTS idx_schedule_blueprint_current
    ON schedule_blueprint_revisions(schedule_id, is_current);

CREATE TABLE IF NOT EXISTS resolved_snapshot_blueprints (
    snapshot_digest TEXT PRIMARY KEY,
    blueprint_digest TEXT NOT NULL REFERENCES blueprint_revisions(digest),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_schedule_stats (
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    day_utc TEXT NOT NULL,
    succeeded INTEGER NOT NULL DEFAULT 0,
    business_failed INTEGER NOT NULL DEFAULT 0,
    infrastructure_failed INTEGER NOT NULL DEFAULT 0,
    cancelled INTEGER NOT NULL DEFAULT 0,
    retries INTEGER NOT NULL DEFAULT 0,
    duration_count INTEGER NOT NULL DEFAULT 0,
    duration_sum_ms INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(schedule_id, day_utc)
);

CREATE INDEX IF NOT EXISTS idx_runs_schedule_updated_state
    ON runs(schedule_id, updated_at, state, id);

CREATE INDEX IF NOT EXISTS idx_attempts_run_finished
    ON attempts(run_id, finished_at, created_at, id);

CREATE INDEX IF NOT EXISTS idx_batches_schedule_updated
    ON batches(schedule_id, updated_at, id);
