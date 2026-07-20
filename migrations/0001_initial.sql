PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schedules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    encrypted_snapshot BLOB NOT NULL,
    snapshot_digest TEXT NOT NULL,
    key_id TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    cron_expression TEXT,
    cron_timezone TEXT,
    last_cron_at TEXT,
    webhook_enabled INTEGER NOT NULL DEFAULT 0,
    webhook_public_id TEXT UNIQUE,
    webhook_secret_hash TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    state TEXT NOT NULL,
    trigger_kind TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    not_before TEXT NOT NULL,
    encrypted_snapshot BLOB NOT NULL,
    key_id TEXT NOT NULL,
    max_attempts INTEGER NOT NULL,
    initial_backoff_seconds INTEGER NOT NULL,
    backoff_cap_seconds INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_cron_occurrence
    ON runs(schedule_id, scheduled_at) WHERE trigger_kind = 'cron';
CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_webhook_idempotency
    ON runs(schedule_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_runs_dispatch
    ON runs(state, not_before, created_at);

CREATE TABLE IF NOT EXISTS attempts (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id),
    agent_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    lease_token TEXT NOT NULL,
    state TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    accepted_at TEXT,
    started_at TEXT,
    finished_at TEXT,
    encrypted_result BLOB,
    result_key_id TEXT,
    outcome TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(run_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_attempts_lease
    ON attempts(state, lease_expires_at);

CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    labels_json TEXT NOT NULL,
    capacity INTEGER NOT NULL,
    running INTEGER NOT NULL DEFAULT 0,
    connected INTEGER NOT NULL DEFAULT 0,
    desired_settings_revision INTEGER NOT NULL DEFAULT 1,
    applied_settings_revision INTEGER NOT NULL DEFAULT 0,
    settings_error TEXT,
    last_seen_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings_documents (
    document_key TEXT PRIMARY KEY,
    document_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS edit_locks (
    document_key TEXT PRIMARY KEY,
    owner_session TEXT NOT NULL,
    lock_token TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_entity
    ON audit_events(entity_type, entity_id, id);
