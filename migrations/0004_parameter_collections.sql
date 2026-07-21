PRAGMA foreign_keys = ON;

-- One durable identity spans legacy one-run triggers and collection batches.
-- This closes the cron/webhook idempotency gap when a schedule changes mode.
CREATE TABLE trigger_identities (
    id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    trigger_kind TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    idempotency_key TEXT,
    target_kind TEXT NOT NULL CHECK(target_kind IN ('run', 'batch')),
    target_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(target_kind, target_id)
);

CREATE UNIQUE INDEX idx_trigger_identity_cron
    ON trigger_identities(schedule_id, scheduled_at)
    WHERE trigger_kind = 'cron';
CREATE UNIQUE INDEX idx_trigger_identity_idempotency
    ON trigger_identities(schedule_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Backfill all existing logical triggers before new collection batches can be
-- created. randomblob is sufficient here because these IDs are internal only.
INSERT INTO trigger_identities(
    id, schedule_id, trigger_kind, scheduled_at, idempotency_key,
    target_kind, target_id, created_at
)
SELECT lower(hex(randomblob(16))), schedule_id, trigger_kind, scheduled_at,
       idempotency_key, 'run', id, created_at
FROM runs;

CREATE TABLE batches (
    id TEXT PRIMARY KEY,
    trigger_identity_id TEXT NOT NULL UNIQUE REFERENCES trigger_identities(id),
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    schedule_revision INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN (
        'scheduled', 'collecting', 'running', 'succeeded',
        'completed_with_errors', 'failed', 'cancelled'
    )),
    trigger_kind TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    encrypted_snapshot BLOB NOT NULL,
    encrypted_trigger_overrides BLOB,
    snapshot_digest TEXT NOT NULL,
    key_id TEXT NOT NULL,
    page_size INTEGER NOT NULL CHECK(page_size BETWEEN 1 AND 1000),
    max_items INTEGER NOT NULL CHECK(max_items BETWEEN 1 AND 10000),
    max_active_runs INTEGER NOT NULL CHECK(max_active_runs BETWEEN 1 AND 1000),
    poison_distinct_nodes INTEGER NOT NULL CHECK(poison_distinct_nodes BETWEEN 2 AND 32),
    collection_snapshot_encrypted BLOB,
    collection_snapshot_digest TEXT,
    next_cursor_encrypted BLOB,
    next_cursor_digest TEXT NOT NULL DEFAULT 'start',
    cursor_generation INTEGER NOT NULL DEFAULT 0,
    ingestion_complete INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER NOT NULL DEFAULT 0,
    valid_item_count INTEGER NOT NULL DEFAULT 0,
    invalid_item_count INTEGER NOT NULL DEFAULT 0,
    poisoned_item_count INTEGER NOT NULL DEFAULT 0,
    held_item_count INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at TEXT,
    failure_code TEXT,
    finalized_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_batches_collection
    ON batches(state, lease_expires_at, created_at);
CREATE INDEX idx_batches_schedule
    ON batches(schedule_id, created_at DESC);

CREATE TABLE batch_collection_pages (
    batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    request_cursor_digest TEXT NOT NULL,
    page_digest TEXT NOT NULL,
    collection_snapshot_digest TEXT NOT NULL,
    next_cursor_encrypted BLOB,
    next_cursor_digest TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    is_final INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(batch_id, generation),
    UNIQUE(batch_id, request_cursor_digest)
);

CREATE TABLE batch_items (
    id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    item_index INTEGER NOT NULL,
    provider_key_encrypted BLOB NOT NULL,
    provider_key_hmac TEXT NOT NULL,
    encrypted_parameters BLOB NOT NULL,
    encrypted_snapshot BLOB,
    key_id TEXT NOT NULL,
    parameters_digest TEXT NOT NULL,
    max_attempts INTEGER,
    initial_backoff_seconds INTEGER,
    backoff_cap_seconds INTEGER,
    state TEXT NOT NULL CHECK(state IN (
        'ready', 'queued', 'running', 'succeeded', 'failed', 'cancelled',
        'invalid', 'suspected_poison', 'poisoned', 'held'
    )),
    failure_code TEXT,
    run_id TEXT UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(batch_id, item_index)
);

CREATE INDEX idx_batch_items_state
    ON batch_items(batch_id, state, item_index);
CREATE INDEX idx_batch_items_provider_key
    ON batch_items(batch_id, provider_key_hmac);

ALTER TABLE runs ADD COLUMN batch_id TEXT REFERENCES batches(id);
ALTER TABLE runs ADD COLUMN batch_item_id TEXT REFERENCES batch_items(id);
CREATE UNIQUE INDEX idx_runs_batch_item
    ON runs(batch_item_id) WHERE batch_item_id IS NOT NULL;
CREATE INDEX idx_runs_batch_state
    ON runs(batch_id, state, not_before) WHERE batch_id IS NOT NULL;
