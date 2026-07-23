PRAGMA foreign_keys = ON;

-- The effective operations boundary is captured once per logical trigger.
-- Existing history cannot be reconstructed safely after schedule edits, so
-- migrated rows are deliberately marked as inferred.
ALTER TABLE trigger_identities ADD COLUMN operations_timezone TEXT;
ALTER TABLE trigger_identities ADD COLUMN operations_day TEXT;
ALTER TABLE trigger_identities ADD COLUMN completion_deadline_at TEXT;
ALTER TABLE trigger_identities ADD COLUMN observability_coverage_complete INTEGER NOT NULL DEFAULT 0;
ALTER TABLE trigger_identities ADD COLUMN traceparent TEXT;
ALTER TABLE trigger_identities ADD COLUMN tracestate TEXT;
ALTER TABLE batches ADD COLUMN observability_traceparent TEXT;
ALTER TABLE batches ADD COLUMN observability_tracestate TEXT;

UPDATE trigger_identities
SET operations_timezone='UTC',
    operations_day=substr(scheduled_at,1,10),
    completion_deadline_at=strftime('%Y-%m-%dT%H:%M:%fZ', scheduled_at, '+1 day')
WHERE operations_timezone IS NULL;

CREATE INDEX idx_trigger_operations_day
    ON trigger_identities(schedule_id, operations_day, scheduled_at);
CREATE INDEX idx_trigger_deadline
    ON trigger_identities(completion_deadline_at, target_kind);

-- Backward-compatible persisted defaults for both existing and fresh databases.
UPDATE settings_documents
SET document_json=json_set(
        document_json,
        '$.default_completion_deadline_seconds',
        COALESCE(json_extract(document_json, '$.default_completion_deadline_seconds'), 86400)
    )
WHERE document_key='global';

UPDATE schedules
SET spec_json=json_set(
        spec_json,
        '$.observability.completion_deadline_seconds',
        COALESCE(
            json_extract(spec_json, '$.observability.completion_deadline_seconds'),
            86400
        )
    );

-- Durable, duplicate-safe state events. `audit_event_id` makes the
-- coordinator's existing transactional audit stream the source of each event.
CREATE TABLE observability_outbox (
    event_id TEXT PRIMARY KEY,
    audit_event_id INTEGER UNIQUE REFERENCES audit_events(id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    traceparent TEXT,
    tracestate TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    delivered_at TEXT,
    last_error_class TEXT
);

CREATE INDEX idx_observability_outbox_pending
    ON observability_outbox(delivered_at, next_attempt_at, occurred_at);

CREATE TABLE observability_coverage (
    id INTEGER PRIMARY KEY CHECK(id=1),
    coverage_started_at TEXT NOT NULL,
    last_snapshot_at TEXT,
    gap_detected INTEGER NOT NULL DEFAULT 0,
    gap_reason TEXT,
    expired_events INTEGER NOT NULL DEFAULT 0
);

INSERT INTO observability_coverage(id,coverage_started_at,gap_detected)
VALUES (1,strftime('%Y-%m-%dT%H:%M:%fZ','now'),0);
