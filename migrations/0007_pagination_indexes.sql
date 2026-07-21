PRAGMA foreign_keys = ON;

-- Global cursor pages use a timestamp plus the immutable entity key as a
-- deterministic tie-breaker. DESC is explicit so query plans remain obvious
-- to operators even though SQLite can reverse-scan an ascending index.
CREATE INDEX IF NOT EXISTS idx_schedules_keyset
    ON schedules(created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_runs_keyset
    ON runs(created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_batches_keyset
    ON batches(created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_blueprint_revisions_keyset
    ON blueprint_revisions(loaded_at DESC, digest DESC);

CREATE INDEX IF NOT EXISTS idx_schedule_blueprint_digest
    ON schedule_blueprint_revisions(
        blueprint_digest, is_current, schedule_id, schedule_revision
    );

CREATE INDEX IF NOT EXISTS idx_health_evidence_keyset
    ON health_evidence(occurred_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_health_evidence_agent_keyset
    ON health_evidence(agent_id, occurred_at DESC, id DESC);

-- The current audit API is entity-scoped and already uses idx_audit_entity.
-- Keep the global audit ordering ready for the cluster-wide audit page.
CREATE INDEX IF NOT EXISTS idx_audit_events_keyset
    ON audit_events(occurred_at DESC, id DESC);

-- UNIQUE(run_id, attempt_number) already makes attempt_number unique within a
-- run. Including the API's ID tie-breaker documents and covers the exact
-- keyset ordering without requiring a table lookup.
CREATE INDEX IF NOT EXISTS idx_attempts_run_keyset
    ON attempts(run_id, attempt_number, id);
