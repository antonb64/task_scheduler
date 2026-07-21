PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS health_evidence (
    id TEXT PRIMARY KEY,
    attempt_id TEXT REFERENCES attempts(id),
    run_id TEXT NOT NULL REFERENCES runs(id),
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    agent_id TEXT NOT NULL,
    blueprint_digest TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    classifier_version INTEGER NOT NULL,
    evidence_class TEXT NOT NULL,
    failure_family TEXT NOT NULL,
    failure_code TEXT,
    failure_origin TEXT,
    failure_stage TEXT,
    diagnostic_json TEXT,
    safe_status_json TEXT,
    node_was_healthy INTEGER NOT NULL,
    cluster_suppressed INTEGER NOT NULL DEFAULT 0,
    retracted INTEGER NOT NULL DEFAULT 0,
    occurred_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_health_evidence_attempt
    ON health_evidence(attempt_id) WHERE attempt_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_health_evidence_input
    ON health_evidence(blueprint_digest, input_fingerprint, occurred_at);
CREATE INDEX IF NOT EXISTS idx_health_evidence_node
    ON health_evidence(agent_id, occurred_at);

CREATE TABLE IF NOT EXISTS input_health (
    blueprint_digest TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL,
    failure_family TEXT,
    distinct_healthy_nodes INTEGER NOT NULL DEFAULT 0,
    probe_available INTEGER NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(blueprint_digest, input_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_input_health_state
    ON input_health(state, updated_at);

CREATE TABLE IF NOT EXISTS node_health (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id),
    state TEXT NOT NULL DEFAULT 'healthy',
    reason_code TEXT,
    distinct_failed_inputs INTEGER NOT NULL DEFAULT 0,
    distinct_schedules INTEGER NOT NULL DEFAULT 0,
    considered_observations INTEGER NOT NULL DEFAULT 0,
    failure_rate REAL NOT NULL DEFAULT 0,
    probation_functional_count INTEGER NOT NULL DEFAULT 0,
    probation_distinct_inputs INTEGER NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL DEFAULT 1,
    transitioned_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_node_health_state
    ON node_health(state, updated_at);

INSERT OR IGNORE INTO node_health(agent_id,state,transitioned_at,updated_at)
SELECT id,'healthy',updated_at,updated_at FROM agents;
