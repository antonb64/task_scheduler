PRAGMA foreign_keys = ON;

-- Keep the dashboard's lifetime node throughput independent of attempts-table
-- size. A completed retry is intentionally another processed task attempt.
CREATE TABLE node_processing_stats (
    agent_id TEXT PRIMARY KEY,
    processed_tasks INTEGER NOT NULL DEFAULT 0,
    succeeded INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    cancelled INTEGER NOT NULL DEFAULT 0,
    last_processed_at TEXT
);

INSERT INTO node_processing_stats(
    agent_id, processed_tasks, succeeded, failed, cancelled, last_processed_at
)
SELECT
    agent_id,
    COUNT(*),
    SUM(CASE WHEN outcome='succeeded' THEN 1 ELSE 0 END),
    SUM(CASE WHEN outcome IS NOT NULL AND outcome NOT IN ('succeeded','cancelled') THEN 1 ELSE 0 END),
    SUM(CASE WHEN outcome='cancelled' THEN 1 ELSE 0 END),
    MAX(finished_at)
FROM attempts
WHERE finished_at IS NOT NULL
GROUP BY agent_id;

CREATE INDEX idx_runs_schedule_keyset
    ON runs(schedule_id, created_at DESC, id DESC);
