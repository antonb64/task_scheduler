use std::{path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use fs2::FileExt;
use scheduler_core::ExecutionAssignment;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use uuid::Uuid;

#[derive(Clone)]
pub struct Ledger {
    pool: SqlitePool,
    // A second live agent process must never recover claims from the first.
    // The OS releases this lock on crash, making restart takeover safe.
    _owner_lock: Arc<std::fs::File>,
    /// Identifies one lifetime of the agent process. Clones deliberately share
    /// this value, while reopening the ledger after a crash creates a new one.
    runner_id: String,
}

#[derive(Debug, Clone)]
pub struct PendingResult {
    pub attempt_id: String,
    pub lease_token: String,
    pub result_json: String,
}

#[derive(Debug, Clone)]
pub struct RecoverableAssignment {
    pub assignment_json: String,
}

impl Ledger {
    pub async fn connect(url: &str) -> Result<Self> {
        let lock_path = ledger_lock_path(url)?;
        let owner_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("cannot open agent ownership lock {}", lock_path.display()))?;
        owner_lock
            .try_lock_exclusive()
            .context("another live agent process already owns this ledger")?;
        let options = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS assignments(attempt_id TEXT PRIMARY KEY,lease_token TEXT NOT NULL,assignment_json TEXT NOT NULL,state TEXT NOT NULL,result_json TEXT,updated_at TEXT NOT NULL)")
            .execute(&pool).await?;
        let columns = sqlx::query("PRAGMA table_info(assignments)")
            .fetch_all(&pool)
            .await?;
        if !columns
            .iter()
            .any(|column| column.get::<String, _>("name") == "runner_id")
        {
            sqlx::query("ALTER TABLE assignments ADD COLUMN runner_id TEXT")
                .execute(&pool)
                .await?;
        }
        sqlx::query("CREATE TABLE IF NOT EXISTS local_settings(id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL,settings_json TEXT NOT NULL,updated_at TEXT NOT NULL)")
            .execute(&pool).await?;
        Ok(Self {
            pool,
            _owner_lock: Arc::new(owner_lock),
            runner_id: Uuid::new_v4().to_string(),
        })
    }

    /// Compatibility helper used by simulations: durably record and claim an
    /// assignment in one call. Production message handling separates these
    /// operations so it can acknowledge the durable record before claiming it.
    #[cfg(test)]
    pub async fn accept(&self, assignment: &ExecutionAssignment, json: &str) -> Result<bool> {
        self.record(assignment, json).await?;
        if !self.claim(assignment, json).await? {
            return Ok(false);
        }
        self.start(&assignment.attempt_id.to_string()).await
    }

    /// Persist an offer before acknowledging it. Duplicate delivery is
    /// idempotent and never replaces the original lease or execution snapshot.
    pub async fn record(&self, assignment: &ExecutionAssignment, json: &str) -> Result<bool> {
        let result = sqlx::query("INSERT OR IGNORE INTO assignments(attempt_id,lease_token,assignment_json,state,updated_at) VALUES (?,?,?,'accepted',?)")
            .bind(assignment.attempt_id.to_string())
            .bind(&assignment.lease_token)
            .bind(json)
            .bind(Utc::now().to_rfc3339())
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 1 {
            return Ok(true);
        }
        let row = sqlx::query(
            "SELECT lease_token,assignment_json,state FROM assignments WHERE attempt_id=?",
        )
        .bind(assignment.attempt_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        let existing_lease: String = row.get("lease_token");
        let existing_json: String = row.get("assignment_json");
        if existing_lease != assignment.lease_token || existing_json != json {
            bail!(
                "conflicting duplicate assignment for attempt {}",
                assignment.attempt_id
            );
        }
        Ok(false)
    }

    /// Atomically claim execution for this agent-process lifetime. A clone of
    /// this ledger cannot claim the same attempt twice, but a newly started
    /// process can take over a claim left by a crashed predecessor.
    pub async fn claim(&self, assignment: &ExecutionAssignment, json: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE assignments SET state='claimed',runner_id=?,updated_at=? \
             WHERE attempt_id=? AND lease_token=? AND assignment_json=? \
             AND (state='accepted' OR (state IN ('claimed','running') AND (runner_id IS NULL OR runner_id<>?)))",
        )
        .bind(&self.runner_id)
        .bind(Utc::now().to_rfc3339())
        .bind(assignment.attempt_id.to_string())
        .bind(&assignment.lease_token)
        .bind(json)
        .bind(&self.runner_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Work which never reached a claim, or which was owned by a previous
    /// agent process, must be acknowledged and executed again after startup.
    pub async fn recoverable_assignments(&self) -> Result<Vec<RecoverableAssignment>> {
        let rows = sqlx::query(
            "SELECT attempt_id,assignment_json FROM assignments \
             WHERE state='accepted' OR (state IN ('claimed','running') AND (runner_id IS NULL OR runner_id<>?)) \
             ORDER BY updated_at,attempt_id",
        )
        .bind(&self.runner_id)
        .fetch_all(&self.pool)
        .await?;
        let mut recovered = Vec::new();
        for row in rows {
            let attempt_id: String = row.get("attempt_id");
            let assignment_json: String = row.get("assignment_json");
            if serde_json::from_str::<ExecutionAssignment>(&assignment_json).is_err() {
                sqlx::query(
                    "UPDATE assignments SET state='quarantined',updated_at=? WHERE attempt_id=?",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(attempt_id)
                .execute(&self.pool)
                .await?;
                continue;
            }
            recovered.push(RecoverableAssignment { assignment_json });
        }
        Ok(recovered)
    }

    /// Moves a claimed row into the executable state. Cancellation wins this
    /// compare-and-swap, including the narrow claim/spawn race.
    pub async fn start(&self, attempt_id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE assignments SET state='running',updated_at=? \
             WHERE attempt_id=? AND state='claimed' AND runner_id=?",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(attempt_id)
        .bind(&self.runner_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Cancellation is durable before signaling a process. Work which has not
    /// started becomes terminal; running work remains result-eligible.
    pub async fn cancel(&self, attempt_id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE assignments SET state=CASE WHEN state='running' THEN 'cancel_requested' ELSE 'cancelled' END,updated_at=? \
             WHERE attempt_id=? AND state IN ('accepted','claimed','running')",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(attempt_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn save_result(&self, attempt_id: &str, result_json: &str) -> Result<()> {
        let result = sqlx::query(
            "UPDATE assignments SET state='finished',result_json=?,updated_at=? \
             WHERE attempt_id=? AND runner_id=? AND state IN ('running','cancel_requested')",
        )
        .bind(result_json)
        .bind(Utc::now().to_rfc3339())
        .bind(attempt_id)
        .bind(&self.runner_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            let existing = sqlx::query(
                "SELECT state,result_json FROM assignments WHERE attempt_id=? AND runner_id=?",
            )
            .bind(attempt_id)
            .bind(&self.runner_id)
            .fetch_optional(&self.pool)
            .await?;
            if !existing.is_some_and(|row| {
                row.get::<String, _>("state") == "finished"
                    && row.get::<Option<String>, _>("result_json").as_deref() == Some(result_json)
            }) {
                bail!("attempt is not owned, running, or result-eligible");
            }
        }
        Ok(())
    }

    pub async fn acknowledge(&self, attempt_id: &str) -> Result<()> {
        let result = sqlx::query("UPDATE assignments SET state='acknowledged',updated_at=? WHERE attempt_id=? AND state='finished' AND result_json IS NOT NULL")
            .bind(Utc::now().to_rfc3339())
            .bind(attempt_id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1
            && self.state(attempt_id).await?.as_deref() != Some("acknowledged")
        {
            bail!("only a finished attempt result can be acknowledged");
        }
        Ok(())
    }

    pub async fn pending_results(&self) -> Result<Vec<PendingResult>> {
        let rows = sqlx::query("SELECT attempt_id,lease_token,result_json FROM assignments WHERE state='finished' AND result_json IS NOT NULL")
            .fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| PendingResult {
                attempt_id: row.get("attempt_id"),
                lease_token: row.get("lease_token"),
                result_json: row.get("result_json"),
            })
            .collect())
    }

    pub async fn result(&self, attempt_id: &str) -> Result<Option<PendingResult>> {
        let row = sqlx::query("SELECT attempt_id,lease_token,result_json FROM assignments WHERE attempt_id=? AND state='finished'")
            .bind(attempt_id).fetch_optional(&self.pool).await?;
        Ok(row.map(|row| PendingResult {
            attempt_id: row.get("attempt_id"),
            lease_token: row.get("lease_token"),
            result_json: row.get("result_json"),
        }))
    }

    pub async fn state(&self, attempt_id: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT state FROM assignments WHERE attempt_id=?")
                .bind(attempt_id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    pub async fn assignment(&self, attempt_id: &str, lease_token: &str) -> Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT assignment_json FROM assignments WHERE attempt_id=? AND lease_token=? AND state IN ('accepted','claimed','running')",
        )
        .bind(attempt_id)
        .bind(lease_token)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn save_settings(&self, revision: i64, json: &str) -> Result<()> {
        sqlx::query("INSERT INTO local_settings(id,revision,settings_json,updated_at) VALUES (1,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,settings_json=excluded.settings_json,updated_at=excluded.updated_at")
            .bind(revision).bind(json).bind(Utc::now().to_rfc3339()).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn load_settings(&self) -> Result<Option<(i64, String)>> {
        let row = sqlx::query("SELECT revision,settings_json FROM local_settings WHERE id=1")
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|row| (row.get("revision"), row.get("settings_json"))))
    }
}

fn ledger_lock_path(url: &str) -> Result<PathBuf> {
    let raw = url
        .strip_prefix("sqlite://")
        .context("agent ledger must use a file-backed sqlite:// URL")?;
    let filename = raw.split('?').next().unwrap_or(raw);
    if filename.is_empty() || filename == ":memory:" {
        bail!("agent ledger must be file-backed to enforce single-process ownership");
    }
    let mut lock = PathBuf::from(filename);
    let extension = lock
        .extension()
        .and_then(|value| value.to_str())
        .map_or_else(|| "lock".to_owned(), |value| format!("{value}.owner.lock"));
    lock.set_extension(extension);
    Ok(lock)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf};

    use scheduler_core::{
        CommandSpec, ExecutionPolicy, ExecutionSnapshot, ExecutorSpec, NodeSettings,
    };

    use super::*;

    fn assignment() -> ExecutionAssignment {
        ExecutionAssignment {
            schedule_id: Uuid::new_v4(),
            run_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            attempt_number: 1,
            lease_token: "test-lease".into(),
            lease_seconds: 60,
            snapshot: ExecutionSnapshot {
                executor: ExecutorSpec::Command(CommandSpec {
                    program: "test-command".into(),
                    args: Vec::new(),
                    env: BTreeMap::new(),
                    working_directory: None,
                }),
                policy: ExecutionPolicy {
                    max_attempts: 1,
                    timeout_seconds: 60,
                    initial_backoff_seconds: 1,
                    backoff_cap_seconds: 1,
                },
                required_labels: BTreeMap::new(),
                parameters_digest: "test-parameters".into(),
            },
        }
    }

    fn database() -> (PathBuf, String) {
        let path =
            std::env::temp_dir().join(format!("scheduler-agent-ledger-{}.sqlite", Uuid::new_v4()));
        let url = format!("sqlite://{}", path.display());
        (path, url)
    }

    #[tokio::test]
    async fn a_live_agent_claims_an_attempt_only_once() {
        let (path, url) = database();
        let ledger = Ledger::connect(&url).await.expect("connect ledger");
        let assignment = assignment();
        let json = serde_json::to_string(&assignment).expect("serialize assignment");

        assert!(
            ledger
                .accept(&assignment, &json)
                .await
                .expect("first claim")
        );
        assert!(
            !ledger
                .accept(&assignment, &json)
                .await
                .expect("duplicate claim")
        );
        assert!(
            ledger
                .recoverable_assignments()
                .await
                .expect("recovery scan")
                .is_empty()
        );

        ledger.pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_restarted_agent_recovers_a_previous_process_claim() {
        let (path, url) = database();
        let assignment = assignment();
        let json = serde_json::to_string(&assignment).expect("serialize assignment");
        let original = Ledger::connect(&url)
            .await
            .expect("connect original ledger");
        assert!(
            original
                .accept(&assignment, &json)
                .await
                .expect("initial claim")
        );
        original.pool.close().await;
        drop(original);

        let restarted = Ledger::connect(&url).await.expect("reopen ledger");
        let recoverable = restarted
            .recoverable_assignments()
            .await
            .expect("recovery scan");
        assert_eq!(recoverable.len(), 1);
        assert_eq!(recoverable[0].assignment_json, json);
        assert!(
            restarted
                .accept(&assignment, &json)
                .await
                .expect("recovery claim")
        );
        assert!(
            !restarted
                .accept(&assignment, &json)
                .await
                .expect("duplicate recovery")
        );

        restarted.pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn an_unclaimed_durable_acceptance_is_recovered_but_finished_work_is_not() {
        let (path, url) = database();
        let assignment = assignment();
        let json = serde_json::to_string(&assignment).expect("serialize assignment");
        let ledger = Ledger::connect(&url).await.expect("connect ledger");
        assert!(
            ledger
                .record(&assignment, &json)
                .await
                .expect("record offer")
        );
        assert_eq!(
            ledger
                .recoverable_assignments()
                .await
                .expect("recovery scan")
                .len(),
            1
        );
        assert!(ledger.claim(&assignment, &json).await.expect("claim offer"));
        assert!(
            ledger
                .start(&assignment.attempt_id.to_string())
                .await
                .expect("start offer")
        );
        ledger
            .save_result(&assignment.attempt_id.to_string(), "{}")
            .await
            .expect("save result");
        assert!(
            ledger
                .recoverable_assignments()
                .await
                .expect("finished scan")
                .is_empty()
        );

        ledger.pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn a_second_live_agent_cannot_own_or_steal_the_same_ledger() {
        let (path, url) = database();
        let first = Ledger::connect(&url).await.expect("first live owner");
        let error = Ledger::connect(&url)
            .await
            .err()
            .expect("second live owner must be rejected");
        assert!(
            format!("{error:#}").contains("another live agent process"),
            "unexpected error: {error:#}"
        );
        first.pool.close().await;
        drop(first);
        Ledger::connect(&url)
            .await
            .expect("lock must be recoverable after owner exits")
            .pool
            .close()
            .await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn cancellation_before_start_is_durable_and_blocks_execution() {
        let (path, url) = database();
        let ledger = Ledger::connect(&url).await.expect("ledger");
        let assignment = assignment();
        let json = serde_json::to_string(&assignment).expect("json");
        assert!(ledger.record(&assignment, &json).await.expect("record"));
        assert!(ledger.claim(&assignment, &json).await.expect("claim"));
        assert!(
            ledger
                .cancel(&assignment.attempt_id.to_string())
                .await
                .expect("cancel")
        );
        assert!(
            !ledger
                .start(&assignment.attempt_id.to_string())
                .await
                .expect("start")
        );
        assert!(
            ledger
                .recoverable_assignments()
                .await
                .expect("recover")
                .is_empty()
        );
        ledger.pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn corrupt_recovery_rows_are_quarantined_without_blocking_valid_work() {
        let (path, url) = database();
        let ledger = Ledger::connect(&url).await.expect("ledger");
        sqlx::query("INSERT INTO assignments(attempt_id,lease_token,assignment_json,state,updated_at) VALUES ('corrupt','lease','{','accepted',?)")
            .bind(Utc::now().to_rfc3339())
            .execute(&ledger.pool).await.expect("corrupt row");
        let assignment = assignment();
        let json = serde_json::to_string(&assignment).expect("json");
        ledger.record(&assignment, &json).await.expect("valid row");
        let rows = ledger.recoverable_assignments().await.expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].assignment_json, json);
        let state: String =
            sqlx::query_scalar("SELECT state FROM assignments WHERE attempt_id='corrupt'")
                .fetch_one(&ledger.pool)
                .await
                .expect("state");
        assert_eq!(state, "quarantined");
        ledger.pool.close().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn authoritative_settings_survive_restart_before_recovery() {
        let (path, url) = database();
        let first = Ledger::connect(&url).await.expect("ledger");
        let settings = NodeSettings {
            enabled: false,
            max_parallel: 1,
            ..NodeSettings::default()
        };
        let json = serde_json::to_string(&settings).expect("json");
        first.save_settings(7, &json).await.expect("persist");
        first.pool.close().await;
        drop(first);

        let restarted = Ledger::connect(&url).await.expect("restart");
        let (revision, persisted) = restarted
            .load_settings()
            .await
            .expect("load")
            .expect("settings");
        assert_eq!(revision, 7);
        let loaded: NodeSettings = serde_json::from_str(&persisted).expect("settings json");
        assert!(!loaded.enabled);
        assert_eq!(loaded.max_parallel, 1);
        restarted.pool.close().await;
        let _ = std::fs::remove_file(path);
    }
}
