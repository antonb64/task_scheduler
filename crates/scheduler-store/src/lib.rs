use std::{path::Path, str::FromStr, sync::Arc};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use rand::Rng;
use scheduler_core::{
    AgentView, CronSpec, ExecutionResult, FailureCode, FailureDiagnostic, GlobalSettings,
    NodeSettings, OutputMetadata, RunState, RunView, ScheduleSpec, ScheduleView,
};
use serde_json::Value;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use uuid::Uuid;

mod blueprints;
mod collection;
mod dashboard;
mod health;
pub use blueprints::*;
pub use collection::*;
pub use dashboard::*;
pub use health::*;

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    _lock: Option<Arc<std::fs::File>>,
}

#[derive(Debug, Clone)]
pub struct AgentRegistration {
    pub desired_settings: NodeSettings,
    pub applied_settings_revision: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsAckStatus {
    Applied,
    Rejected,
    Ignored,
}

#[derive(Debug, Clone)]
pub struct SettingsAckOutcome {
    pub status: SettingsAckStatus,
    pub desired_revision: i64,
    pub applied_revision: i64,
    /// The exact document that the coordinator durably recorded as applied.
    /// This is present only for a successful acknowledgement of the current
    /// desired revision.
    pub applied_settings: Option<NodeSettings>,
}

#[derive(Debug, Clone)]
pub struct ScheduleRecord {
    pub view: ScheduleView,
    pub encrypted_snapshot: Vec<u8>,
    pub snapshot_digest: String,
    pub key_id: String,
    pub webhook_secret_hash: Option<String>,
    pub last_cron_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct NewSchedule {
    pub id: Uuid,
    pub spec: ScheduleSpec,
    pub encrypted_snapshot: Vec<u8>,
    pub snapshot_digest: String,
    pub key_id: String,
    pub webhook_public_id: Option<String>,
    pub webhook_secret_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunRecord {
    pub view: RunView,
    pub encrypted_snapshot: Vec<u8>,
    pub key_id: String,
    pub max_attempts: u32,
    pub initial_backoff_seconds: u64,
    pub backoff_cap_seconds: u64,
    pub not_before: DateTime<Utc>,
}

/// Exact queued-run counts captured from one SQLite statement.
///
/// `ready` is the dispatchable subset whose `not_before` fence has elapsed;
/// `delayed` is still queued but waiting for its retry or future-run fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueDepth {
    pub ready: u64,
    pub delayed: u64,
}

#[derive(Debug, Clone)]
pub struct NewRun {
    pub id: Uuid,
    pub schedule_id: Uuid,
    pub trigger_kind: String,
    pub scheduled_at: DateTime<Utc>,
    pub encrypted_snapshot: Vec<u8>,
    pub key_id: String,
    pub max_attempts: u32,
    pub initial_backoff_seconds: u64,
    pub backoff_cap_seconds: u64,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone)]
pub enum CronOccurrenceResult {
    Applied(RunView),
    StaleSchedule,
}

#[derive(Debug, Clone)]
pub struct AttemptRecord {
    pub id: Uuid,
    pub run_id: Uuid,
    pub agent_id: String,
    pub attempt_number: u32,
    pub lease_token: String,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AttemptView {
    pub id: Uuid,
    pub run_id: Uuid,
    pub agent_id: String,
    pub attempt_number: u32,
    pub state: String,
    pub outcome: Option<String>,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub duration_ms: Option<u64>,
    pub diagnostic: Option<FailureDiagnostic>,
    pub output: Option<OutputMetadata>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EditLock {
    pub document_key: String,
    pub owner_session: String,
    pub lock_token: String,
    pub expires_at: DateTime<Utc>,
}

impl Store {
    pub async fn connect(database_url: &str, lock_path: Option<&Path>) -> Result<Self> {
        let lock = if let Some(path) = lock_path {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)
                .with_context(|| format!("cannot open coordinator lock {}", path.display()))?;
            file.try_lock_exclusive()
                .context("another coordinator already owns this database")?;
            Some(Arc::new(file))
        } else {
            None
        };

        let options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        sqlx::migrate!("../../migrations").run(&pool).await?;
        let store = Self { pool, _lock: lock };
        store.ensure_default_settings().await?;
        Ok(store)
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn ensure_default_settings(&self) -> Result<()> {
        let settings = GlobalSettings::default();
        let now = now_string();
        sqlx::query(
            "INSERT OR IGNORE INTO settings_documents(document_key, document_json, revision, updated_at) VALUES ('global', ?, 1, ?)",
        )
        .bind(serde_json::to_string(&settings)?)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_schedule(&self, new: NewSchedule) -> Result<ScheduleView> {
        let now = now_string();
        let id = new.id.to_string();
        let snapshot_digest = new.snapshot_digest.clone();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO schedules(id,name,spec_json,encrypted_snapshot,snapshot_digest,key_id,revision,enabled,cron_expression,cron_timezone,webhook_enabled,webhook_public_id,webhook_secret_hash,created_at,updated_at) VALUES (?,?,?,?,?,?,1,?,?,?,?,?,?,?,?)",
        )
        .bind(&id)
        .bind(&new.spec.name)
        .bind(serde_json::to_string(&new.spec)?)
        .bind(new.encrypted_snapshot)
        .bind(new.snapshot_digest)
        .bind(new.key_id)
        .bind(new.spec.enabled)
        .bind(new.spec.cron.as_ref().map(|cron| &cron.expression))
        .bind(new.spec.cron.as_ref().map(|cron| &cron.timezone))
        .bind(new.spec.webhook_enabled)
        .bind(new.webhook_public_id)
        .bind(new.webhook_secret_hash)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO schedule_blueprint_revisions(\
             schedule_id,schedule_revision,blueprint_digest,is_current,created_at) \
             SELECT ?,1,blueprint_digest,1,? FROM resolved_snapshot_blueprints \
             WHERE snapshot_digest=?",
        )
        .bind(&id)
        .bind(&now)
        .bind(&snapshot_digest)
        .execute(&mut *tx)
        .await?;
        append_audit_tx(
            &mut tx,
            "schedule",
            &id,
            "schedule.created",
            serde_json::json!({"name": new.spec.name}),
        )
        .await?;
        tx.commit().await?;
        self.get_schedule(new.id)
            .await?
            .context("created schedule missing")
    }

    pub async fn update_schedule(
        &self,
        id: Uuid,
        expected_revision: i64,
        spec: ScheduleSpec,
        encrypted_snapshot: Vec<u8>,
        digest: String,
        key_id: String,
    ) -> Result<ScheduleView> {
        let now = now_string();
        let cron_expression = spec.cron.as_ref().map(|cron| cron.expression.as_str());
        let cron_timezone = spec.cron.as_ref().map(|cron| cron.timezone.as_str());
        let snapshot_digest = digest.clone();
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE schedules SET name=?,spec_json=?,encrypted_snapshot=?,snapshot_digest=?,key_id=?,revision=revision+1,enabled=?,last_cron_at=CASE \
             WHEN enabled=0 AND ?=1 AND ? IS NOT NULL THEN CASE WHEN last_cron_at IS NULL OR last_cron_at<? THEN ? ELSE last_cron_at END \
             WHEN NOT (cron_expression IS ? AND cron_timezone IS ?) THEN ? ELSE last_cron_at END,cron_expression=?,cron_timezone=?,webhook_enabled=?,updated_at=? WHERE id=? AND revision=?",
        )
        .bind(&spec.name)
        .bind(serde_json::to_string(&spec)?)
        .bind(encrypted_snapshot)
        .bind(digest)
        .bind(key_id)
        .bind(spec.enabled)
        .bind(spec.enabled)
        .bind(cron_expression)
        .bind(&now)
        .bind(&now)
        .bind(cron_expression)
        .bind(cron_timezone)
        .bind(&now)
        .bind(cron_expression)
        .bind(cron_timezone)
        .bind(spec.webhook_enabled)
        .bind(&now)
        .bind(id.to_string())
        .bind(expected_revision)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            tx.rollback().await?;
            bail!("schedule revision conflict or schedule not found");
        }
        sqlx::query("UPDATE schedule_blueprint_revisions SET is_current=0 WHERE schedule_id=?")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO schedule_blueprint_revisions(\
             schedule_id,schedule_revision,blueprint_digest,is_current,created_at) \
             SELECT ?,?,blueprint_digest,1,? FROM resolved_snapshot_blueprints \
             WHERE snapshot_digest=?",
        )
        .bind(id.to_string())
        .bind(expected_revision + 1)
        .bind(&now)
        .bind(&snapshot_digest)
        .execute(&mut *tx)
        .await?;
        append_audit_tx(
            &mut tx,
            "schedule",
            &id.to_string(),
            "schedule.updated",
            serde_json::json!({"revision": expected_revision + 1}),
        )
        .await?;
        tx.commit().await?;
        self.get_schedule(id)
            .await?
            .context("updated schedule missing")
    }

    pub async fn set_schedule_enabled(&self, id: Uuid, enabled: bool) -> Result<()> {
        self.set_schedule_enabled_at(id, enabled, Utc::now()).await
    }

    async fn set_schedule_enabled_at(
        &self,
        id: Uuid,
        enabled: bool,
        transition_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT spec_json,enabled,revision FROM schedules WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&mut *tx)
            .await?
            .context("schedule not found")?;
        let was_enabled = row.get::<bool, _>("enabled");
        if was_enabled == enabled {
            tx.commit().await?;
            return Ok(());
        }
        let revision = row.get::<i64, _>("revision");
        let mut spec: ScheduleSpec = serde_json::from_str(row.get("spec_json"))?;
        spec.enabled = enabled;
        let transition_at = format_time(transition_at);
        let advance_cron_cursor = enabled && !was_enabled && spec.cron.is_some();
        let updated = sqlx::query(
            "UPDATE schedules SET enabled=?,spec_json=?,revision=revision+1,\
             last_cron_at=CASE WHEN ?=1 AND cron_expression IS NOT NULL AND \
             (last_cron_at IS NULL OR last_cron_at<?) THEN ? ELSE last_cron_at END,\
             updated_at=? WHERE id=? AND enabled=? AND revision=?",
        )
        .bind(enabled)
        .bind(serde_json::to_string(&spec)?)
        .bind(advance_cron_cursor)
        .bind(&transition_at)
        .bind(&transition_at)
        .bind(&transition_at)
        .bind(id.to_string())
        .bind(was_enabled)
        .bind(revision)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            tx.rollback().await?;
            bail!("schedule revision conflict or schedule not found");
        }
        append_audit_tx(
            &mut tx,
            "schedule",
            &id.to_string(),
            if enabled {
                "schedule.resumed"
            } else {
                "schedule.paused"
            },
            Value::Null,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn rotate_webhook(
        &self,
        id: Uuid,
        public_id: String,
        secret_hash: String,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT spec_json FROM schedules WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&mut *tx)
            .await?
            .context("schedule not found")?;
        let mut spec: ScheduleSpec = serde_json::from_str(row.get("spec_json"))?;
        spec.webhook_enabled = true;
        sqlx::query("UPDATE schedules SET webhook_enabled=1,spec_json=?,webhook_public_id=?,webhook_secret_hash=?,revision=revision+1,updated_at=? WHERE id=?")
            .bind(serde_json::to_string(&spec)?)
            .bind(public_id)
            .bind(secret_hash)
            .bind(now_string())
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        append_audit_tx(
            &mut tx,
            "schedule",
            &id.to_string(),
            "schedule.webhook_rotated",
            Value::Null,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_schedules(&self) -> Result<Vec<ScheduleView>> {
        let rows = sqlx::query("SELECT id,spec_json,revision,created_at,updated_at,webhook_public_id FROM schedules ORDER BY created_at DESC")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(schedule_view_from_row).collect()
    }

    pub async fn list_schedules_page(
        &self,
        cursor_created_at: Option<&str>,
        cursor_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ScheduleView>> {
        let rows = sqlx::query(
            "SELECT id,spec_json,revision,created_at,updated_at,webhook_public_id FROM schedules \
             WHERE (? IS NULL OR created_at<? OR (created_at=? AND id<?)) \
             ORDER BY created_at DESC,id DESC LIMIT ?",
        )
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.clamp(1, 201)))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(schedule_view_from_row).collect()
    }

    pub async fn get_schedule(&self, id: Uuid) -> Result<Option<ScheduleView>> {
        let row = sqlx::query("SELECT id,spec_json,revision,created_at,updated_at,webhook_public_id FROM schedules WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(schedule_view_from_row).transpose()
    }

    pub async fn get_schedule_record(&self, id: Uuid) -> Result<Option<ScheduleRecord>> {
        let row = sqlx::query("SELECT id,spec_json,revision,created_at,updated_at,webhook_public_id,encrypted_snapshot,snapshot_digest,key_id,webhook_secret_hash,last_cron_at FROM schedules WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(schedule_record_from_row).transpose()
    }

    pub async fn get_schedule_by_public_id(
        &self,
        public_id: &str,
    ) -> Result<Option<ScheduleRecord>> {
        let row = sqlx::query("SELECT id,spec_json,revision,created_at,updated_at,webhook_public_id,encrypted_snapshot,snapshot_digest,key_id,webhook_secret_hash,last_cron_at FROM schedules WHERE webhook_public_id=? AND webhook_enabled=1 AND enabled=1")
            .bind(public_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(schedule_record_from_row).transpose()
    }

    pub async fn cron_schedules(&self) -> Result<Vec<ScheduleRecord>> {
        let rows = sqlx::query("SELECT id,spec_json,revision,created_at,updated_at,webhook_public_id,encrypted_snapshot,snapshot_digest,key_id,webhook_secret_hash,last_cron_at FROM schedules WHERE enabled=1 AND cron_expression IS NOT NULL")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(schedule_record_from_row).collect()
    }

    pub async fn advance_cron_cursor(&self, id: Uuid, occurrence: DateTime<Utc>) -> Result<()> {
        sqlx::query("UPDATE schedules SET last_cron_at=? WHERE id=? AND (last_cron_at IS NULL OR last_cron_at < ?)")
            .bind(format_time(occurrence))
            .bind(id.to_string())
            .bind(format_time(occurrence))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Atomically creates (or returns) one cron run and advances its schedule cursor.
    ///
    /// The schedule revision and complete cron identity are fenced in the same SQLite write
    /// transaction as the run insert. A caller holding a detached, stale `ScheduleRecord`
    /// therefore cannot materialize an old snapshot after an edit commits.
    pub async fn create_cron_occurrence(
        &self,
        new: NewRun,
        expected_revision: i64,
        expected_cron: &CronSpec,
    ) -> Result<CronOccurrenceResult> {
        if new.trigger_kind != "cron" {
            bail!("cron occurrence must use the cron trigger kind");
        }

        let now = now_string();
        let occurrence = format_time(new.scheduled_at);
        let schedule_id = new.schedule_id.to_string();
        let id = new.id.to_string();
        let trigger_identity_id = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin().await?;

        // Take the SQLite writer lock and fence the detached schedule before inserting a run.
        // This update and the insert either both commit or both roll back.
        let fenced = sqlx::query(
            "UPDATE schedules SET last_cron_at=CASE WHEN last_cron_at IS NULL OR last_cron_at < ? THEN ? ELSE last_cron_at END WHERE id=? AND revision=? AND enabled=1 AND cron_expression=? AND cron_timezone=?",
        )
        .bind(&occurrence)
        .bind(&occurrence)
        .bind(&schedule_id)
        .bind(expected_revision)
        .bind(&expected_cron.expression)
        .bind(&expected_cron.timezone)
        .execute(&mut *tx)
        .await?;
        if fenced.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(CronOccurrenceResult::StaleSchedule);
        }

        let trigger = sqlx::query(
            "INSERT INTO trigger_identities(id,schedule_id,trigger_kind,scheduled_at,idempotency_key,target_kind,target_id,created_at) VALUES (?,?,'cron',?,NULL,'run',?,?)",
        )
        .bind(trigger_identity_id)
        .bind(&schedule_id)
        .bind(&occurrence)
        .bind(&id)
        .bind(&now)
        .execute(&mut *tx)
        .await;
        if let Err(error) = trigger {
            if is_unique_violation(&error) {
                tx.rollback().await?;
                let existing = self
                    .trigger_target_for_cron(new.schedule_id, new.scheduled_at)
                    .await?
                    .context("cron trigger identity exists without a target")?;
                if existing.0 != "run" {
                    bail!("cron occurrence was already materialized as a collection batch");
                }
                let run_id = Uuid::parse_str(&existing.1)?;
                return Ok(CronOccurrenceResult::Applied(
                    self.get_run(run_id)
                        .await?
                        .context("cron trigger points to a missing run")?,
                ));
            }
            tx.rollback().await?;
            return Err(error.into());
        }

        let insert = sqlx::query(
            "INSERT INTO runs(id,schedule_id,state,trigger_kind,scheduled_at,not_before,encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,backoff_cap_seconds,idempotency_key,created_at,updated_at) VALUES (?,?, 'queued','cron',?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&id)
        .bind(&schedule_id)
        .bind(&occurrence)
        .bind(&occurrence)
        .bind(new.encrypted_snapshot)
        .bind(new.key_id)
        .bind(new.max_attempts)
        .bind(new.initial_backoff_seconds as i64)
        .bind(new.backoff_cap_seconds as i64)
        .bind(new.idempotency_key)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await;

        let run_id = match insert {
            Ok(_) => {
                append_audit_tx(
                    &mut tx,
                    "run",
                    &id,
                    "run.queued",
                    serde_json::json!({"trigger": "cron"}),
                )
                .await?;
                id
            }
            Err(error) if is_unique_violation(&error) => sqlx::query_scalar::<_, String>(
                "SELECT id FROM runs WHERE schedule_id=? AND scheduled_at=? AND trigger_kind='cron'",
            )
            .bind(&schedule_id)
            .bind(&occurrence)
            .fetch_one(&mut *tx)
            .await?,
            Err(error) => {
                tx.rollback().await?;
                return Err(error.into());
            }
        };

        let row = sqlx::query("SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at FROM runs WHERE id=?")
            .bind(run_id)
            .fetch_one(&mut *tx)
            .await?;
        let run = run_view_from_row(row)?;
        tx.commit().await?;
        Ok(CronOccurrenceResult::Applied(run))
    }

    pub async fn create_run(&self, new: NewRun) -> Result<RunView> {
        let now = now_string();
        let id = new.id.to_string();
        let idempotency_key = new.idempotency_key.clone();
        let trigger_identity_id = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin().await?;
        let trigger_insert = sqlx::query(
            "INSERT INTO trigger_identities(id,schedule_id,trigger_kind,scheduled_at,idempotency_key,target_kind,target_id,created_at) VALUES (?,?,?,?,?,'run',?,?)",
        )
        .bind(trigger_identity_id)
        .bind(new.schedule_id.to_string())
        .bind(&new.trigger_kind)
        .bind(format_time(new.scheduled_at))
        .bind(&new.idempotency_key)
        .bind(&id)
        .bind(&now)
        .execute(&mut *tx)
        .await;
        if let Err(error) = trigger_insert {
            if is_unique_violation(&error) {
                tx.rollback().await?;
                let target = if let Some(key) = idempotency_key.as_deref() {
                    self.trigger_target_for_idempotency(new.schedule_id, key)
                        .await?
                } else if new.trigger_kind == "cron" {
                    self.trigger_target_for_cron(new.schedule_id, new.scheduled_at)
                        .await?
                } else {
                    None
                };
                let (kind, target_id) =
                    target.context("trigger identity conflict has no target")?;
                if kind != "run" {
                    bail!("trigger was already materialized as a collection batch");
                }
                return self
                    .get_run(Uuid::parse_str(&target_id)?)
                    .await?
                    .context("trigger identity points to a missing run");
            }
            tx.rollback().await?;
            return Err(error.into());
        }
        let insert = sqlx::query(
            "INSERT INTO runs(id,schedule_id,state,trigger_kind,scheduled_at,not_before,encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,backoff_cap_seconds,idempotency_key,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&id)
        .bind(new.schedule_id.to_string())
        .bind("queued")
        .bind(&new.trigger_kind)
        .bind(format_time(new.scheduled_at))
        .bind(format_time(new.scheduled_at))
        .bind(new.encrypted_snapshot)
        .bind(new.key_id)
        .bind(new.max_attempts)
        .bind(new.initial_backoff_seconds as i64)
        .bind(new.backoff_cap_seconds as i64)
        .bind(&new.idempotency_key)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await;
        match insert {
            Ok(_) => {}
            Err(error) if is_unique_violation(&error) => {
                tx.rollback().await?;
                if let Some(key) = idempotency_key.as_deref()
                    && let Some(existing) =
                        self.get_run_by_idempotency(new.schedule_id, key).await?
                {
                    return Ok(existing);
                }
                if let Some(existing) = self
                    .find_existing_run(new.schedule_id, new.scheduled_at, &new.trigger_kind)
                    .await?
                {
                    return Ok(existing);
                }
                return Err(error.into());
            }
            Err(error) => {
                tx.rollback().await?;
                return Err(error.into());
            }
        }
        append_audit_tx(
            &mut tx,
            "run",
            &id,
            "run.queued",
            serde_json::json!({"trigger": new.trigger_kind}),
        )
        .await?;
        tx.commit().await?;
        self.get_run(new.id).await?.context("created run missing")
    }

    async fn find_existing_run(
        &self,
        schedule_id: Uuid,
        scheduled_at: DateTime<Utc>,
        trigger: &str,
    ) -> Result<Option<RunView>> {
        let row = sqlx::query("SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at FROM runs WHERE schedule_id=? AND scheduled_at=? AND trigger_kind=?")
            .bind(schedule_id.to_string())
            .bind(format_time(scheduled_at))
            .bind(trigger)
            .fetch_optional(&self.pool)
            .await?;
        row.map(run_view_from_row).transpose()
    }

    async fn trigger_target_for_cron(
        &self,
        schedule_id: Uuid,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Option<(String, String)>> {
        let row = sqlx::query(
            "SELECT target_kind,target_id FROM trigger_identities WHERE schedule_id=? AND trigger_kind='cron' AND scheduled_at=?",
        )
        .bind(schedule_id.to_string())
        .bind(format_time(scheduled_at))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| (row.get("target_kind"), row.get("target_id"))))
    }

    async fn trigger_target_for_idempotency(
        &self,
        schedule_id: Uuid,
        key: &str,
    ) -> Result<Option<(String, String)>> {
        let row = sqlx::query(
            "SELECT target_kind,target_id FROM trigger_identities WHERE schedule_id=? AND idempotency_key=?",
        )
        .bind(schedule_id.to_string())
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| (row.get("target_kind"), row.get("target_id"))))
    }

    pub async fn get_run_by_idempotency(
        &self,
        schedule_id: Uuid,
        key: &str,
    ) -> Result<Option<RunView>> {
        let row = sqlx::query("SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at FROM runs WHERE schedule_id=? AND idempotency_key=?")
            .bind(schedule_id.to_string())
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        row.map(run_view_from_row).transpose()
    }

    pub async fn list_runs(&self, limit: u32) -> Result<Vec<RunView>> {
        let rows = sqlx::query("SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at FROM runs ORDER BY created_at DESC LIMIT ?")
            .bind(limit.min(1_000))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(run_view_from_row).collect()
    }

    pub async fn list_runs_page(
        &self,
        cursor_created_at: Option<&str>,
        cursor_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<RunView>> {
        let rows = sqlx::query(
            "SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at \
             FROM runs WHERE (? IS NULL OR created_at<? OR (created_at=? AND id<?)) \
             ORDER BY created_at DESC,id DESC LIMIT ?",
        )
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_created_at)
        .bind(cursor_id)
        .bind(i64::from(limit.clamp(1, 201)))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(run_view_from_row).collect()
    }

    pub async fn get_run(&self, id: Uuid) -> Result<Option<RunView>> {
        let row = sqlx::query("SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at FROM runs WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(run_view_from_row).transpose()
    }

    pub async fn run_attempts(&self, run_id: Uuid) -> Result<Vec<AttemptView>> {
        let rows = sqlx::query("SELECT id,run_id,agent_id,attempt_number,state,outcome,exit_code,signal,duration_ms,diagnostic_json,output_metadata_json,accepted_at,finished_at,created_at FROM attempts WHERE run_id=? ORDER BY attempt_number")
            .bind(run_id.to_string())
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(attempt_view_from_row).collect()
    }

    pub async fn run_attempts_page(
        &self,
        run_id: Uuid,
        cursor_attempt_number: Option<u32>,
        cursor_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<AttemptView>> {
        let rows = sqlx::query(
            "SELECT id,run_id,agent_id,attempt_number,state,outcome,exit_code,signal,duration_ms,diagnostic_json,output_metadata_json,accepted_at,finished_at,created_at \
             FROM attempts WHERE run_id=? AND (? IS NULL OR attempt_number>? OR (attempt_number=? AND id>?)) \
             ORDER BY attempt_number,id LIMIT ?",
        )
        .bind(run_id.to_string())
        .bind(cursor_attempt_number.map(i64::from))
        .bind(cursor_attempt_number.map(i64::from))
        .bind(cursor_id)
        .bind(i64::from(limit.clamp(1, 201)))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(attempt_view_from_row).collect()
    }

    pub async fn get_run_record(&self, id: Uuid) -> Result<Option<RunRecord>> {
        let row = sqlx::query("SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at,encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,backoff_cap_seconds,not_before FROM runs WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(run_record_from_row).transpose()
    }

    pub async fn queued_runs(&self, limit: u32) -> Result<Vec<RunRecord>> {
        // Rank within each schedule so a large collection cannot monopolize a
        // dispatch pass. A second rank fences each batch to its configured
        // active cap, accounting for attempts which are already running.
        let rows = sqlx::query(
            "WITH eligible AS (
                SELECT r.id,r.schedule_id,r.state,r.trigger_kind,r.scheduled_at,r.attempt_count,
                       r.created_at,r.updated_at,r.encrypted_snapshot,r.key_id,r.max_attempts,
                       r.initial_backoff_seconds,r.backoff_cap_seconds,r.not_before,r.batch_id,
                       b.max_active_runs,
                       ROW_NUMBER() OVER (
                           PARTITION BY r.schedule_id ORDER BY r.created_at,r.id
                       ) AS schedule_rank,
                       ROW_NUMBER() OVER (
                           PARTITION BY r.batch_id ORDER BY r.created_at,r.id
                       ) AS batch_rank,
                       CASE WHEN r.batch_id IS NULL THEN 0 ELSE (
                           SELECT COUNT(*) FROM runs running_run
                           WHERE running_run.batch_id=r.batch_id AND running_run.state='running'
                       ) END AS batch_running
                FROM runs r LEFT JOIN batches b ON b.id=r.batch_id
                WHERE r.state='queued' AND r.not_before<=? AND NOT EXISTS (
                    SELECT 1 FROM attempts pending
                    LEFT JOIN health_evidence classified ON classified.attempt_id=pending.id
                    WHERE pending.run_id=r.id AND pending.state='finished'
                      AND COALESCE(pending.outcome,'') NOT IN ('succeeded','cancelled')
                      AND (pending.diagnostic_json IS NULL OR
                           json_extract(pending.diagnostic_json,'$.code') NOT IN (
                             'process_exited_non_zero','excel_macro_returned_failure'
                           ))
                      AND classified.id IS NULL
                )
            )
            SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at,
                   encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,
                   backoff_cap_seconds,not_before
            FROM eligible
            WHERE batch_id IS NULL OR batch_rank<=MAX(0,max_active_runs-batch_running)
            ORDER BY schedule_rank,created_at,id LIMIT ?",
        )
            .bind(now_string())
            .bind(limit.min(500))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(run_record_from_row).collect()
    }

    pub async fn queue_depth(&self) -> Result<QueueDepth> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS total,\
             COALESCE(SUM(CASE WHEN not_before<=? AND NOT EXISTS (\
               SELECT 1 FROM attempts pending \
               LEFT JOIN health_evidence classified ON classified.attempt_id=pending.id \
               WHERE pending.run_id=runs.id AND pending.state='finished' \
                 AND COALESCE(pending.outcome,'') NOT IN ('succeeded','cancelled') \
                 AND (pending.diagnostic_json IS NULL OR \
                      json_extract(pending.diagnostic_json,'$.code') NOT IN (\
                        'process_exited_non_zero','excel_macro_returned_failure'\
                      )) AND classified.id IS NULL\
             ) THEN 1 ELSE 0 END),0) AS ready \
             FROM runs WHERE state='queued'",
        )
        .bind(now_string())
        .fetch_one(&self.pool)
        .await?;
        let total = u64::try_from(row.get::<i64, _>("total")).unwrap_or_default();
        let ready = u64::try_from(row.get::<i64, _>("ready")).unwrap_or_default();
        Ok(QueueDepth {
            ready,
            delayed: total.saturating_sub(ready),
        })
    }

    pub async fn create_attempt(
        &self,
        run_id: Uuid,
        agent_id: &str,
        lease_seconds: u64,
    ) -> Result<Option<AttemptRecord>> {
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT state,attempt_count,(SELECT COALESCE(MAX(attempt_number),0) FROM attempts WHERE run_id=runs.id) AS max_attempt_number FROM runs WHERE id=?")
            .bind(run_id.to_string())
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(None);
        };
        if row.get::<String, _>("state") != "queued" {
            tx.rollback().await?;
            return Ok(None);
        }
        let attempt_number = row.get::<i64, _>("max_attempt_number") as u32 + 1;
        let id = Uuid::new_v4();
        let lease_token = Uuid::new_v4().to_string();
        let expires = now + Duration::seconds(lease_seconds as i64);
        sqlx::query("INSERT INTO attempts(id,run_id,agent_id,attempt_number,lease_token,state,lease_expires_at,created_at,updated_at) VALUES (?,?,?,?,?,'offered',?,?,?)")
            .bind(id.to_string())
            .bind(run_id.to_string())
            .bind(agent_id)
            .bind(attempt_number)
            .bind(&lease_token)
            .bind(format_time(expires))
            .bind(format_time(now))
            .bind(format_time(now))
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE runs SET state='running',updated_at=? WHERE id=? AND state='queued'")
            .bind(format_time(now))
            .bind(run_id.to_string())
            .execute(&mut *tx)
            .await?;
        collection::sync_batch_run_state_tx(
            &mut tx,
            &run_id.to_string(),
            "running",
            &format_time(now),
        )
        .await?;
        append_audit_tx(
            &mut tx,
            "run",
            &run_id.to_string(),
            "attempt.offered",
            serde_json::json!({"attempt_id": id, "agent_id": agent_id}),
        )
        .await?;
        tx.commit().await?;
        Ok(Some(AttemptRecord {
            id,
            run_id,
            agent_id: agent_id.into(),
            attempt_number,
            lease_token,
            lease_expires_at: expires,
        }))
    }

    pub async fn release_offer(&self, attempt_id: Uuid) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT run_id FROM attempts WHERE id=? AND state='offered'")
            .bind(attempt_id.to_string())
            .fetch_optional(&mut *tx)
            .await?;
        if let Some(row) = row {
            let run_id: String = row.get("run_id");
            sqlx::query("UPDATE attempts SET state='offer_failed',updated_at=? WHERE id=?")
                .bind(now_string())
                .bind(attempt_id.to_string())
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "UPDATE runs SET state='queued',updated_at=? WHERE id=? AND state='running'",
            )
            .bind(now_string())
            .bind(&run_id)
            .execute(&mut *tx)
            .await?;
            collection::sync_batch_run_state_tx(&mut tx, &run_id, "queued", &now_string()).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn reject_offer(
        &self,
        attempt_id: Uuid,
        lease_token: &str,
        result: &ExecutionResult,
        encrypted_result: Vec<u8>,
        key_id: &str,
    ) -> Result<()> {
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT run_id FROM attempts WHERE id=? AND lease_token=? AND state='offered'",
        )
        .bind(attempt_id.to_string())
        .bind(lease_token)
        .fetch_optional(&mut *tx)
        .await?
        .context("attempt offer or lease token is invalid")?;
        let run_id: String = row.get("run_id");
        let diagnostic_json = result
            .diagnostic
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        sqlx::query(
            "UPDATE attempts SET state='finished',outcome=?,encrypted_result=?,result_key_id=?,\
             diagnostic_json=?,output_metadata_json=?,duration_ms=0,started_at=?,finished_at=?,updated_at=? \
             WHERE id=? AND state='offered'",
        )
        .bind(result.outcome.as_str())
        .bind(encrypted_result)
        .bind(key_id)
        .bind(&diagnostic_json)
        .bind(serde_json::to_string(&result.output)?)
        .bind(format_time(now))
        .bind(format_time(now))
        .bind(format_time(now))
        .bind(attempt_id.to_string())
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE runs SET state='queued',not_before=?,updated_at=? WHERE id=? AND state='running'")
            .bind(format_time(now))
            .bind(format_time(now))
            .bind(&run_id)
            .execute(&mut *tx)
            .await?;
        collection::sync_batch_run_state_tx(&mut tx, &run_id, "queued", &format_time(now)).await?;
        append_audit_tx(
            &mut tx,
            "run",
            &run_id,
            "attempt.rejected_before_acceptance",
            serde_json::json!({
                "attempt_id": attempt_id,
                "diagnostic": result.diagnostic,
            }),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn accept_attempt(
        &self,
        attempt_id: Uuid,
        lease_token: &str,
        lease_seconds: u64,
    ) -> Result<()> {
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT run_id FROM attempts WHERE id=? AND lease_token=? AND state IN ('offered','accepted')")
            .bind(attempt_id.to_string()).bind(lease_token).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.rollback().await?;
            bail!("attempt or lease token is invalid");
        };
        let run_id: String = row.get("run_id");
        sqlx::query("UPDATE attempts SET state='accepted',accepted_at=COALESCE(accepted_at,?),lease_expires_at=?,updated_at=? WHERE id=?")
            .bind(format_time(now))
            .bind(format_time(now + Duration::seconds(lease_seconds as i64)))
            .bind(format_time(now))
            .bind(attempt_id.to_string())
            .execute(&mut *tx).await?;
        sqlx::query("UPDATE runs SET attempt_count=MAX(0,(SELECT COUNT(*) FROM attempts WHERE run_id=? AND accepted_at IS NOT NULL)-attempt_offset),updated_at=? WHERE id=?")
            .bind(&run_id).bind(format_time(now)).bind(&run_id).execute(&mut *tx).await?;
        append_audit_tx(
            &mut tx,
            "run",
            &run_id,
            "attempt.accepted",
            serde_json::json!({"attempt_id": attempt_id}),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn renew_attempts(
        &self,
        agent_id: &str,
        attempt_ids: &[String],
        lease_seconds: u64,
        running: u32,
    ) -> Result<Vec<String>> {
        let expires = format_time(Utc::now() + Duration::seconds(lease_seconds as i64));
        let mut tx = self.pool.begin().await?;
        let mut renewed = Vec::new();
        for attempt_id in attempt_ids {
            let result = sqlx::query("UPDATE attempts SET lease_expires_at=?,updated_at=? WHERE id=? AND agent_id=? AND state='accepted'")
                .bind(&expires).bind(now_string()).bind(attempt_id).bind(agent_id).execute(&mut *tx).await?;
            if result.rows_affected() == 1 {
                renewed.push(attempt_id.clone());
            }
        }
        sqlx::query(
            "UPDATE agents SET running=?,last_seen_at=?,updated_at=?,connected=1 WHERE id=?",
        )
        .bind(running)
        .bind(now_string())
        .bind(now_string())
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(renewed)
    }

    /// Atomically reauthorizes a durable agent-side assignment after restart.
    /// A cancelled, completed, replaced, expired, or differently-owned attempt
    /// can never be resurrected by replaying an old local ledger row.
    pub async fn reauthorize_attempt(
        &self,
        attempt_id: Uuid,
        agent_id: &str,
        lease_token: &str,
        lease_seconds: u64,
    ) -> Result<bool> {
        let now = Utc::now();
        let expires = now + Duration::seconds(lease_seconds as i64);
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT a.lease_expires_at FROM attempts a JOIN runs r ON r.id=a.run_id \
             WHERE a.id=? AND a.agent_id=? AND a.lease_token=? AND a.state='accepted' \
             AND r.state='running'",
        )
        .bind(attempt_id.to_string())
        .bind(agent_id)
        .bind(lease_token)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(false);
        };
        let previous_expiry_text: String = row.get("lease_expires_at");
        let previous_expiry = parse_time(previous_expiry_text.clone())?;
        if previous_expiry <= now {
            tx.rollback().await?;
            return Ok(false);
        }
        let result = sqlx::query(
            "UPDATE attempts SET lease_expires_at=?,updated_at=? \
             WHERE id=? AND agent_id=? AND lease_token=? AND state='accepted' \
             AND lease_expires_at=?",
        )
        .bind(format_time(expires))
        .bind(format_time(now))
        .bind(attempt_id.to_string())
        .bind(agent_id)
        .bind(lease_token)
        .bind(previous_expiry_text)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn finish_attempt(
        &self,
        attempt_id: Uuid,
        lease_token: &str,
        result: &ExecutionResult,
        encrypted_result: Vec<u8>,
        key_id: &str,
    ) -> Result<RunState> {
        self.finish_attempt_inner(
            attempt_id,
            lease_token,
            result,
            encrypted_result,
            key_id,
            false,
        )
        .await?
        .context("attempt unexpectedly stopped being eligible")
    }

    /// Finishes an attempt previously claimed by `claim_expired_attempts`.
    pub async fn finish_expired_attempt(
        &self,
        attempt_id: Uuid,
        lease_token: &str,
        result: &ExecutionResult,
        encrypted_result: Vec<u8>,
        key_id: &str,
    ) -> Result<Option<RunState>> {
        self.finish_attempt_inner(
            attempt_id,
            lease_token,
            result,
            encrypted_result,
            key_id,
            true,
        )
        .await
    }

    async fn finish_attempt_inner(
        &self,
        attempt_id: Uuid,
        lease_token: &str,
        result: &ExecutionResult,
        encrypted_result: Vec<u8>,
        key_id: &str,
        require_expired_lease: bool,
    ) -> Result<Option<RunState>> {
        let now = Utc::now();
        let outcome = result.outcome.as_str();
        let diagnostic_json = result
            .diagnostic
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let output_metadata_json = serde_json::to_string(&result.output)?;
        let duration_ms = (result.finished_at - result.started_at)
            .num_milliseconds()
            .max(0) as u64;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT a.run_id,a.agent_id,a.state AS attempt_state,r.attempt_count,r.max_attempts,r.initial_backoff_seconds,r.backoff_cap_seconds,r.state AS run_state FROM attempts a JOIN runs r ON r.id=a.run_id WHERE a.id=? AND a.lease_token=?")
            .bind(attempt_id.to_string()).bind(lease_token).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.rollback().await?;
            bail!("attempt or lease token is invalid");
        };
        let run_id: String = row.get("run_id");
        let agent_id: String = row.get("agent_id");
        let attempt_state: String = row.get("attempt_state");
        let current_state: String = row.get("run_state");
        if require_expired_lease && attempt_state != "expiring" {
            tx.rollback().await?;
            return Ok(None);
        }
        if !require_expired_lease && attempt_state == "expiring" {
            // The lease-expiry worker won the race. A result arriving after the
            // atomic claim is acknowledged but cannot restore the lost lease.
            tx.rollback().await?;
            return parse_state(&current_state).map(Some);
        }
        if matches!(attempt_state.as_str(), "finished" | "late_result") {
            tx.rollback().await?;
            return parse_state(&current_state).map(Some);
        }
        if current_state == "cancelled" {
            sqlx::query("UPDATE attempts SET state='late_result',outcome=?,encrypted_result=?,result_key_id=?,diagnostic_json=?,output_metadata_json=?,exit_code=?,signal=?,duration_ms=?,started_at=?,finished_at=?,updated_at=? WHERE id=?")
                .bind(outcome).bind(encrypted_result).bind(key_id).bind(&diagnostic_json).bind(&output_metadata_json).bind(result.exit_code).bind(&result.signal).bind(duration_ms as i64).bind(format_time(result.started_at)).bind(format_time(result.finished_at)).bind(format_time(now)).bind(attempt_id.to_string()).execute(&mut *tx).await?;
            update_node_processing_stats_tx(&mut tx, &agent_id, result).await?;
            append_audit_tx(
                &mut tx,
                "run",
                &run_id,
                "attempt.late_result",
                serde_json::json!({"attempt_id": attempt_id, "outcome": outcome, "diagnostic": result.diagnostic, "exit_code": result.exit_code, "signal": result.signal, "duration_ms": duration_ms}),
            )
            .await?;
            tx.commit().await?;
            return Ok(Some(RunState::Cancelled));
        }
        sqlx::query("UPDATE attempts SET state='finished',outcome=?,encrypted_result=?,result_key_id=?,diagnostic_json=?,output_metadata_json=?,exit_code=?,signal=?,duration_ms=?,started_at=?,finished_at=?,updated_at=? WHERE id=?")
            .bind(outcome).bind(encrypted_result).bind(key_id).bind(&diagnostic_json).bind(&output_metadata_json).bind(result.exit_code).bind(&result.signal).bind(duration_ms as i64).bind(format_time(result.started_at)).bind(format_time(result.finished_at)).bind(format_time(now)).bind(attempt_id.to_string()).execute(&mut *tx).await?;
        update_node_processing_stats_tx(&mut tx, &agent_id, result).await?;
        let successful = outcome == "succeeded";
        let attempt_count = row.get::<i64, _>("attempt_count") as u32;
        let max_attempts = row.get::<i64, _>("max_attempts") as u32;
        let state = if successful {
            RunState::Succeeded
        } else if attempt_count < max_attempts {
            RunState::Queued
        } else {
            RunState::Failed
        };
        let initial = row.get::<i64, _>("initial_backoff_seconds") as u64;
        let cap = row.get::<i64, _>("backoff_cap_seconds") as u64;
        let exponential = initial.saturating_mul(1_u64 << attempt_count.saturating_sub(1).min(20));
        let base = exponential.min(cap);
        let jitter = rand::thread_rng().gen_range(0..=(base / 5).max(1));
        let not_before = now + Duration::seconds((base + jitter) as i64);
        sqlx::query("UPDATE runs SET state=?,not_before=?,updated_at=? WHERE id=?")
            .bind(state.as_str())
            .bind(format_time(not_before))
            .bind(format_time(now))
            .bind(&run_id)
            .execute(&mut *tx)
            .await?;
        update_daily_schedule_stats_tx(&mut tx, &run_id, state, result, duration_ms).await?;
        collection::sync_batch_run_state_tx(&mut tx, &run_id, state.as_str(), &format_time(now))
            .await?;
        append_audit_tx(
            &mut tx,
            "run",
            &run_id,
            if successful {
                "run.succeeded"
            } else if state == RunState::Queued {
                "run.retry_scheduled"
            } else {
                "run.failed"
            },
            serde_json::json!({"attempt_id": attempt_id, "outcome": outcome, "diagnostic": result.diagnostic, "exit_code": result.exit_code, "signal": result.signal, "duration_ms": duration_ms}),
        )
        .await?;
        tx.commit().await?;
        Ok(Some(state))
    }

    pub async fn claim_expired_attempts(
        &self,
        limit: u32,
        heartbeat_grace_seconds: u64,
    ) -> Result<Vec<AttemptRecord>> {
        // Offers also need a deadline. If the transport delivers an assignment but
        // the durable acceptance acknowledgement is lost, leaving it in `offered`
        // forever would wedge the run in `running`. Expiring it does not consume an
        // accepted-attempt retry because attempt_count is derived from accepted_at.
        // Read the exact deadline and then claim it with compare-and-swap. If a
        // concurrent heartbeat changes that deadline, the UPDATE affects no row;
        // if this UPDATE changes the state first, the heartbeat's `accepted`
        // predicate affects no row. This avoids a stale expiry scan revoking a
        // lease which was renewed while SQLite was scheduling the two writers.
        let rows = sqlx::query("SELECT id,run_id,agent_id,attempt_number,lease_token,lease_expires_at FROM attempts WHERE state IN ('offered','accepted') ORDER BY lease_expires_at LIMIT ?")
            .bind(limit.min(500))
            .fetch_all(&self.pool)
            .await?;
        let now = Utc::now();
        let grace = Duration::seconds(heartbeat_grace_seconds.min(i64::MAX as u64) as i64);
        let mut claimed = Vec::new();
        for row in rows {
            let lease_expires_at_text: String = row.get("lease_expires_at");
            let lease_expires_at = parse_time(lease_expires_at_text.clone())?;
            if lease_expires_at + grace >= now {
                continue;
            }
            let attempt = attempt_from_row(row)?;
            let result = sqlx::query("UPDATE attempts SET state='expiring',updated_at=? WHERE id=? AND lease_token=? AND state IN ('offered','accepted') AND lease_expires_at=?")
                .bind(format_time(now))
                .bind(attempt.id.to_string())
                .bind(&attempt.lease_token)
                .bind(lease_expires_at_text)
                .execute(&self.pool)
                .await?;
            if result.rows_affected() == 1 {
                claimed.push(attempt);
            }
        }
        Ok(claimed)
    }

    pub async fn cancel_run(&self, run_id: Uuid) -> Result<Vec<(String, Uuid)>> {
        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        let row = sqlx::query("UPDATE runs SET state='cancelled',updated_at=? WHERE id=? AND state IN ('queued','running') RETURNING schedule_id")
            .bind(format_time(now)).bind(run_id.to_string()).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            tx.rollback().await?;
            bail!("run is already terminal or does not exist");
        };
        increment_cancelled_rollup_tx(&mut tx, row.get("schedule_id"), now).await?;
        collection::sync_batch_run_state_tx(
            &mut tx,
            &run_id.to_string(),
            "cancelled",
            &now_string(),
        )
        .await?;
        let rows =
            sqlx::query("SELECT agent_id,id FROM attempts WHERE run_id=? AND state='accepted'")
                .bind(run_id.to_string())
                .fetch_all(&mut *tx)
                .await?;
        append_audit_tx(
            &mut tx,
            "run",
            &run_id.to_string(),
            "run.cancelled",
            Value::Null,
        )
        .await?;
        tx.commit().await?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.get("agent_id"),
                    Uuid::parse_str(row.get::<String, _>("id").as_str())?,
                ))
            })
            .collect()
    }

    pub async fn retry_run(&self, run_id: Uuid) -> Result<()> {
        let now = now_string();
        let mut tx = self.pool.begin().await?;
        decrement_failed_rollup_tx(&mut tx, &run_id.to_string()).await?;
        let result = sqlx::query("UPDATE runs SET state='queued',attempt_count=0,attempt_offset=(SELECT COUNT(*) FROM attempts WHERE run_id=? AND accepted_at IS NOT NULL),not_before=?,updated_at=? WHERE id=? AND state='failed'")
            .bind(run_id.to_string())
            .bind(&now).bind(&now).bind(run_id.to_string()).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            tx.rollback().await?;
            bail!("only failed runs can be retried");
        }
        collection::sync_batch_run_state_tx(&mut tx, &run_id.to_string(), "queued", &now).await?;
        append_audit_tx(
            &mut tx,
            "run",
            &run_id.to_string(),
            "run.retried",
            Value::Null,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_agent(
        &self,
        id: &str,
        hostname: &str,
        labels: &std::collections::HashMap<String, String>,
        capacity: u32,
        running: u32,
    ) -> Result<AgentRegistration> {
        let now = now_string();
        let labels_json = serde_json::to_string(labels)?;
        let mut settings = NodeSettings {
            labels: labels.clone().into_iter().collect(),
            max_parallel: capacity,
            ..NodeSettings::default()
        };
        let key = format!("node:{id}");
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT OR IGNORE INTO settings_documents(document_key,document_json,revision,updated_at) VALUES (?,?,1,?)")
            .bind(&key).bind(serde_json::to_string(&settings)?).bind(&now).execute(&mut *tx).await?;
        let row = sqlx::query(
            "SELECT document_json,revision FROM settings_documents WHERE document_key=?",
        )
        .bind(&key)
        .fetch_one(&mut *tx)
        .await?;
        settings = serde_json::from_str(row.get("document_json"))?;
        settings.revision = row.get("revision");
        sqlx::query("INSERT INTO agents(id,hostname,labels_json,capacity,running,connected,desired_settings_revision,last_seen_at,created_at,updated_at) VALUES (?,?,?,?,?,1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET hostname=excluded.hostname,labels_json=excluded.labels_json,capacity=excluded.capacity,running=excluded.running,connected=1,last_seen_at=excluded.last_seen_at,updated_at=excluded.updated_at")
            .bind(id).bind(hostname).bind(labels_json).bind(capacity).bind(running).bind(settings.revision).bind(&now).bind(&now).bind(&now).execute(&mut *tx).await?;
        sqlx::query(
            "INSERT OR IGNORE INTO node_health(agent_id,state,transitioned_at,updated_at) \
             VALUES (?,'healthy',?,?)",
        )
        .bind(id)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        append_audit_tx(
            &mut tx,
            "agent",
            id,
            "agent.connected",
            serde_json::json!({"capacity": capacity}),
        )
        .await?;
        let applied_settings_revision =
            sqlx::query_scalar::<_, i64>("SELECT applied_settings_revision FROM agents WHERE id=?")
                .bind(id)
                .fetch_one(&mut *tx)
                .await?;
        tx.commit().await?;
        Ok(AgentRegistration {
            desired_settings: settings,
            applied_settings_revision,
        })
    }

    pub async fn disconnect_agent(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE agents SET connected=0,updated_at=? WHERE id=?")
            .bind(now_string())
            .bind(id)
            .execute(&self.pool)
            .await?;
        self.append_audit("agent", id, "agent.disconnected", Value::Null)
            .await
    }

    pub async fn settings_applied(
        &self,
        id: &str,
        revision: i64,
        error: Option<&str>,
    ) -> Result<SettingsAckOutcome> {
        let now = now_string();
        let mut tx = self.pool.begin().await?;
        let affected = if let Some(error) = error {
            // A rejection is current only while this exact revision remains
            // desired and no success for it has already been recorded.
            sqlx::query("UPDATE agents SET settings_error=?,updated_at=? WHERE id=? AND desired_settings_revision=? AND applied_settings_revision<?")
                .bind(error)
                .bind(&now)
                .bind(id)
                .bind(revision)
                .bind(revision)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        } else {
            // Only the current desired revision may advance applied state. The
            // final predicate prevents an out-of-order ACK from regressing it.
            sqlx::query("UPDATE agents SET applied_settings_revision=?,settings_error=NULL,updated_at=? WHERE id=? AND desired_settings_revision=? AND applied_settings_revision<=?")
                .bind(revision)
                .bind(&now)
                .bind(id)
                .bind(revision)
                .bind(revision)
                .execute(&mut *tx)
                .await?
                .rows_affected()
        };
        let current = sqlx::query(
            "SELECT desired_settings_revision,applied_settings_revision,settings_error FROM agents WHERE id=?",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .with_context(|| format!("agent {id} not found"))?;
        let desired_revision: i64 = current.get("desired_settings_revision");
        let applied_revision: i64 = current.get("applied_settings_revision");
        let current_error: Option<String> = current.get("settings_error");

        let (status, event_type, metadata) = if affected == 0 {
            (
                SettingsAckStatus::Ignored,
                "settings.ack_ignored",
                serde_json::json!({
                    "revision": revision,
                    "desired_revision": desired_revision,
                    "applied_revision": applied_revision,
                    "acknowledged_success": error.is_none(),
                    "current_error": current_error.is_some(),
                }),
            )
        } else if error.is_some() {
            (
                SettingsAckStatus::Rejected,
                "settings.rejected",
                serde_json::json!({"revision": revision}),
            )
        } else {
            (
                SettingsAckStatus::Applied,
                "settings.applied",
                serde_json::json!({"revision": revision}),
            )
        };
        let applied_settings = if status == SettingsAckStatus::Applied {
            let row = sqlx::query(
                "SELECT document_json,revision FROM settings_documents WHERE document_key=? AND revision=?",
            )
            .bind(format!("node:{id}"))
            .bind(applied_revision)
            .fetch_optional(&mut *tx)
            .await?
            .with_context(|| {
                format!(
                    "applied settings document for agent {id} revision {applied_revision} not found"
                )
            })?;
            let mut settings: NodeSettings = serde_json::from_str(row.get("document_json"))?;
            settings.revision = row.get("revision");
            sqlx::query(
                "UPDATE agents SET labels_json=?,capacity=? WHERE id=? AND desired_settings_revision=? AND applied_settings_revision=?",
            )
            .bind(serde_json::to_string(&settings.labels)?)
            .bind(i64::from(settings.max_parallel))
            .bind(id)
            .bind(settings.revision)
            .bind(settings.revision)
            .execute(&mut *tx)
            .await?;
            Some(settings)
        } else {
            None
        };
        append_audit_tx(&mut tx, "agent", id, event_type, metadata).await?;
        tx.commit().await?;
        Ok(SettingsAckOutcome {
            status,
            desired_revision,
            applied_revision,
            applied_settings,
        })
    }

    pub async fn list_agents(&self) -> Result<Vec<AgentView>> {
        let rows = sqlx::query("SELECT id,hostname,labels_json,capacity,running,connected,desired_settings_revision,applied_settings_revision,settings_error,last_seen_at FROM agents ORDER BY id")
            .fetch_all(&self.pool).await?;
        rows.into_iter().map(agent_from_row).collect()
    }

    pub async fn list_agents_page(
        &self,
        cursor_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<AgentView>> {
        let rows = sqlx::query(
            "SELECT id,hostname,labels_json,capacity,running,connected,desired_settings_revision,applied_settings_revision,settings_error,last_seen_at \
             FROM agents WHERE (? IS NULL OR id>?) ORDER BY id LIMIT ?",
        )
        .bind(cursor_id)
        .bind(cursor_id)
        .bind(i64::from(limit.clamp(1, 201)))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(agent_from_row).collect()
    }

    pub async fn get_global_settings(&self) -> Result<GlobalSettings> {
        let row = sqlx::query(
            "SELECT document_json,revision FROM settings_documents WHERE document_key='global'",
        )
        .fetch_one(&self.pool)
        .await?;
        let mut settings: GlobalSettings = serde_json::from_str(row.get("document_json"))?;
        settings.revision = row.get("revision");
        Ok(settings)
    }

    pub async fn get_node_settings(&self, id: &str) -> Result<Option<NodeSettings>> {
        let row = sqlx::query(
            "SELECT document_json,revision FROM settings_documents WHERE document_key=?",
        )
        .bind(format!("node:{id}"))
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let mut settings: NodeSettings = serde_json::from_str(row.get("document_json"))?;
            settings.revision = row.get("revision");
            Ok(settings)
        })
        .transpose()
    }

    pub async fn update_settings(
        &self,
        document_key: &str,
        expected_revision: i64,
        document_json: &str,
        lock_token: &str,
    ) -> Result<i64> {
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;
        let lock = sqlx::query("SELECT lock_token,expires_at FROM edit_locks WHERE document_key=?")
            .bind(document_key)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(lock) = lock else {
            // Await the rollback before returning a normal business error. Relying on
            // Transaction::drop schedules the rollback asynchronously; under writer
            // contention the next pooled connection can otherwise observe SQLITE_BUSY.
            tx.rollback().await?;
            bail!("settings edit lock is required");
        };
        if lock.get::<String, _>("lock_token") != lock_token
            || parse_time(lock.get("expires_at"))? <= now
        {
            tx.rollback().await?;
            bail!("settings edit lock is invalid or expired");
        }
        let result = sqlx::query("UPDATE settings_documents SET document_json=?,revision=revision+1,updated_at=? WHERE document_key=? AND revision=?")
            .bind(document_json).bind(format_time(now)).bind(document_key).bind(expected_revision).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            tx.rollback().await?;
            bail!("settings revision conflict");
        }
        let revision = expected_revision + 1;
        if let Some(agent_id) = document_key.strip_prefix("node:") {
            sqlx::query("UPDATE agents SET desired_settings_revision=?,settings_error=NULL,updated_at=? WHERE id=?")
                .bind(revision)
                .bind(format_time(now))
                .bind(agent_id)
                .execute(&mut *tx)
                .await?;
        }
        append_audit_tx(
            &mut tx,
            "settings",
            document_key,
            "settings.updated",
            serde_json::json!({"revision": revision}),
        )
        .await?;
        tx.commit().await?;
        Ok(revision)
    }

    pub async fn acquire_lock(&self, document_key: &str, owner: &str) -> Result<EditLock> {
        let now = Utc::now();
        let expires = now + Duration::minutes(2);
        let token = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM edit_locks WHERE document_key=? AND expires_at<=?")
            .bind(document_key)
            .bind(format_time(now))
            .execute(&mut *tx)
            .await?;
        let result = sqlx::query("INSERT INTO edit_locks(document_key,owner_session,lock_token,expires_at) VALUES (?,?,?,?) ON CONFLICT(document_key) DO NOTHING")
            .bind(document_key).bind(owner).bind(&token).bind(format_time(expires)).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            let row = sqlx::query("SELECT owner_session FROM edit_locks WHERE document_key=?")
                .bind(document_key)
                .fetch_one(&mut *tx)
                .await?;
            let owner: String = row.get("owner_session");
            // This is an expected conflict, so finish the transaction before the
            // caller immediately starts another write on a different connection.
            tx.rollback().await?;
            bail!("settings are currently being edited by {owner}");
        }
        tx.commit().await?;
        Ok(EditLock {
            document_key: document_key.into(),
            owner_session: owner.into(),
            lock_token: token,
            expires_at: expires,
        })
    }

    pub async fn current_lock(&self, document_key: &str) -> Result<Option<EditLock>> {
        let now = Utc::now();
        let row = sqlx::query(
            "SELECT document_key,owner_session,lock_token,expires_at FROM edit_locks WHERE document_key=? AND expires_at>?",
        )
        .bind(document_key)
        .bind(format_time(now))
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(EditLock {
                document_key: row.get("document_key"),
                owner_session: row.get("owner_session"),
                lock_token: row.get("lock_token"),
                expires_at: parse_time(row.get("expires_at"))?,
            })
        })
        .transpose()
    }

    pub async fn renew_lock(&self, document_key: &str, token: &str) -> Result<DateTime<Utc>> {
        let expires = Utc::now() + Duration::minutes(2);
        let result =
            sqlx::query("UPDATE edit_locks SET expires_at=? WHERE document_key=? AND lock_token=?")
                .bind(format_time(expires))
                .bind(document_key)
                .bind(token)
                .execute(&self.pool)
                .await?;
        if result.rows_affected() != 1 {
            bail!("settings edit lock not found");
        }
        Ok(expires)
    }

    pub async fn release_lock(&self, document_key: &str, token: &str, force: bool) -> Result<()> {
        if force {
            let mut tx = self.pool.begin().await?;
            let result = sqlx::query("DELETE FROM edit_locks WHERE document_key=?")
                .bind(document_key)
                .execute(&mut *tx)
                .await?;
            if result.rows_affected() != 0 {
                append_audit_tx(
                    &mut tx,
                    "settings",
                    document_key,
                    "settings.lock_force_released",
                    serde_json::json!({}),
                )
                .await?;
            }
            tx.commit().await?;
        } else {
            sqlx::query("DELETE FROM edit_locks WHERE document_key=? AND lock_token=?")
                .bind(document_key)
                .bind(token)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    pub async fn audit_events(
        &self,
        entity_type: &str,
        entity_id: &str,
        limit: u32,
    ) -> Result<Vec<Value>> {
        let rows = sqlx::query("SELECT id,event_type,metadata_json,occurred_at FROM audit_events WHERE entity_type=? AND entity_id=? ORDER BY id DESC LIMIT ?")
            .bind(entity_type).bind(entity_id).bind(limit.min(1_000)).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok(serde_json::json!({
                    "id": row.get::<i64, _>("id"),
                    "event_type": row.get::<String, _>("event_type"),
                    "metadata": serde_json::from_str::<Value>(row.get("metadata_json"))?,
                    "occurred_at": row.get::<String, _>("occurred_at"),
                }))
            })
            .collect()
    }

    pub async fn audit_events_page(
        &self,
        entity_type: &str,
        entity_id: &str,
        cursor_id: Option<i64>,
        limit: u32,
    ) -> Result<Vec<Value>> {
        let rows = sqlx::query(
            "SELECT id,event_type,metadata_json,occurred_at FROM audit_events \
             WHERE entity_type=? AND entity_id=? AND (? IS NULL OR id<?) \
             ORDER BY id DESC LIMIT ?",
        )
        .bind(entity_type)
        .bind(entity_id)
        .bind(cursor_id)
        .bind(cursor_id)
        .bind(i64::from(limit.clamp(1, 201)))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(serde_json::json!({
                    "id": row.get::<i64, _>("id"),
                    "event_type": row.get::<String, _>("event_type"),
                    "metadata": serde_json::from_str::<Value>(row.get("metadata_json"))?,
                    "occurred_at": parse_time(row.get("occurred_at"))?,
                }))
            })
            .collect()
    }

    pub async fn append_audit(
        &self,
        entity_type: &str,
        entity_id: &str,
        event_type: &str,
        metadata: Value,
    ) -> Result<()> {
        sqlx::query("INSERT INTO audit_events(entity_type,entity_id,event_type,metadata_json,occurred_at) VALUES (?,?,?,?,?)")
            .bind(entity_type).bind(entity_id).bind(event_type).bind(serde_json::to_string(&metadata)?).bind(now_string()).execute(&self.pool).await?;
        Ok(())
    }
}

async fn append_audit_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    entity_type: &str,
    entity_id: &str,
    event_type: &str,
    metadata: Value,
) -> Result<()> {
    sqlx::query("INSERT INTO audit_events(entity_type,entity_id,event_type,metadata_json,occurred_at) VALUES (?,?,?,?,?)")
        .bind(entity_type).bind(entity_id).bind(event_type).bind(serde_json::to_string(&metadata)?).bind(now_string()).execute(&mut **tx).await?;
    Ok(())
}

async fn update_daily_schedule_stats_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
    state: RunState,
    result: &ExecutionResult,
    duration_ms: u64,
) -> Result<()> {
    let business_failure = state == RunState::Failed
        && result.diagnostic.as_ref().is_some_and(|diagnostic| {
            matches!(
                diagnostic.code,
                FailureCode::ProcessExitedNonZero | FailureCode::ExcelMacroReturnedFailure
            )
        });
    let succeeded = i64::from(state == RunState::Succeeded);
    let business_failed = i64::from(business_failure);
    let infrastructure_failed = i64::from(state == RunState::Failed && !business_failure);
    let retries = i64::from(state == RunState::Queued);
    sqlx::query(
        "INSERT INTO daily_schedule_stats(schedule_id,day_utc,succeeded,business_failed,\
         infrastructure_failed,cancelled,retries,duration_count,duration_sum_ms,updated_at) \
         SELECT schedule_id,?,?,?,?,?,?,1,?,? FROM runs WHERE id=? \
         ON CONFLICT(schedule_id,day_utc) DO UPDATE SET \
         succeeded=succeeded+excluded.succeeded,\
         business_failed=business_failed+excluded.business_failed,\
         infrastructure_failed=infrastructure_failed+excluded.infrastructure_failed,\
         retries=retries+excluded.retries,duration_count=duration_count+1,\
         duration_sum_ms=duration_sum_ms+excluded.duration_sum_ms,updated_at=excluded.updated_at",
    )
    .bind(result.finished_at.format("%Y-%m-%d").to_string())
    .bind(succeeded)
    .bind(business_failed)
    .bind(infrastructure_failed)
    .bind(0_i64)
    .bind(retries)
    .bind(i64::try_from(duration_ms).unwrap_or(i64::MAX))
    .bind(format_time(result.finished_at))
    .bind(run_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn update_node_processing_stats_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    result: &ExecutionResult,
) -> Result<()> {
    let outcome = result.outcome.as_str();
    let succeeded = i64::from(outcome == "succeeded");
    let failed = i64::from(!matches!(outcome, "succeeded" | "cancelled"));
    let cancelled = i64::from(outcome == "cancelled");
    sqlx::query(
        "INSERT INTO node_processing_stats(agent_id,processed_tasks,succeeded,failed,cancelled,last_processed_at) \
         VALUES (?,1,?,?,?,?) ON CONFLICT(agent_id) DO UPDATE SET \
         processed_tasks=processed_tasks+1,succeeded=succeeded+excluded.succeeded,\
         failed=failed+excluded.failed,cancelled=cancelled+excluded.cancelled,\
         last_processed_at=CASE WHEN last_processed_at IS NULL OR excluded.last_processed_at>last_processed_at \
         THEN excluded.last_processed_at ELSE last_processed_at END",
    )
    .bind(agent_id)
    .bind(succeeded)
    .bind(failed)
    .bind(cancelled)
    .bind(format_time(result.finished_at))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn increment_cancelled_rollup_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    schedule_id: String,
    occurred_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO daily_schedule_stats(schedule_id,day_utc,cancelled,updated_at) \
         VALUES (?,?,1,?) ON CONFLICT(schedule_id,day_utc) DO UPDATE SET \
         cancelled=cancelled+1,updated_at=excluded.updated_at",
    )
    .bind(schedule_id)
    .bind(occurred_at.format("%Y-%m-%d").to_string())
    .bind(format_time(occurred_at))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn decrement_failed_rollup_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run_id: &str,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT r.schedule_id,a.diagnostic_json,a.finished_at FROM runs r \
         LEFT JOIN attempts a ON a.id=(SELECT latest.id FROM attempts latest \
           WHERE latest.run_id=r.id ORDER BY latest.finished_at DESC,latest.created_at DESC,latest.id DESC LIMIT 1) \
         WHERE r.id=? AND r.state='failed'",
    )
    .bind(run_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let diagnostic: Option<FailureDiagnostic> = row
        .get::<Option<String>, _>("diagnostic_json")
        .map(|json| serde_json::from_str(&json))
        .transpose()?;
    let finished_at = row
        .get::<Option<String>, _>("finished_at")
        .map(parse_time)
        .transpose()?
        .unwrap_or_else(Utc::now);
    let column = if diagnostic.as_ref().is_some_and(|diagnostic| {
        matches!(
            diagnostic.code,
            FailureCode::ProcessExitedNonZero | FailureCode::ExcelMacroReturnedFailure
        )
    }) {
        "business_failed"
    } else {
        "infrastructure_failed"
    };
    let query = format!(
        "UPDATE daily_schedule_stats SET {column}=MAX({column}-1,0),updated_at=? \
         WHERE schedule_id=? AND day_utc=?"
    );
    sqlx::query(&query)
        .bind(now_string())
        .bind(row.get::<String, _>("schedule_id"))
        .bind(finished_at.format("%Y-%m-%d").to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn schedule_view_from_row(row: sqlx::sqlite::SqliteRow) -> Result<ScheduleView> {
    Ok(ScheduleView {
        id: Uuid::parse_str(row.get::<String, _>("id").as_str())?,
        spec: serde_json::from_str(row.get("spec_json"))?,
        revision: row.get("revision"),
        created_at: parse_time(row.get("created_at"))?,
        updated_at: parse_time(row.get("updated_at"))?,
        webhook_public_id: row.get("webhook_public_id"),
    })
}

fn schedule_record_from_row(row: sqlx::sqlite::SqliteRow) -> Result<ScheduleRecord> {
    let view = ScheduleView {
        id: Uuid::parse_str(row.get::<String, _>("id").as_str())?,
        spec: serde_json::from_str(row.get("spec_json"))?,
        revision: row.get("revision"),
        created_at: parse_time(row.get("created_at"))?,
        updated_at: parse_time(row.get("updated_at"))?,
        webhook_public_id: row.get("webhook_public_id"),
    };
    let last: Option<String> = row.get("last_cron_at");
    Ok(ScheduleRecord {
        view,
        encrypted_snapshot: row.get("encrypted_snapshot"),
        snapshot_digest: row.get("snapshot_digest"),
        key_id: row.get("key_id"),
        webhook_secret_hash: row.get("webhook_secret_hash"),
        last_cron_at: last.map(parse_time).transpose()?,
    })
}

fn run_view_from_row(row: sqlx::sqlite::SqliteRow) -> Result<RunView> {
    let state: String = row.get("state");
    Ok(RunView {
        id: Uuid::parse_str(row.get::<String, _>("id").as_str())?,
        schedule_id: Uuid::parse_str(row.get::<String, _>("schedule_id").as_str())?,
        state: parse_state(&state)?,
        trigger_kind: row.get("trigger_kind"),
        scheduled_at: parse_time(row.get("scheduled_at"))?,
        attempt_count: row.get::<i64, _>("attempt_count") as u32,
        created_at: parse_time(row.get("created_at"))?,
        updated_at: parse_time(row.get("updated_at"))?,
    })
}

fn run_record_from_row(row: sqlx::sqlite::SqliteRow) -> Result<RunRecord> {
    let state: String = row.get("state");
    let view = RunView {
        id: Uuid::parse_str(row.get::<String, _>("id").as_str())?,
        schedule_id: Uuid::parse_str(row.get::<String, _>("schedule_id").as_str())?,
        state: parse_state(&state)?,
        trigger_kind: row.get("trigger_kind"),
        scheduled_at: parse_time(row.get("scheduled_at"))?,
        attempt_count: row.get::<i64, _>("attempt_count") as u32,
        created_at: parse_time(row.get("created_at"))?,
        updated_at: parse_time(row.get("updated_at"))?,
    };
    Ok(RunRecord {
        view,
        encrypted_snapshot: row.get("encrypted_snapshot"),
        key_id: row.get("key_id"),
        max_attempts: row.get::<i64, _>("max_attempts") as u32,
        initial_backoff_seconds: row.get::<i64, _>("initial_backoff_seconds") as u64,
        backoff_cap_seconds: row.get::<i64, _>("backoff_cap_seconds") as u64,
        not_before: parse_time(row.get("not_before"))?,
    })
}

fn attempt_from_row(row: sqlx::sqlite::SqliteRow) -> Result<AttemptRecord> {
    Ok(AttemptRecord {
        id: Uuid::parse_str(row.get::<String, _>("id").as_str())?,
        run_id: Uuid::parse_str(row.get::<String, _>("run_id").as_str())?,
        agent_id: row.get("agent_id"),
        attempt_number: row.get::<i64, _>("attempt_number") as u32,
        lease_token: row.get("lease_token"),
        lease_expires_at: parse_time(row.get("lease_expires_at"))?,
    })
}

fn attempt_view_from_row(row: sqlx::sqlite::SqliteRow) -> Result<AttemptView> {
    let diagnostic = row
        .get::<Option<String>, _>("diagnostic_json")
        .map(|json| serde_json::from_str(&json))
        .transpose()?;
    let output = row
        .get::<Option<String>, _>("output_metadata_json")
        .map(|json| serde_json::from_str(&json))
        .transpose()?;
    let accepted_at = row
        .get::<Option<String>, _>("accepted_at")
        .map(parse_time)
        .transpose()?;
    let finished_at = row
        .get::<Option<String>, _>("finished_at")
        .map(parse_time)
        .transpose()?;
    Ok(AttemptView {
        id: Uuid::parse_str(row.get::<String, _>("id").as_str())?,
        run_id: Uuid::parse_str(row.get::<String, _>("run_id").as_str())?,
        agent_id: row.get("agent_id"),
        attempt_number: row.get::<i64, _>("attempt_number") as u32,
        state: row.get("state"),
        outcome: row.get("outcome"),
        exit_code: row.get("exit_code"),
        signal: row.get("signal"),
        duration_ms: row
            .get::<Option<i64>, _>("duration_ms")
            .map(|value| value.max(0) as u64),
        diagnostic,
        output,
        accepted_at,
        finished_at,
        created_at: parse_time(row.get("created_at"))?,
    })
}

fn agent_from_row(row: sqlx::sqlite::SqliteRow) -> Result<AgentView> {
    Ok(AgentView {
        id: row.get("id"),
        hostname: row.get("hostname"),
        labels: serde_json::from_str::<std::collections::HashMap<String, String>>(
            row.get("labels_json"),
        )?
        .into_iter()
        .collect(),
        capacity: row.get::<i64, _>("capacity") as u32,
        running: row.get::<i64, _>("running") as u32,
        connected: row.get::<bool, _>("connected"),
        desired_settings_revision: row.get("desired_settings_revision"),
        applied_settings_revision: row.get("applied_settings_revision"),
        settings_error: row.get("settings_error"),
        last_seen_at: parse_time(row.get("last_seen_at"))?,
    })
}

fn parse_state(value: &str) -> Result<RunState> {
    match value {
        "queued" => Ok(RunState::Queued),
        "running" => Ok(RunState::Running),
        "succeeded" => Ok(RunState::Succeeded),
        "failed" => Ok(RunState::Failed),
        "cancelled" => Ok(RunState::Cancelled),
        _ => bail!("unknown run state {value}"),
    }
}

fn format_time(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn now_string() -> String {
    format_time(Utc::now())
}

fn parse_time(value: String) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&value)?.with_timezone(&Utc))
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.is_unique_violation())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use chrono::TimeZone;
    use scheduler_core::{
        ArtifactRef, ExecutionOutcome, ExecutionPolicy, FailureOrigin, FailureStage, ScheduleSpec,
    };
    use tempfile::TempDir;

    use super::*;

    async fn test_store() -> (TempDir, Store) {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("test.db");
        let url = format!("sqlite://{}", database.display());
        let store = Store::connect(&url, None).await.expect("store");
        (directory, store)
    }

    async fn register_test_agent(store: &Store, id: &str) {
        store
            .upsert_agent(id, "test-host", &HashMap::new(), 2, 0)
            .await
            .expect("register test agent");
    }

    async fn advance_node_settings(store: &Store, id: &str) -> i64 {
        let key = format!("node:{id}");
        let settings = store
            .get_node_settings(id)
            .await
            .expect("read node settings")
            .expect("node settings");
        let lock = store
            .acquire_lock(&key, "settings-test")
            .await
            .expect("settings lock");
        let mut document = serde_json::to_value(&settings).expect("settings JSON");
        document["max_parallel"] = serde_json::json!(settings.max_parallel + 1);
        let revision = store
            .update_settings(
                &key,
                settings.revision,
                &document.to_string(),
                &lock.lock_token,
            )
            .await
            .expect("advance node settings");
        store
            .release_lock(&key, &lock.lock_token, false)
            .await
            .expect("release settings lock");
        revision
    }

    fn spec() -> ScheduleSpec {
        ScheduleSpec {
            name: "test".into(),
            blueprint_ref: ArtifactRef {
                uri: "file:///blueprint".into(),
            },
            parameters_ref: ArtifactRef {
                uri: "file:///parameters".into(),
            },
            parameter_collection: None,
            required_labels: BTreeMap::new(),
            cron: None,
            webhook_enabled: false,
            enabled: true,
        }
    }

    fn completion(outcome: scheduler_core::ExecutionOutcome) -> ExecutionResult {
        let now = Utc::now();
        ExecutionResult {
            outcome,
            exit_code: if outcome == scheduler_core::ExecutionOutcome::Succeeded {
                Some(0)
            } else {
                Some(1)
            },
            signal: None,
            stdout: String::new(),
            stderr: String::new(),
            started_at: now,
            finished_at: now,
            error: None,
            output: OutputMetadata::default(),
            diagnostic: None,
        }
    }

    async fn schedule_and_run(store: &Store) -> (Uuid, Uuid) {
        let schedule_id = Uuid::new_v4();
        store
            .create_schedule(NewSchedule {
                id: schedule_id,
                spec: spec(),
                encrypted_snapshot: vec![1],
                snapshot_digest: "digest".into(),
                key_id: "v1".into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("schedule");
        let run_id = Uuid::new_v4();
        store
            .create_run(NewRun {
                id: run_id,
                schedule_id,
                trigger_kind: "manual".into(),
                scheduled_at: Utc::now(),
                encrypted_snapshot: vec![2],
                key_id: "v1".into(),
                max_attempts: ExecutionPolicy::default().max_attempts,
                initial_backoff_seconds: 1,
                backoff_cap_seconds: 3,
                idempotency_key: None,
            })
            .await
            .expect("run");
        (schedule_id, run_id)
    }

    async fn create_schedule(store: &Store) -> Uuid {
        let schedule_id = Uuid::new_v4();
        store
            .create_schedule(NewSchedule {
                id: schedule_id,
                spec: spec(),
                encrypted_snapshot: vec![1],
                snapshot_digest: "digest".into(),
                key_id: "v1".into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("schedule");
        schedule_id
    }

    async fn create_cron_schedule(store: &Store, expression: &str, timezone: &str) -> Uuid {
        let schedule_id = Uuid::new_v4();
        let mut schedule = spec();
        schedule.cron = Some(CronSpec {
            expression: expression.into(),
            timezone: timezone.into(),
        });
        store
            .create_schedule(NewSchedule {
                id: schedule_id,
                spec: schedule,
                encrypted_snapshot: vec![1],
                snapshot_digest: "digest".into(),
                key_id: "v1".into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("cron schedule");
        schedule_id
    }

    fn cron_run(schedule_id: Uuid, scheduled_at: DateTime<Utc>, snapshot: u8) -> NewRun {
        NewRun {
            id: Uuid::new_v4(),
            schedule_id,
            trigger_kind: "cron".into(),
            scheduled_at,
            encrypted_snapshot: vec![snapshot],
            key_id: "v1".into(),
            max_attempts: 3,
            initial_backoff_seconds: 1,
            backoff_cap_seconds: 3,
            idempotency_key: None,
        }
    }

    #[tokio::test]
    async fn settings_are_locked_and_revision_checked() {
        let (_directory, store) = test_store().await;
        let settings = store.get_global_settings().await.expect("settings");
        let lock = store
            .acquire_lock("global", "session-one")
            .await
            .expect("lock");
        assert!(store.acquire_lock("global", "session-two").await.is_err());
        let current = store
            .current_lock("global")
            .await
            .expect("current lock")
            .expect("active lock");
        assert_eq!(current.owner_session, "session-one");
        assert_eq!(current.lock_token, lock.lock_token);
        let mut document = serde_json::to_value(&settings).expect("json");
        document["default_timeout_seconds"] = 42.into();
        let revision = store
            .update_settings(
                "global",
                settings.revision,
                &document.to_string(),
                &lock.lock_token,
            )
            .await
            .expect("update");
        assert_eq!(revision, settings.revision + 1);
        assert!(
            store
                .update_settings(
                    "global",
                    settings.revision,
                    &document.to_string(),
                    &lock.lock_token
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn force_releasing_a_settings_lock_is_audited() {
        let (_directory, store) = test_store().await;
        store
            .acquire_lock("global", "abandoned-session")
            .await
            .expect("lock");

        store
            .release_lock("global", "", true)
            .await
            .expect("force release");

        assert!(
            store
                .current_lock("global")
                .await
                .expect("current lock")
                .is_none()
        );
        let events = store
            .audit_events("settings", "global", 10)
            .await
            .expect("audit events");
        assert!(
            events
                .iter()
                .any(|event| event["event_type"] == "settings.lock_force_released")
        );
    }

    #[tokio::test]
    async fn rejected_settings_ack_preserves_applied_revision_and_exposes_safe_state() {
        let (_directory, store) = test_store().await;
        register_test_agent(&store, "node-rejected").await;

        store
            .settings_applied("node-rejected", 1, Some("command root is not allowed"))
            .await
            .expect("record rejection");

        let agent = store.list_agents().await.expect("agents").remove(0);
        assert_eq!(agent.desired_settings_revision, 1);
        assert_eq!(agent.applied_settings_revision, 0);
        assert_eq!(
            agent.settings_error.as_deref(),
            Some("command root is not allowed")
        );
        let events = store
            .audit_events("agent", "node-rejected", 10)
            .await
            .expect("audit events");
        let rejected = events
            .iter()
            .find(|event| event["event_type"] == "settings.rejected")
            .expect("rejection audit");
        assert_eq!(rejected["metadata"], serde_json::json!({"revision": 1}));
        assert!(
            !rejected.to_string().contains("command root is not allowed"),
            "audit metadata must not copy the agent-provided error"
        );
    }

    #[tokio::test]
    async fn successful_settings_ack_recovers_from_rejection() {
        let (_directory, store) = test_store().await;
        register_test_agent(&store, "node-recovery").await;
        let key = "node:node-recovery";
        let settings = store
            .get_node_settings("node-recovery")
            .await
            .expect("read settings")
            .expect("node settings");
        let lock = store
            .acquire_lock(key, "recovery-test")
            .await
            .expect("settings lock");
        let mut document = serde_json::to_value(&settings).expect("settings JSON");
        document["max_parallel"] = serde_json::json!(7);
        document["labels"] = serde_json::json!({"pool": "synchronized"});
        let revision = store
            .update_settings(
                key,
                settings.revision,
                &document.to_string(),
                &lock.lock_token,
            )
            .await
            .expect("update settings");
        store
            .settings_applied("node-recovery", revision, Some("temporary rejection"))
            .await
            .expect("record rejection");

        store
            .settings_applied("node-recovery", revision, None)
            .await
            .expect("record success");

        let agent = store.list_agents().await.expect("agents").remove(0);
        assert_eq!(agent.applied_settings_revision, revision);
        assert_eq!(agent.desired_settings_revision, revision);
        assert_eq!(agent.settings_error, None);
        assert_eq!(agent.capacity, 7);
        assert_eq!(agent.labels["pool"], "synchronized");
        let events = store
            .audit_events("agent", "node-recovery", 10)
            .await
            .expect("audit events");
        assert_eq!(events[0]["event_type"], "settings.applied");
        assert_eq!(
            events[0]["metadata"],
            serde_json::json!({"revision": revision})
        );
    }

    #[tokio::test]
    async fn stale_and_out_of_order_settings_acks_cannot_overwrite_current_state() {
        let (_directory, store) = test_store().await;
        register_test_agent(&store, "node-stale").await;
        store
            .settings_applied("node-stale", 1, None)
            .await
            .expect("initial success");
        let revision = advance_node_settings(&store, "node-stale").await;
        assert_eq!(revision, 2);
        store
            .settings_applied("node-stale", revision, Some("current rejection"))
            .await
            .expect("current rejection");

        for (ack_revision, error) in [
            (1, None),
            (1, Some("stale rejection")),
            (3, None),
            (3, Some("future rejection")),
        ] {
            store
                .settings_applied("node-stale", ack_revision, error)
                .await
                .expect("ignored acknowledgement");
        }
        let rejected = store.list_agents().await.expect("agents").remove(0);
        assert_eq!(rejected.desired_settings_revision, 2);
        assert_eq!(rejected.applied_settings_revision, 1);
        assert_eq!(
            rejected.settings_error.as_deref(),
            Some("current rejection")
        );

        store
            .settings_applied("node-stale", revision, None)
            .await
            .expect("current success");
        store
            .settings_applied("node-stale", revision, Some("late rejection"))
            .await
            .expect("late rejection is ignored");
        let applied = store.list_agents().await.expect("agents").remove(0);
        assert_eq!(applied.applied_settings_revision, 2);
        assert_eq!(applied.settings_error, None);

        let events = store
            .audit_events("agent", "node-stale", 20)
            .await
            .expect("audit events");
        assert_eq!(
            events
                .iter()
                .filter(|event| event["event_type"] == "settings.ack_ignored")
                .count(),
            5
        );
        assert!(
            events
                .iter()
                .all(|event| !event.to_string().contains("stale rejection")
                    && !event.to_string().contains("future rejection")
                    && !event.to_string().contains("late rejection")),
            "ignored acknowledgements must not audit agent-provided error text"
        );
    }

    #[tokio::test]
    async fn delayed_success_cannot_apply_a_newer_rejected_desired_document() {
        let (_directory, store) = test_store().await;
        register_test_agent(&store, "node-delayed-success").await;
        let initial = store
            .settings_applied("node-delayed-success", 1, None)
            .await
            .expect("initial success");
        assert_eq!(initial.status, SettingsAckStatus::Applied);

        let revision_two = advance_node_settings(&store, "node-delayed-success").await;
        let revision_three = advance_node_settings(&store, "node-delayed-success").await;
        assert_eq!((revision_two, revision_three), (2, 3));

        let rejected = store
            .settings_applied(
                "node-delayed-success",
                revision_three,
                Some("revision three is invalid on this node"),
            )
            .await
            .expect("current rejection");
        assert_eq!(rejected.status, SettingsAckStatus::Rejected);
        assert_eq!(rejected.desired_revision, 3);
        assert_eq!(rejected.applied_revision, 1);
        assert!(rejected.applied_settings.is_none());

        let delayed = store
            .settings_applied("node-delayed-success", revision_two, None)
            .await
            .expect("delayed success is safely ignored");
        assert_eq!(delayed.status, SettingsAckStatus::Ignored);
        assert_eq!(delayed.desired_revision, 3);
        assert_eq!(delayed.applied_revision, 1);
        assert!(delayed.applied_settings.is_none());

        let agent = store.list_agents().await.expect("agents").remove(0);
        assert_eq!(agent.desired_settings_revision, 3);
        assert_eq!(agent.applied_settings_revision, 1);
        assert_eq!(
            agent.settings_error.as_deref(),
            Some("revision three is invalid on this node")
        );
    }

    #[tokio::test]
    async fn failed_offer_does_not_reuse_attempt_number() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;
        let first = store
            .create_attempt(run_id, "node-a", 60)
            .await
            .expect("attempt")
            .expect("first");
        store.release_offer(first.id).await.expect("release");
        let second = store
            .create_attempt(run_id, "node-a", 60)
            .await
            .expect("attempt")
            .expect("second");
        assert_eq!(first.attempt_number, 1);
        assert_eq!(second.attempt_number, 2);
        let run = store.get_run(run_id).await.expect("run").expect("present");
        assert_eq!(
            run.attempt_count, 0,
            "unaccepted offers must not consume retries"
        );
    }

    #[tokio::test]
    async fn duplicate_result_is_idempotent() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;
        let attempt = store
            .create_attempt(run_id, "node-a", 60)
            .await
            .expect("attempt")
            .expect("present");
        store
            .accept_attempt(attempt.id, &attempt.lease_token, 60)
            .await
            .expect("accept");
        let success = completion(scheduler_core::ExecutionOutcome::Succeeded);
        let first = store
            .finish_attempt(attempt.id, &attempt.lease_token, &success, vec![4], "v1")
            .await
            .expect("finish");
        let second = store
            .finish_attempt(attempt.id, &attempt.lease_token, &success, vec![4], "v1")
            .await
            .expect("duplicate");
        assert_eq!(first, RunState::Succeeded);
        assert_eq!(second, RunState::Succeeded);

        let rollup = sqlx::query(
            "SELECT succeeded,business_failed,infrastructure_failed,cancelled,retries,\
             duration_count FROM daily_schedule_stats",
        )
        .fetch_one(store.pool())
        .await
        .expect("daily rollup");
        assert_eq!(rollup.get::<i64, _>("succeeded"), 1);
        assert_eq!(rollup.get::<i64, _>("business_failed"), 0);
        assert_eq!(rollup.get::<i64, _>("infrastructure_failed"), 0);
        assert_eq!(rollup.get::<i64, _>("cancelled"), 0);
        assert_eq!(rollup.get::<i64, _>("retries"), 0);
        assert_eq!(rollup.get::<i64, _>("duration_count"), 1);

        let node_rollup = sqlx::query(
            "SELECT processed_tasks,succeeded,failed,cancelled FROM node_processing_stats \
             WHERE agent_id='node-a'",
        )
        .fetch_one(store.pool())
        .await
        .expect("node processing rollup");
        assert_eq!(node_rollup.get::<i64, _>("processed_tasks"), 1);
        assert_eq!(node_rollup.get::<i64, _>("succeeded"), 1);
        assert_eq!(node_rollup.get::<i64, _>("failed"), 0);
        assert_eq!(node_rollup.get::<i64, _>("cancelled"), 0);
    }

    #[tokio::test]
    async fn node_processing_rollup_counts_each_completed_retry_attempt() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;

        let failed_attempt = store
            .create_attempt(run_id, "node-a", 60)
            .await
            .expect("failed attempt")
            .expect("present");
        store
            .accept_attempt(failed_attempt.id, &failed_attempt.lease_token, 60)
            .await
            .expect("accept failed attempt");
        store
            .finish_attempt(
                failed_attempt.id,
                &failed_attempt.lease_token,
                &completion(scheduler_core::ExecutionOutcome::Failed),
                vec![4],
                "v1",
            )
            .await
            .expect("finish failed attempt");

        let successful_attempt = store
            .create_attempt(run_id, "node-a", 60)
            .await
            .expect("successful attempt")
            .expect("present");
        store
            .accept_attempt(successful_attempt.id, &successful_attempt.lease_token, 60)
            .await
            .expect("accept successful attempt");
        store
            .finish_attempt(
                successful_attempt.id,
                &successful_attempt.lease_token,
                &completion(scheduler_core::ExecutionOutcome::Succeeded),
                vec![4],
                "v1",
            )
            .await
            .expect("finish successful attempt");

        let rollup = sqlx::query(
            "SELECT processed_tasks,succeeded,failed,cancelled FROM node_processing_stats \
             WHERE agent_id='node-a'",
        )
        .fetch_one(store.pool())
        .await
        .expect("node processing rollup");
        assert_eq!(rollup.get::<i64, _>("processed_tasks"), 2);
        assert_eq!(rollup.get::<i64, _>("succeeded"), 1);
        assert_eq!(rollup.get::<i64, _>("failed"), 1);
        assert_eq!(rollup.get::<i64, _>("cancelled"), 0);
    }

    #[tokio::test]
    async fn cancellation_is_counted_once_in_daily_rollups() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;

        store.cancel_run(run_id).await.expect("cancel run");
        assert!(store.cancel_run(run_id).await.is_err());

        let cancelled: i64 = sqlx::query_scalar("SELECT cancelled FROM daily_schedule_stats")
            .fetch_one(store.pool())
            .await
            .expect("cancelled rollup");
        assert_eq!(cancelled, 1);
    }

    #[tokio::test]
    async fn attempt_diagnostics_are_queryable_without_decrypting_task_output() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;
        let attempt = store
            .create_attempt(run_id, "node-a", 60)
            .await
            .expect("attempt")
            .expect("present");
        store
            .accept_attempt(attempt.id, &attempt.lease_token, 60)
            .await
            .expect("accept");
        let mut result = completion(scheduler_core::ExecutionOutcome::InfrastructureError);
        result.exit_code = None;
        result.signal = Some("11".into());
        result.output = OutputMetadata {
            stdout_bytes: 12,
            stderr_bytes: 34,
            stdout_truncated: false,
            stderr_truncated: true,
        };
        result.diagnostic = Some(FailureDiagnostic::new(
            scheduler_core::FailureCode::ProcessCrashed,
            scheduler_core::FailureOrigin::CommandProcess,
            scheduler_core::FailureStage::Execution,
            "command process crashed or was terminated by the operating system",
            true,
        ));
        store
            .finish_attempt(attempt.id, &attempt.lease_token, &result, vec![9], "v1")
            .await
            .expect("finish");

        let attempts = store.run_attempts(run_id).await.expect("attempt views");
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].signal.as_deref(), Some("11"));
        assert_eq!(
            attempts[0].diagnostic.as_ref().expect("diagnostic").code,
            scheduler_core::FailureCode::ProcessCrashed
        );
        assert!(
            attempts[0]
                .output
                .as_ref()
                .expect("output")
                .stderr_truncated
        );
        let events = store
            .audit_events("run", &run_id.to_string(), 20)
            .await
            .expect("events");
        let finished = events
            .iter()
            .find(|event| event["event_type"] == "run.retry_scheduled")
            .expect("retry event");
        assert_eq!(
            finished["metadata"]["diagnostic"]["code"],
            "process_crashed"
        );
    }

    #[tokio::test]
    async fn pause_and_resume_skip_the_paused_cron_window_and_are_idempotent() {
        let (_directory, store) = test_store().await;
        let schedule_id = create_cron_schedule(&store, "0 * * * * *", "UTC").await;
        let previous_cursor = Utc.with_ymd_and_hms(2030, 1, 1, 7, 0, 0).unwrap();
        let paused_at = Utc.with_ymd_and_hms(2030, 1, 1, 7, 30, 0).unwrap();
        let resumed_at = Utc.with_ymd_and_hms(2030, 1, 1, 8, 0, 30).unwrap();
        store
            .advance_cron_cursor(schedule_id, previous_cursor)
            .await
            .expect("seed cron cursor");

        store
            .set_schedule_enabled_at(schedule_id, false, paused_at)
            .await
            .expect("pause");
        let paused = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert!(!paused.view.spec.enabled);
        assert_eq!(paused.last_cron_at, Some(previous_cursor));
        let paused_revision = paused.view.revision;

        store
            .set_schedule_enabled_at(
                schedule_id,
                false,
                Utc.with_ymd_and_hms(2030, 1, 1, 7, 45, 0).unwrap(),
            )
            .await
            .expect("repeat pause");
        let unchanged_pause = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert_eq!(unchanged_pause.view.revision, paused_revision);
        assert_eq!(unchanged_pause.last_cron_at, Some(previous_cursor));

        store
            .set_schedule_enabled_at(schedule_id, true, resumed_at)
            .await
            .expect("resume");
        let resumed = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert!(resumed.view.spec.enabled);
        assert_eq!(resumed.last_cron_at, Some(resumed_at));
        let cron = resumed.view.spec.cron.as_ref().expect("cron");
        let next = scheduler_core::schedule::next_occurrences(cron, resumed_at, 1)
            .expect("next occurrence");
        assert_eq!(
            next,
            vec![Utc.with_ymd_and_hms(2030, 1, 1, 8, 1, 0).unwrap()]
        );

        let resumed_revision = resumed.view.revision;
        store
            .set_schedule_enabled_at(
                schedule_id,
                true,
                Utc.with_ymd_and_hms(2030, 1, 1, 9, 0, 0).unwrap(),
            )
            .await
            .expect("repeat resume");
        let unchanged = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert_eq!(unchanged.view.revision, resumed_revision);
        assert_eq!(unchanged.last_cron_at, Some(resumed_at));

        let resume_events = store
            .audit_events("schedule", &schedule_id.to_string(), 20)
            .await
            .expect("audit events")
            .iter()
            .filter(|event| event["event_type"] == "schedule.resumed")
            .count();
        let pause_events = store
            .audit_events("schedule", &schedule_id.to_string(), 20)
            .await
            .expect("audit events")
            .iter()
            .filter(|event| event["event_type"] == "schedule.paused")
            .count();
        assert_eq!(resume_events, 1);
        assert_eq!(pause_events, 1);
    }

    #[tokio::test]
    async fn resume_never_moves_a_future_cron_cursor_backwards() {
        let (_directory, store) = test_store().await;
        let schedule_id = create_cron_schedule(&store, "0 * * * * *", "UTC").await;
        let future_cursor = Utc.with_ymd_and_hms(2035, 1, 1, 0, 0, 0).unwrap();
        store
            .advance_cron_cursor(schedule_id, future_cursor)
            .await
            .expect("seed future cursor");
        store
            .set_schedule_enabled_at(
                schedule_id,
                false,
                Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap(),
            )
            .await
            .expect("pause");
        store
            .set_schedule_enabled_at(
                schedule_id,
                true,
                Utc.with_ymd_and_hms(2030, 2, 1, 0, 0, 0).unwrap(),
            )
            .await
            .expect("resume");

        let resumed = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert_eq!(resumed.last_cron_at, Some(future_cursor));
    }

    #[tokio::test]
    async fn schedule_update_resume_also_skips_the_paused_cron_window() {
        let (_directory, store) = test_store().await;
        let schedule_id = create_cron_schedule(&store, "0 * * * * *", "UTC").await;
        store
            .set_schedule_enabled(schedule_id, false)
            .await
            .expect("pause");
        let paused = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        let before_resume = Utc::now() - Duration::milliseconds(2);
        let mut resumed_spec = paused.view.spec.clone();
        resumed_spec.enabled = true;
        store
            .update_schedule(
                schedule_id,
                paused.view.revision,
                resumed_spec,
                vec![2],
                "resumed snapshot".into(),
                "v2".into(),
            )
            .await
            .expect("resume through update");
        let after_resume = Utc::now() + Duration::milliseconds(2);

        let resumed = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        let cursor = resumed.last_cron_at.expect("resume cursor");
        assert!(cursor >= before_resume && cursor <= after_resume);
    }

    #[tokio::test]
    async fn webhook_rotation_updates_the_schedule_document() {
        let (_directory, store) = test_store().await;
        let schedule_id = create_schedule(&store).await;

        store
            .rotate_webhook(schedule_id, "public".into(), "hash".into())
            .await
            .expect("rotate");
        let schedule = store
            .get_schedule(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert!(schedule.spec.webhook_enabled);
        assert_eq!(schedule.webhook_public_id.as_deref(), Some("public"));
    }

    #[tokio::test]
    async fn cron_identity_edits_reset_cursor_but_other_edits_preserve_it() {
        let (_directory, store) = test_store().await;
        let schedule_id = create_schedule(&store).await;

        let before_add = Utc::now() - Duration::milliseconds(1);
        let initial = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        let mut added_spec = initial.view.spec.clone();
        added_spec.cron = Some(CronSpec {
            expression: "0 * * * * *".into(),
            timezone: "UTC".into(),
        });
        store
            .update_schedule(
                schedule_id,
                initial.view.revision,
                added_spec,
                vec![2],
                "added".into(),
                "v1".into(),
            )
            .await
            .expect("add cron");
        let added = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        let add_cursor = added.last_cron_at.expect("add resets cursor");
        assert!(add_cursor >= before_add && add_cursor <= Utc::now());

        let sentinel =
            DateTime::from_timestamp_millis((Utc::now() + Duration::days(30)).timestamp_millis())
                .expect("valid sentinel");
        store
            .advance_cron_cursor(schedule_id, sentinel)
            .await
            .expect("set sentinel");
        let mut snapshot_only = added.view.spec.clone();
        snapshot_only.name = "same cron, new snapshot".into();
        store
            .update_schedule(
                schedule_id,
                added.view.revision,
                snapshot_only,
                vec![3],
                "snapshot-only".into(),
                "v2".into(),
            )
            .await
            .expect("non-cron edit");
        let unchanged = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert_eq!(unchanged.last_cron_at, Some(sentinel));

        let before_expression = Utc::now() - Duration::milliseconds(1);
        let mut changed_expression = unchanged.view.spec.clone();
        changed_expression.cron.as_mut().expect("cron").expression = "30 * * * * *".into();
        store
            .update_schedule(
                schedule_id,
                unchanged.view.revision,
                changed_expression,
                vec![4],
                "expression".into(),
                "v2".into(),
            )
            .await
            .expect("change expression");
        let expression_changed = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        let expression_cursor = expression_changed.last_cron_at.expect("cursor");
        assert!(expression_cursor >= before_expression && expression_cursor < sentinel);

        let before_timezone = Utc::now() - Duration::milliseconds(1);
        let mut changed_timezone = expression_changed.view.spec.clone();
        changed_timezone.cron.as_mut().expect("cron").timezone = "Europe/Vienna".into();
        store
            .update_schedule(
                schedule_id,
                expression_changed.view.revision,
                changed_timezone,
                vec![5],
                "timezone".into(),
                "v2".into(),
            )
            .await
            .expect("change timezone");
        let timezone_changed = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert!(timezone_changed.last_cron_at.expect("cursor") >= before_timezone);

        let before_remove = Utc::now() - Duration::milliseconds(1);
        let mut removed_spec = timezone_changed.view.spec.clone();
        removed_spec.cron = None;
        store
            .update_schedule(
                schedule_id,
                timezone_changed.view.revision,
                removed_spec,
                vec![6],
                "removed".into(),
                "v2".into(),
            )
            .await
            .expect("remove cron");
        let removed = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert!(removed.last_cron_at.expect("remove resets cursor") >= before_remove);

        let before_readd = Utc::now() - Duration::milliseconds(1);
        let mut readded_spec = removed.view.spec.clone();
        readded_spec.cron = Some(CronSpec {
            expression: "0 */5 * * * *".into(),
            timezone: "UTC".into(),
        });
        store
            .update_schedule(
                schedule_id,
                removed.view.revision,
                readded_spec,
                vec![7],
                "readded".into(),
                "v3".into(),
            )
            .await
            .expect("re-add cron");
        let readded = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert!(readded.last_cron_at.expect("re-add resets cursor") >= before_readd);
    }

    #[tokio::test]
    async fn cron_occurrence_transaction_rejects_stale_revision_and_preserves_new_cursor() {
        let (_directory, store) = test_store().await;
        let schedule_id = create_cron_schedule(&store, "0 * * * * *", "UTC").await;
        let detached = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        let cron = detached.view.spec.cron.clone().expect("cron");

        let mut edited_spec = detached.view.spec.clone();
        edited_spec.name = "new revision".into();
        store
            .update_schedule(
                schedule_id,
                detached.view.revision,
                edited_spec,
                vec![9],
                "new snapshot".into(),
                "v2".into(),
            )
            .await
            .expect("edit");
        let current = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        let occurrence = Utc::now() + Duration::hours(1);
        let result = store
            .create_cron_occurrence(
                cron_run(schedule_id, occurrence, 0xaa),
                detached.view.revision,
                &cron,
            )
            .await
            .expect("fenced create");
        assert!(matches!(result, CronOccurrenceResult::StaleSchedule));
        assert_eq!(
            store
                .get_schedule_record(schedule_id)
                .await
                .expect("read")
                .expect("schedule")
                .last_cron_at,
            current.last_cron_at
        );
        let run_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE schedule_id=?")
            .bind(schedule_id.to_string())
            .fetch_one(store.pool())
            .await
            .expect("count");
        assert_eq!(run_count, 0);
    }

    #[tokio::test]
    async fn cron_occurrence_transaction_fences_expression_and_timezone_identity() {
        for (column, replacement) in [
            ("cron_expression", "30 * * * * *"),
            ("cron_timezone", "Europe/Vienna"),
        ] {
            let (_directory, store) = test_store().await;
            let schedule_id = create_cron_schedule(&store, "0 * * * * *", "UTC").await;
            let detached = store
                .get_schedule_record(schedule_id)
                .await
                .expect("read")
                .expect("schedule");
            let cron = detached.view.spec.cron.clone().expect("cron");
            let statement = format!("UPDATE schedules SET {column}=? WHERE id=?");
            sqlx::query(&statement)
                .bind(replacement)
                .bind(schedule_id.to_string())
                .execute(store.pool())
                .await
                .expect("mutate identity without revision");

            let result = store
                .create_cron_occurrence(
                    cron_run(schedule_id, Utc::now() + Duration::hours(1), 0xbb),
                    detached.view.revision,
                    &cron,
                )
                .await
                .expect("fenced create");
            assert!(
                matches!(result, CronOccurrenceResult::StaleSchedule),
                "identity column {column} was not fenced"
            );
            let run_count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE schedule_id=?")
                    .bind(schedule_id.to_string())
                    .fetch_one(store.pool())
                    .await
                    .expect("count");
            assert_eq!(run_count, 0, "identity column {column}");
        }
    }

    #[tokio::test]
    async fn cron_occurrence_transaction_returns_unique_run_without_replacing_snapshot() {
        let (_directory, store) = test_store().await;
        let schedule_id = create_cron_schedule(&store, "0 * * * * *", "UTC").await;
        let schedule = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        let cron = schedule.view.spec.cron.clone().expect("cron");
        let occurrence = Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap();

        let first = store
            .create_cron_occurrence(
                cron_run(schedule_id, occurrence, 0x11),
                schedule.view.revision,
                &cron,
            )
            .await
            .expect("first");
        let duplicate = store
            .create_cron_occurrence(
                cron_run(schedule_id, occurrence, 0x22),
                schedule.view.revision,
                &cron,
            )
            .await
            .expect("duplicate");
        let CronOccurrenceResult::Applied(first) = first else {
            panic!("current schedule unexpectedly stale");
        };
        let CronOccurrenceResult::Applied(duplicate) = duplicate else {
            panic!("current schedule unexpectedly stale");
        };
        assert_eq!(first.id, duplicate.id);
        let encrypted: Vec<u8> =
            sqlx::query_scalar("SELECT encrypted_snapshot FROM runs WHERE id=?")
                .bind(first.id.to_string())
                .fetch_one(store.pool())
                .await
                .expect("snapshot");
        assert_eq!(encrypted, vec![0x11]);
        assert_eq!(
            store
                .get_schedule_record(schedule_id)
                .await
                .expect("read")
                .expect("schedule")
                .last_cron_at,
            Some(occurrence)
        );
    }

    #[tokio::test]
    async fn concurrent_idempotent_run_creation_returns_the_existing_run() {
        let (_directory, store) = test_store().await;
        let schedule_id = create_schedule(&store).await;
        let create = |scheduled_at| NewRun {
            id: Uuid::new_v4(),
            schedule_id,
            trigger_kind: "manual".into(),
            scheduled_at,
            encrypted_snapshot: vec![2],
            key_id: "v1".into(),
            max_attempts: 3,
            initial_backoff_seconds: 1,
            backoff_cap_seconds: 3,
            idempotency_key: Some("same-key".into()),
        };
        let now = Utc::now();

        let (first, second) = tokio::join!(
            store.create_run(create(now)),
            store.create_run(create(now + Duration::seconds(1)))
        );
        let first = first.expect("first request");
        let second = second.expect("racing request");
        assert_eq!(first.id, second.id);
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM runs WHERE schedule_id=? AND idempotency_key='same-key'",
        )
        .bind(schedule_id.to_string())
        .fetch_one(store.pool())
        .await
        .expect("count");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn an_unacknowledged_offer_expires_without_consuming_a_retry() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;
        let offer = store
            .create_attempt(run_id, "node-a", 0)
            .await
            .expect("offer")
            .expect("attempt");
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;

        let expired = store
            .claim_expired_attempts(100, 0)
            .await
            .expect("expired offers");
        assert!(expired.iter().any(|attempt| attempt.id == offer.id));
        let state = store
            .finish_expired_attempt(
                offer.id,
                &offer.lease_token,
                &completion(scheduler_core::ExecutionOutcome::LeaseExpired),
                vec![3],
                "v1",
            )
            .await
            .expect("expire")
            .expect("still expired");
        assert_eq!(state, RunState::Queued);
        let run = store.get_run(run_id).await.expect("run").expect("present");
        assert_eq!(run.attempt_count, 0);
    }

    #[tokio::test]
    async fn a_renewal_before_the_atomic_expiry_claim_wins_the_race() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;
        let attempt = store
            .create_attempt(run_id, "node-a", 0)
            .await
            .expect("offer")
            .expect("attempt");
        store
            .accept_attempt(attempt.id, &attempt.lease_token, 0)
            .await
            .expect("accept");
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;

        store
            .renew_attempts("node-a", &[attempt.id.to_string()], 60, 1)
            .await
            .expect("heartbeat renewal");

        let claimed = store
            .claim_expired_attempts(100, 0)
            .await
            .expect("atomic expiry claim");
        assert!(
            !claimed.iter().any(|found| found.id == attempt.id),
            "the expiry claim must observe the renewed deadline"
        );
        let run = store.get_run(run_id).await.expect("run").expect("present");
        assert_eq!(run.state, RunState::Running);
        assert_eq!(run.attempt_count, 1);
    }

    #[tokio::test]
    async fn manual_retry_starts_a_fresh_attempt_budget_without_losing_history() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;

        for expected_number in 1..=3 {
            let attempt = store
                .create_attempt(run_id, "node-a", 60)
                .await
                .expect("offer")
                .expect("attempt");
            assert_eq!(attempt.attempt_number, expected_number);
            store
                .accept_attempt(attempt.id, &attempt.lease_token, 60)
                .await
                .expect("accept");
            store
                .finish_attempt(
                    attempt.id,
                    &attempt.lease_token,
                    &completion(scheduler_core::ExecutionOutcome::Failed),
                    vec![4],
                    "v1",
                )
                .await
                .expect("finish");
        }
        assert_eq!(
            store
                .get_run(run_id)
                .await
                .expect("run")
                .expect("present")
                .state,
            RunState::Failed
        );
        let before_retry =
            sqlx::query("SELECT infrastructure_failed,retries FROM daily_schedule_stats")
                .fetch_one(store.pool())
                .await
                .expect("rollup before manual retry");
        assert_eq!(before_retry.get::<i64, _>("infrastructure_failed"), 1);
        assert_eq!(before_retry.get::<i64, _>("retries"), 2);

        store.retry_run(run_id).await.expect("manual retry");
        let after_reopen: i64 =
            sqlx::query_scalar("SELECT infrastructure_failed FROM daily_schedule_stats")
                .fetch_one(store.pool())
                .await
                .expect("rollup after reopening failed run");
        assert_eq!(after_reopen, 0);
        let next = store
            .create_attempt(run_id, "node-a", 60)
            .await
            .expect("offer")
            .expect("attempt");
        assert_eq!(next.attempt_number, 4, "history remains monotonic");
        store
            .accept_attempt(next.id, &next.lease_token, 60)
            .await
            .expect("accept");
        assert_eq!(
            store
                .get_run(run_id)
                .await
                .expect("run")
                .expect("present")
                .attempt_count,
            1
        );
        let state = store
            .finish_attempt(
                next.id,
                &next.lease_token,
                &completion(scheduler_core::ExecutionOutcome::Failed),
                vec![4],
                "v1",
            )
            .await
            .expect("finish");
        assert_eq!(state, RunState::Queued);
        let retries: i64 = sqlx::query_scalar("SELECT retries FROM daily_schedule_stats")
            .fetch_one(store.pool())
            .await
            .expect("retry rollup");
        assert_eq!(retries, 3);
    }

    #[tokio::test]
    async fn recovery_reauthorization_rejects_wrong_expired_cancelled_and_finished_leases() {
        let (_directory, store) = test_store().await;

        let (_, live_run) = schedule_and_run(&store).await;
        let live = store
            .create_attempt(live_run, "node-a", 60)
            .await
            .expect("offer")
            .expect("attempt");
        store
            .accept_attempt(live.id, &live.lease_token, 60)
            .await
            .expect("accept");
        assert!(
            store
                .reauthorize_attempt(live.id, "node-a", &live.lease_token, 60)
                .await
                .expect("valid resume")
        );
        assert!(
            !store
                .reauthorize_attempt(live.id, "node-a", "replacement-token", 60)
                .await
                .expect("wrong token")
        );

        let (_, expired_run) = schedule_and_run(&store).await;
        let expired = store
            .create_attempt(expired_run, "node-a", 0)
            .await
            .expect("offer")
            .expect("attempt");
        store
            .accept_attempt(expired.id, &expired.lease_token, 0)
            .await
            .expect("accept");
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        assert!(
            !store
                .reauthorize_attempt(expired.id, "node-a", &expired.lease_token, 60)
                .await
                .expect("expired")
        );

        let (_, cancelled_run) = schedule_and_run(&store).await;
        let cancelled = store
            .create_attempt(cancelled_run, "node-a", 60)
            .await
            .expect("offer")
            .expect("attempt");
        store
            .accept_attempt(cancelled.id, &cancelled.lease_token, 60)
            .await
            .expect("accept");
        store.cancel_run(cancelled_run).await.expect("cancel run");
        assert!(
            !store
                .reauthorize_attempt(cancelled.id, "node-a", &cancelled.lease_token, 60)
                .await
                .expect("cancelled")
        );

        let (_, finished_run) = schedule_and_run(&store).await;
        let finished = store
            .create_attempt(finished_run, "node-a", 60)
            .await
            .expect("offer")
            .expect("attempt");
        store
            .accept_attempt(finished.id, &finished.lease_token, 60)
            .await
            .expect("accept");
        store
            .finish_attempt(
                finished.id,
                &finished.lease_token,
                &completion(scheduler_core::ExecutionOutcome::Succeeded),
                vec![1],
                "v1",
            )
            .await
            .expect("finish");
        assert!(
            !store
                .reauthorize_attempt(finished.id, "node-a", &finished.lease_token, 60)
                .await
                .expect("finished")
        );
    }

    #[tokio::test]
    async fn queue_depth_is_exact_and_separates_ready_from_delayed_runs() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;
        assert_eq!(
            store.queue_depth().await.expect("ready queue depth"),
            QueueDepth {
                ready: 1,
                delayed: 0,
            }
        );

        sqlx::query("UPDATE runs SET not_before=? WHERE id=?")
            .bind(format_time(Utc::now() + Duration::hours(1)))
            .bind(run_id.to_string())
            .execute(store.pool())
            .await
            .expect("delay run");
        assert_eq!(
            store.queue_depth().await.expect("delayed queue depth"),
            QueueDepth {
                ready: 0,
                delayed: 1,
            }
        );
    }

    #[tokio::test]
    async fn unclassified_failure_without_a_diagnostic_is_not_redispatched() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;
        let attempt = store
            .create_attempt(run_id, "node-a", 60)
            .await
            .expect("offer")
            .expect("attempt");
        store
            .accept_attempt(attempt.id, &attempt.lease_token, 60)
            .await
            .expect("accept");
        store
            .finish_attempt(
                attempt.id,
                &attempt.lease_token,
                &completion(ExecutionOutcome::Failed),
                vec![9],
                "v1",
            )
            .await
            .expect("finish");
        sqlx::query("UPDATE runs SET not_before=? WHERE id=?")
            .bind(now_string())
            .bind(run_id.to_string())
            .execute(store.pool())
            .await
            .expect("remove retry backoff");

        assert!(store.queued_runs(10).await.expect("queued runs").is_empty());
        assert_eq!(
            store.queue_depth().await.expect("queue depth"),
            QueueDepth {
                ready: 0,
                delayed: 1,
            }
        );
    }

    #[tokio::test]
    async fn preacceptance_rejection_is_diagnostic_and_does_not_consume_retry_budget() {
        let (_directory, store) = test_store().await;
        let (_, run_id) = schedule_and_run(&store).await;
        let attempt = store
            .create_attempt(run_id, "node-a", 60)
            .await
            .expect("offer")
            .expect("attempt");
        let now = Utc::now();
        let result = ExecutionResult {
            outcome: ExecutionOutcome::InfrastructureError,
            exit_code: None,
            signal: None,
            stdout: String::new(),
            stderr: String::new(),
            started_at: now,
            finished_at: now,
            error: Some("required binding could not be resolved".into()),
            output: OutputMetadata::default(),
            diagnostic: Some(FailureDiagnostic::new(
                FailureCode::ParameterBindingFailed,
                FailureOrigin::Agent,
                FailureStage::ParameterBinding,
                "required binding could not be resolved",
                true,
            )),
        };
        store
            .reject_offer(attempt.id, &attempt.lease_token, &result, vec![7], "v1")
            .await
            .expect("reject offer");

        let run = store.get_run(run_id).await.expect("run").expect("present");
        assert_eq!(run.state, RunState::Queued);
        assert_eq!(run.attempt_count, 0);
        let attempt = store
            .run_attempts(run_id)
            .await
            .expect("attempt history")
            .remove(0);
        assert_eq!(attempt.state, "finished");
        assert_eq!(
            attempt.diagnostic.expect("diagnostic").code,
            FailureCode::ParameterBindingFailed
        );
        assert!(store.queued_runs(10).await.expect("queued runs").is_empty());
    }
}
