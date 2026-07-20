use std::{path::Path, str::FromStr, sync::Arc};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use rand::Rng;
use scheduler_core::{
    AgentView, ExecutionResult, FailureDiagnostic, GlobalSettings, NodeSettings, OutputMetadata,
    RunState, RunView, ScheduleSpec, ScheduleView,
};
use serde_json::Value;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use uuid::Uuid;

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
    _lock: Option<Arc<std::fs::File>>,
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
        let result = sqlx::query(
            "UPDATE schedules SET name=?,spec_json=?,encrypted_snapshot=?,snapshot_digest=?,key_id=?,revision=revision+1,enabled=?,cron_expression=?,cron_timezone=?,webhook_enabled=?,updated_at=? WHERE id=? AND revision=?",
        )
        .bind(&spec.name)
        .bind(serde_json::to_string(&spec)?)
        .bind(encrypted_snapshot)
        .bind(digest)
        .bind(key_id)
        .bind(spec.enabled)
        .bind(spec.cron.as_ref().map(|cron| &cron.expression))
        .bind(spec.cron.as_ref().map(|cron| &cron.timezone))
        .bind(spec.webhook_enabled)
        .bind(&now)
        .bind(id.to_string())
        .bind(expected_revision)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            bail!("schedule revision conflict or schedule not found");
        }
        self.append_audit(
            "schedule",
            &id.to_string(),
            "schedule.updated",
            serde_json::json!({"revision": expected_revision + 1}),
        )
        .await?;
        self.get_schedule(id)
            .await?
            .context("updated schedule missing")
    }

    pub async fn set_schedule_enabled(&self, id: Uuid, enabled: bool) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT spec_json FROM schedules WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&mut *tx)
            .await?
            .context("schedule not found")?;
        let mut spec: ScheduleSpec = serde_json::from_str(row.get("spec_json"))?;
        spec.enabled = enabled;
        sqlx::query("UPDATE schedules SET enabled=?,spec_json=?,revision=revision+1,updated_at=? WHERE id=?")
            .bind(enabled)
            .bind(serde_json::to_string(&spec)?)
            .bind(now_string())
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
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

    pub async fn create_run(&self, new: NewRun) -> Result<RunView> {
        let now = now_string();
        let id = new.id.to_string();
        let idempotency_key = new.idempotency_key.clone();
        let mut tx = self.pool.begin().await?;
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
            Err(error) => return Err(error.into()),
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

    pub async fn get_run_record(&self, id: Uuid) -> Result<Option<RunRecord>> {
        let row = sqlx::query("SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at,encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,backoff_cap_seconds,not_before FROM runs WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(run_record_from_row).transpose()
    }

    pub async fn queued_runs(&self, limit: u32) -> Result<Vec<RunRecord>> {
        let rows = sqlx::query("SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at,encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,backoff_cap_seconds,not_before FROM runs WHERE state='queued' AND not_before<=? ORDER BY created_at LIMIT ?")
            .bind(now_string())
            .bind(limit.min(500))
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(run_record_from_row).collect()
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
            return Ok(None);
        };
        if row.get::<String, _>("state") != "queued" {
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
        }
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
    ) -> Result<()> {
        let expires = format_time(Utc::now() + Duration::seconds(lease_seconds as i64));
        let mut tx = self.pool.begin().await?;
        for attempt_id in attempt_ids {
            sqlx::query("UPDATE attempts SET lease_expires_at=?,updated_at=? WHERE id=? AND agent_id=? AND state='accepted'")
                .bind(&expires).bind(now_string()).bind(attempt_id).bind(agent_id).execute(&mut *tx).await?;
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
        Ok(())
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
        let row = sqlx::query("SELECT a.run_id,a.state AS attempt_state,r.attempt_count,r.max_attempts,r.initial_backoff_seconds,r.backoff_cap_seconds,r.state AS run_state FROM attempts a JOIN runs r ON r.id=a.run_id WHERE a.id=? AND a.lease_token=?")
            .bind(attempt_id.to_string()).bind(lease_token).fetch_optional(&mut *tx).await?;
        let Some(row) = row else {
            bail!("attempt or lease token is invalid");
        };
        let run_id: String = row.get("run_id");
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
        let result = sqlx::query("UPDATE runs SET state='cancelled',updated_at=? WHERE id=? AND state IN ('queued','running')")
            .bind(now_string()).bind(run_id.to_string()).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            bail!("run is already terminal or does not exist");
        }
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
        let result = sqlx::query("UPDATE runs SET state='queued',attempt_count=0,attempt_offset=(SELECT COUNT(*) FROM attempts WHERE run_id=? AND accepted_at IS NOT NULL),not_before=?,updated_at=? WHERE id=? AND state='failed'")
            .bind(run_id.to_string())
            .bind(now_string()).bind(now_string()).bind(run_id.to_string()).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            bail!("only failed runs can be retried");
        }
        self.append_audit("run", &run_id.to_string(), "run.retried", Value::Null)
            .await
    }

    pub async fn upsert_agent(
        &self,
        id: &str,
        hostname: &str,
        labels: &std::collections::HashMap<String, String>,
        capacity: u32,
        running: u32,
    ) -> Result<NodeSettings> {
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
        append_audit_tx(
            &mut tx,
            "agent",
            id,
            "agent.connected",
            serde_json::json!({"capacity": capacity}),
        )
        .await?;
        tx.commit().await?;
        Ok(settings)
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
    ) -> Result<()> {
        sqlx::query("UPDATE agents SET applied_settings_revision=?,settings_error=?,updated_at=? WHERE id=?")
            .bind(revision).bind(error).bind(now_string()).bind(id).execute(&self.pool).await?;
        self.append_audit(
            "agent",
            id,
            "settings.applied",
            serde_json::json!({"revision": revision, "error": error}),
        )
        .await
    }

    pub async fn list_agents(&self) -> Result<Vec<AgentView>> {
        let rows = sqlx::query("SELECT id,hostname,labels_json,capacity,running,connected,desired_settings_revision,applied_settings_revision,last_seen_at FROM agents ORDER BY id")
            .fetch_all(&self.pool).await?;
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
            bail!("settings edit lock is required");
        };
        if lock.get::<String, _>("lock_token") != lock_token
            || parse_time(lock.get("expires_at"))? <= now
        {
            bail!("settings edit lock is invalid or expired");
        }
        let result = sqlx::query("UPDATE settings_documents SET document_json=?,revision=revision+1,updated_at=? WHERE document_key=? AND revision=?")
            .bind(document_json).bind(format_time(now)).bind(document_key).bind(expected_revision).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            bail!("settings revision conflict");
        }
        let revision = expected_revision + 1;
        if let Some(agent_id) = document_key.strip_prefix("node:") {
            sqlx::query("UPDATE agents SET desired_settings_revision=?,updated_at=? WHERE id=?")
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
            bail!(
                "settings are currently being edited by {}",
                row.get::<String, _>("owner_session")
            );
        }
        tx.commit().await?;
        Ok(EditLock {
            document_key: document_key.into(),
            owner_session: owner.into(),
            lock_token: token,
            expires_at: expires,
        })
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
            sqlx::query("DELETE FROM edit_locks WHERE document_key=?")
                .bind(document_key)
                .execute(&self.pool)
                .await?;
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
    use std::collections::BTreeMap;

    use scheduler_core::{ArtifactRef, ExecutionPolicy, ScheduleSpec};
    use tempfile::TempDir;

    use super::*;

    async fn test_store() -> (TempDir, Store) {
        let directory = tempfile::tempdir().expect("tempdir");
        let database = directory.path().join("test.db");
        let url = format!("sqlite://{}", database.display());
        let store = Store::connect(&url, None).await.expect("store");
        (directory, store)
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

    #[tokio::test]
    async fn settings_are_locked_and_revision_checked() {
        let (_directory, store) = test_store().await;
        let settings = store.get_global_settings().await.expect("settings");
        let lock = store
            .acquire_lock("global", "session-one")
            .await
            .expect("lock");
        assert!(store.acquire_lock("global", "session-two").await.is_err());
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
    async fn pause_and_resume_update_the_trigger_facing_schedule_document() {
        let (_directory, store) = test_store().await;
        let schedule_id = create_schedule(&store).await;

        store
            .set_schedule_enabled(schedule_id, false)
            .await
            .expect("pause");
        let paused = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert!(!paused.view.spec.enabled);

        store
            .set_schedule_enabled(schedule_id, true)
            .await
            .expect("resume");
        let resumed = store
            .get_schedule_record(schedule_id)
            .await
            .expect("read")
            .expect("schedule");
        assert!(resumed.view.spec.enabled);
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

        store.retry_run(run_id).await.expect("manual retry");
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
    }
}
