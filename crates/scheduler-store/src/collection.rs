use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use scheduler_core::{BatchItemState, BatchState, CronSpec};
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

use super::{
    Store, append_audit_tx, format_time, insert_trigger_identity_tx, now_string, parse_time,
};

const START_CURSOR_DIGEST: &str = "start";

#[derive(Debug, Clone)]
pub struct NewBatch {
    pub id: Uuid,
    pub schedule_id: Uuid,
    pub schedule_revision: i64,
    pub trigger_kind: String,
    pub scheduled_at: DateTime<Utc>,
    pub idempotency_key: Option<String>,
    pub encrypted_snapshot: Vec<u8>,
    pub encrypted_trigger_overrides: Option<Vec<u8>>,
    pub snapshot_digest: String,
    pub key_id: String,
    pub page_size: u32,
    pub max_items: u32,
    pub max_active_runs: u32,
    pub poison_distinct_nodes: u32,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BatchView {
    pub id: Uuid,
    pub schedule_id: Uuid,
    pub schedule_revision: i64,
    pub state: BatchState,
    pub trigger_kind: String,
    pub scheduled_at: DateTime<Utc>,
    pub item_count: u32,
    pub valid_item_count: u32,
    pub invalid_item_count: u32,
    pub poisoned_item_count: u32,
    pub held_item_count: u32,
    pub failure_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct BatchRecord {
    pub view: BatchView,
    pub encrypted_snapshot: Vec<u8>,
    pub encrypted_trigger_overrides: Option<Vec<u8>>,
    pub snapshot_digest: String,
    pub key_id: String,
    pub page_size: u32,
    pub max_items: u32,
    pub max_active_runs: u32,
    pub poison_distinct_nodes: u32,
    pub collection_snapshot_encrypted: Option<Vec<u8>>,
    pub collection_snapshot_digest: Option<String>,
    pub next_cursor_encrypted: Option<Vec<u8>>,
    pub next_cursor_digest: String,
    pub cursor_generation: u64,
    pub ingestion_complete: bool,
    pub lease_owner: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct NewBatchItem {
    pub id: Uuid,
    pub item_index: u32,
    pub provider_key_encrypted: Vec<u8>,
    pub provider_key_hmac: String,
    pub encrypted_parameters: Vec<u8>,
    pub encrypted_snapshot: Option<Vec<u8>>,
    pub key_id: String,
    pub parameters_digest: String,
    pub state: BatchItemState,
    pub failure_code: Option<String>,
    pub max_attempts: Option<u32>,
    pub initial_backoff_seconds: Option<u64>,
    pub backoff_cap_seconds: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BatchItemView {
    pub id: Uuid,
    pub batch_id: Uuid,
    pub item_index: u32,
    pub parameters_digest: String,
    pub state: BatchItemState,
    pub failure_code: Option<String>,
    pub run_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CommitCollectionPage {
    pub batch_id: Uuid,
    pub lease_token: String,
    pub expected_generation: u64,
    pub request_cursor_digest: String,
    pub page_digest: String,
    pub collection_snapshot_encrypted: Vec<u8>,
    pub collection_snapshot_digest: String,
    pub next_cursor_encrypted: Option<Vec<u8>>,
    pub next_cursor_digest: String,
    pub is_final: bool,
    pub items: Vec<NewBatchItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitPageOutcome {
    Applied,
    Replayed,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeBatchOutcome {
    Finalized,
    AlreadyFinalized,
}

#[derive(Debug, Clone)]
pub enum CronBatchOccurrenceResult {
    Applied(BatchView),
    StaleSchedule,
}

impl Store {
    /// Returns an exact, low-cardinality snapshot for batch-state gauges.
    pub async fn batch_state_counts(&self) -> Result<Vec<(BatchState, u64)>> {
        let rows = sqlx::query("SELECT state,COUNT(*) AS count FROM batches GROUP BY state")
            .fetch_all(self.pool())
            .await?;
        let mut counts = [0_u64; 7];
        for row in rows {
            let state = BatchState::parse(&row.get::<String, _>("state"))?;
            let index = match state {
                BatchState::Scheduled => 0,
                BatchState::Collecting => 1,
                BatchState::Running => 2,
                BatchState::Succeeded => 3,
                BatchState::CompletedWithErrors => 4,
                BatchState::Failed => 5,
                BatchState::Cancelled => 6,
            };
            counts[index] = u64::try_from(row.get::<i64, _>("count")).unwrap_or_default();
        }
        Ok([
            BatchState::Scheduled,
            BatchState::Collecting,
            BatchState::Running,
            BatchState::Succeeded,
            BatchState::CompletedWithErrors,
            BatchState::Failed,
            BatchState::Cancelled,
        ]
        .into_iter()
        .zip(counts)
        .collect())
    }

    /// Atomically fences a cron schedule revision, advances its cursor, and
    /// materializes exactly one collection batch for the occurrence.
    pub async fn create_cron_batch(
        &self,
        new: NewBatch,
        expected_revision: i64,
        expected_cron: &CronSpec,
    ) -> Result<CronBatchOccurrenceResult> {
        if new.trigger_kind != "cron" {
            bail!("cron batch occurrence must use the cron trigger kind");
        }
        if new.schedule_revision != expected_revision {
            bail!("cron batch snapshot revision does not match the fenced schedule revision");
        }
        validate_batch_limits(&new)?;
        let now = now_string();
        let occurrence = format_time(new.scheduled_at);
        let schedule_id = new.schedule_id.to_string();
        let batch_id = new.id.to_string();
        let trigger_id = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let fenced = sqlx::query(
            "UPDATE schedules SET last_cron_at=CASE WHEN last_cron_at IS NULL OR last_cron_at<? THEN ? ELSE last_cron_at END WHERE id=? AND revision=? AND enabled=1 AND cron_expression=? AND cron_timezone=?",
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
            return Ok(CronBatchOccurrenceResult::StaleSchedule);
        }

        let trigger = insert_trigger_identity_tx(
            &mut tx,
            &trigger_id,
            &schedule_id,
            "cron",
            new.scheduled_at,
            None,
            "batch",
            &batch_id,
            &now,
            None,
            None,
        )
        .await;
        if let Err(error) = trigger {
            if error
                .as_database_error()
                .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
            {
                tx.rollback().await?;
                if let Some(existing) = self
                    .find_batch_for_trigger(new.schedule_id, new.scheduled_at, "cron", None)
                    .await?
                {
                    return Ok(CronBatchOccurrenceResult::Applied(existing));
                }
                bail!("cron occurrence was already materialized as a non-collection run");
            }
            tx.rollback().await?;
            return Err(error.into());
        }

        insert_batch_tx(&mut tx, &new, &trigger_id, &now).await?;
        append_audit_tx(
            &mut tx,
            "batch",
            &batch_id,
            "batch.scheduled",
            serde_json::json!({
                "schedule_id": new.schedule_id,
                "schedule_revision": new.schedule_revision,
                "trigger": "cron",
            }),
        )
        .await?;
        let row = sqlx::query("SELECT * FROM batches WHERE id=?")
            .bind(&batch_id)
            .fetch_one(&mut *tx)
            .await?;
        let batch = batch_view_from_row(&row)?;
        tx.commit().await?;
        Ok(CronBatchOccurrenceResult::Applied(batch))
    }

    pub async fn create_batch(&self, new: NewBatch) -> Result<BatchView> {
        validate_batch_limits(&new)?;
        let now = now_string();
        let batch_id = new.id.to_string();
        let schedule_id = new.schedule_id.to_string();
        let trigger_id = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let current_revision =
            sqlx::query_scalar::<_, i64>("SELECT revision FROM schedules WHERE id=?")
                .bind(&schedule_id)
                .fetch_optional(&mut *tx)
                .await?
                .context("schedule not found")?;
        if current_revision != new.schedule_revision {
            tx.rollback().await?;
            bail!("schedule revision changed before batch creation");
        }

        let trigger = insert_trigger_identity_tx(
            &mut tx,
            &trigger_id,
            &schedule_id,
            &new.trigger_kind,
            new.scheduled_at,
            new.idempotency_key.as_deref(),
            "batch",
            &batch_id,
            &now,
            None,
            None,
        )
        .await;
        if let Err(error) = trigger {
            if error
                .as_database_error()
                .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
            {
                tx.rollback().await?;
                if let Some(existing) = self
                    .find_batch_for_trigger(
                        new.schedule_id,
                        new.scheduled_at,
                        &new.trigger_kind,
                        new.idempotency_key.as_deref(),
                    )
                    .await?
                {
                    return Ok(existing);
                }
                bail!("trigger was already materialized as a non-collection run");
            }
            return Err(error.into());
        }

        insert_batch_tx(&mut tx, &new, &trigger_id, &now).await?;
        append_audit_tx(
            &mut tx,
            "batch",
            &batch_id,
            "batch.scheduled",
            serde_json::json!({
                "schedule_id": new.schedule_id,
                "schedule_revision": new.schedule_revision,
                "trigger": new.trigger_kind,
            }),
        )
        .await?;
        tx.commit().await?;
        self.get_batch(new.id)
            .await?
            .map(|record| record.view)
            .context("created batch missing")
    }

    /// Retriggers the exact immutable snapshot of a historical batch. This is
    /// intentionally distinct from normal trigger creation: a later schedule
    /// edit must not make an operator-requested replay impossible or silently
    /// substitute the current revision.
    pub async fn retrigger_batch_snapshot(
        &self,
        source_batch_id: Uuid,
        new_batch_id: Uuid,
        scheduled_at: DateTime<Utc>,
    ) -> Result<BatchView> {
        let now = now_string();
        let trigger_id = Uuid::new_v4().to_string();
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = sqlx::query("SELECT * FROM batches WHERE id=?")
            .bind(source_batch_id.to_string())
            .fetch_optional(&mut *tx)
            .await?
            .context("source batch not found")?;
        let source = batch_record_from_row(row)?;
        let new = NewBatch {
            id: new_batch_id,
            schedule_id: source.view.schedule_id,
            schedule_revision: source.view.schedule_revision,
            trigger_kind: "retrigger".into(),
            scheduled_at,
            idempotency_key: None,
            encrypted_snapshot: source.encrypted_snapshot,
            encrypted_trigger_overrides: source.encrypted_trigger_overrides,
            snapshot_digest: source.snapshot_digest,
            key_id: source.key_id,
            page_size: source.page_size,
            max_items: source.max_items,
            max_active_runs: source.max_active_runs,
            poison_distinct_nodes: source.poison_distinct_nodes,
        };
        validate_batch_limits(&new)?;
        insert_trigger_identity_tx(
            &mut tx,
            &trigger_id,
            &new.schedule_id.to_string(),
            "retrigger",
            scheduled_at,
            None,
            "batch",
            &new_batch_id.to_string(),
            &now,
            None,
            None,
        )
        .await?;
        insert_batch_tx(&mut tx, &new, &trigger_id, &now).await?;
        append_audit_tx(
            &mut tx,
            "batch",
            &new_batch_id.to_string(),
            "batch.retriggered",
            serde_json::json!({
                "source_batch_id": source_batch_id,
                "schedule_id": new.schedule_id,
                "schedule_revision": new.schedule_revision,
            }),
        )
        .await?;
        let row = sqlx::query("SELECT * FROM batches WHERE id=?")
            .bind(new_batch_id.to_string())
            .fetch_one(&mut *tx)
            .await?;
        let view = batch_view_from_row(&row)?;
        tx.commit().await?;
        Ok(view)
    }

    async fn find_batch_for_trigger(
        &self,
        schedule_id: Uuid,
        scheduled_at: DateTime<Utc>,
        trigger_kind: &str,
        idempotency_key: Option<&str>,
    ) -> Result<Option<BatchView>> {
        let row = if let Some(key) = idempotency_key {
            sqlx::query(
                "SELECT b.* FROM trigger_identities t JOIN batches b ON b.id=t.target_id WHERE t.schedule_id=? AND t.idempotency_key=? AND t.target_kind='batch'",
            )
            .bind(schedule_id.to_string())
            .bind(key)
            .fetch_optional(&self.pool)
            .await?
        } else {
            sqlx::query(
                "SELECT b.* FROM trigger_identities t JOIN batches b ON b.id=t.target_id WHERE t.schedule_id=? AND t.scheduled_at=? AND t.trigger_kind=? AND t.target_kind='batch'",
            )
            .bind(schedule_id.to_string())
            .bind(format_time(scheduled_at))
            .bind(trigger_kind)
            .fetch_optional(&self.pool)
            .await?
        };
        row.as_ref().map(batch_view_from_row).transpose()
    }

    pub async fn get_batch(&self, id: Uuid) -> Result<Option<BatchRecord>> {
        let row = sqlx::query("SELECT * FROM batches WHERE id=?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.map(batch_record_from_row).transpose()
    }

    pub async fn list_batches(&self, limit: u32) -> Result<Vec<BatchView>> {
        let rows = sqlx::query("SELECT * FROM batches ORDER BY created_at DESC,id DESC LIMIT ?")
            .bind(i64::from(limit.clamp(1, 1_000)))
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(batch_view_from_row).collect()
    }

    pub async fn list_batch_items(&self, batch_id: Uuid, limit: u32) -> Result<Vec<BatchItemView>> {
        let rows = sqlx::query(
            "SELECT id,batch_id,item_index,parameters_digest,state,failure_code,run_id,created_at,updated_at FROM batch_items WHERE batch_id=? ORDER BY item_index LIMIT ?",
        )
        .bind(batch_id.to_string())
        .bind(i64::from(limit.clamp(1, 10_000)))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(batch_item_view_from_row).collect()
    }

    /// Claims due or expired collection work. The lease token fences page commits
    /// from a worker which resumes after another coordinator worker took over.
    pub async fn claim_collection_batches(
        &self,
        owner: &str,
        lease_seconds: u64,
        limit: u32,
    ) -> Result<Vec<BatchRecord>> {
        if owner.trim().is_empty() {
            bail!("collection lease owner must not be empty");
        }
        let now = Utc::now();
        let rows = sqlx::query(
            "SELECT id,state,lease_token,lease_expires_at FROM batches WHERE state='scheduled' OR (state='collecting' AND lease_expires_at<?) ORDER BY created_at,id LIMIT ?",
        )
        .bind(format_time(now))
        .bind(i64::from(limit.clamp(1, 100)))
        .fetch_all(&self.pool)
        .await?;
        let mut claimed = Vec::new();
        for row in rows {
            let id: String = row.get("id");
            let old_state: String = row.get("state");
            let old_token: Option<String> = row.get("lease_token");
            let old_expiry: Option<String> = row.get("lease_expires_at");
            let token = Uuid::new_v4().to_string();
            let mut tx = self.pool.begin().await?;
            let result = sqlx::query(
                "UPDATE batches SET state='collecting',lease_owner=?,lease_token=?,lease_expires_at=?,updated_at=? WHERE id=? AND state=? AND lease_token IS ? AND lease_expires_at IS ?",
            )
            .bind(owner)
            .bind(&token)
            .bind(format_time(now + Duration::seconds(lease_seconds.max(1) as i64)))
            .bind(format_time(now))
            .bind(&id)
            .bind(&old_state)
            .bind(old_token)
            .bind(old_expiry)
            .execute(&mut *tx)
            .await?;
            if result.rows_affected() == 1 {
                append_audit_tx(
                    &mut tx,
                    "batch",
                    &id,
                    if old_state == "scheduled" {
                        "batch.collection_started"
                    } else {
                        "batch.collection_lease_recovered"
                    },
                    serde_json::json!({"from": old_state, "to": "collecting"}),
                )
                .await?;
                tx.commit().await?;
                let batch_id = Uuid::parse_str(&id)?;
                claimed.push(
                    self.get_batch(batch_id)
                        .await?
                        .context("claimed batch missing")?,
                );
            } else {
                tx.rollback().await?;
            }
        }
        Ok(claimed)
    }

    pub async fn renew_collection_lease(
        &self,
        batch_id: Uuid,
        lease_token: &str,
        lease_seconds: u64,
    ) -> Result<bool> {
        let now = Utc::now();
        let result = sqlx::query(
            "UPDATE batches SET lease_expires_at=?,updated_at=? WHERE id=? AND state='collecting' AND lease_token=? AND lease_expires_at>?",
        )
        .bind(format_time(now + Duration::seconds(lease_seconds.max(1) as i64)))
        .bind(format_time(now))
        .bind(batch_id.to_string())
        .bind(lease_token)
        .bind(format_time(now))
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn commit_collection_page(
        &self,
        page: CommitCollectionPage,
    ) -> Result<CommitPageOutcome> {
        validate_page(&page)?;
        let batch_id = page.batch_id.to_string();
        let now = now_string();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT state,lease_token,lease_expires_at,cursor_generation,next_cursor_digest,collection_snapshot_digest,page_size,max_items,item_count,ingestion_complete FROM batches WHERE id=?",
        )
        .bind(&batch_id)
        .fetch_optional(&mut *tx)
        .await?
        .context("batch not found")?;

        let generation = row.get::<i64, _>("cursor_generation") as u64;
        if generation != page.expected_generation {
            let replay = sqlx::query(
                "SELECT page_digest FROM batch_collection_pages WHERE batch_id=? AND request_cursor_digest=?",
            )
            .bind(&batch_id)
            .bind(&page.request_cursor_digest)
            .fetch_optional(&mut *tx)
            .await?;
            tx.rollback().await?;
            return match replay {
                Some(row) if row.get::<String, _>("page_digest") == page.page_digest => {
                    Ok(CommitPageOutcome::Replayed)
                }
                _ => Ok(CommitPageOutcome::Stale),
            };
        }

        let state: String = row.get("state");
        let token: Option<String> = row.get("lease_token");
        let lease_expires_at: Option<String> = row.get("lease_expires_at");
        if state != "collecting"
            || row.get::<bool, _>("ingestion_complete")
            || token.as_deref() != Some(page.lease_token.as_str())
            || lease_expires_at
                .map(parse_time)
                .transpose()?
                .is_none_or(|expiry| expiry <= Utc::now())
        {
            tx.rollback().await?;
            return Ok(CommitPageOutcome::Stale);
        }
        if row.get::<String, _>("next_cursor_digest") != page.request_cursor_digest {
            tx.rollback().await?;
            return Ok(CommitPageOutcome::Stale);
        }
        let prior_snapshot: Option<String> = row.get("collection_snapshot_digest");
        if prior_snapshot
            .as_deref()
            .is_some_and(|digest| digest != page.collection_snapshot_digest)
        {
            tx.rollback().await?;
            bail!("collection snapshot changed while ingesting batch");
        }
        let existing_count = row.get::<i64, _>("item_count") as usize;
        let page_size = row.get::<i64, _>("page_size") as usize;
        let max_items = row.get::<i64, _>("max_items") as usize;
        if page.items.len() > page_size {
            tx.rollback().await?;
            bail!("parameter collection page exceeds configured page_size");
        }
        if existing_count.saturating_add(page.items.len()) > max_items {
            tx.rollback().await?;
            bail!("parameter collection exceeds configured max_items");
        }

        sqlx::query(
            "INSERT INTO batch_collection_pages(batch_id,generation,request_cursor_digest,page_digest,collection_snapshot_digest,next_cursor_encrypted,next_cursor_digest,item_count,is_final,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&batch_id)
        .bind(page.expected_generation as i64)
        .bind(&page.request_cursor_digest)
        .bind(&page.page_digest)
        .bind(&page.collection_snapshot_digest)
        .bind(&page.next_cursor_encrypted)
        .bind(&page.next_cursor_digest)
        .bind(page.items.len() as i64)
        .bind(page.is_final)
        .bind(&now)
        .execute(&mut *tx)
        .await?;

        let mut counts = PageCounts::default();
        for mut item in page.items {
            let duplicate = sqlx::query(
                "SELECT id,state,parameters_digest FROM batch_items WHERE batch_id=? AND provider_key_hmac=? ORDER BY item_index LIMIT 1",
            )
            .bind(&batch_id)
            .bind(&item.provider_key_hmac)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(duplicate) = duplicate {
                let prior_digest: String = duplicate.get("parameters_digest");
                if prior_digest != item.parameters_digest {
                    tx.rollback().await?;
                    bail!("collection_conflicting_duplicate_key");
                }
                let prior_state =
                    BatchItemState::parse(duplicate.get::<String, _>("state").as_str())?;
                if prior_state != BatchItemState::Invalid {
                    sqlx::query(
                        "UPDATE batch_items SET state='invalid',failure_code='collection_duplicate_key',updated_at=? WHERE id=?",
                    )
                    .bind(&now)
                    .bind(duplicate.get::<String, _>("id"))
                    .execute(&mut *tx)
                    .await?;
                    counts.reclassify_as_invalid(prior_state);
                }
                item.state = BatchItemState::Invalid;
                item.failure_code = Some("collection_duplicate_key".into());
            }
            counts.observe(item.state);
            insert_batch_item(&mut tx, &batch_id, item, &now).await?;
        }

        let result = sqlx::query(
            "UPDATE batches SET collection_snapshot_encrypted=COALESCE(collection_snapshot_encrypted,?),collection_snapshot_digest=COALESCE(collection_snapshot_digest,?),next_cursor_encrypted=?,next_cursor_digest=?,cursor_generation=cursor_generation+1,ingestion_complete=?,item_count=item_count+?,valid_item_count=valid_item_count+?,invalid_item_count=invalid_item_count+?,poisoned_item_count=poisoned_item_count+?,held_item_count=held_item_count+?,updated_at=? WHERE id=? AND state='collecting' AND lease_token=? AND cursor_generation=? AND next_cursor_digest=?",
        )
        .bind(page.collection_snapshot_encrypted)
        .bind(&page.collection_snapshot_digest)
        .bind(page.next_cursor_encrypted)
        .bind(&page.next_cursor_digest)
        .bind(page.is_final)
        .bind(counts.total)
        .bind(counts.ready)
        .bind(counts.invalid)
        .bind(counts.poisoned)
        .bind(counts.held)
        .bind(&now)
        .bind(&batch_id)
        .bind(&page.lease_token)
        .bind(page.expected_generation as i64)
        .bind(&page.request_cursor_digest)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(CommitPageOutcome::Stale);
        }
        append_audit_tx(
            &mut tx,
            "batch",
            &batch_id,
            "batch.page_committed",
            serde_json::json!({
                "generation": page.expected_generation,
                "items": counts.total,
                "valid": counts.ready,
                "invalid": counts.invalid,
                "is_final": page.is_final,
            }),
        )
        .await?;
        tx.commit().await?;
        Ok(CommitPageOutcome::Applied)
    }

    /// Atomically materializes every validated item into one ordinary queued
    /// run. A crash before commit leaves no runs; replay after commit observes
    /// the running/terminal state and does not duplicate them.
    pub async fn finalize_batch(
        &self,
        batch_id: Uuid,
        lease_token: &str,
    ) -> Result<FinalizeBatchOutcome> {
        let id = batch_id.to_string();
        let now = now_string();
        let mut tx = self.pool.begin().await?;
        let batch = sqlx::query(
            "SELECT schedule_id,state,trigger_kind,scheduled_at,ingestion_complete,invalid_item_count,poisoned_item_count,held_item_count,lease_token,lease_expires_at FROM batches WHERE id=?",
        )
        .bind(&id)
        .fetch_optional(&mut *tx)
        .await?
        .context("batch not found")?;
        let state: String = batch.get("state");
        if matches!(
            state.as_str(),
            "running" | "succeeded" | "completed_with_errors" | "failed" | "cancelled"
        ) {
            tx.rollback().await?;
            return Ok(FinalizeBatchOutcome::AlreadyFinalized);
        }
        if state != "collecting"
            || !batch.get::<bool, _>("ingestion_complete")
            || batch.get::<Option<String>, _>("lease_token").as_deref() != Some(lease_token)
            || batch
                .get::<Option<String>, _>("lease_expires_at")
                .map(parse_time)
                .transpose()?
                .is_none_or(|expiry| expiry <= Utc::now())
        {
            tx.rollback().await?;
            bail!("batch is not ready for finalization or collection lease was lost");
        }
        let schedule_id: String = batch.get("schedule_id");
        let scheduled_at: String = batch.get("scheduled_at");
        let mut materialized_runs = 0_usize;
        loop {
            // Keep peak memory bounded even when a batch contains 10,000
            // relatively large encrypted execution snapshots.
            let items = sqlx::query(
                "SELECT id,encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,backoff_cap_seconds FROM batch_items WHERE batch_id=? AND state='ready' ORDER BY item_index LIMIT 100",
            )
            .bind(&id)
            .fetch_all(&mut *tx)
            .await?;
            if items.is_empty() {
                break;
            }
            for item in items {
                let item_id: String = item.get("id");
                let encrypted_snapshot: Option<Vec<u8>> = item.get("encrypted_snapshot");
                let max_attempts: Option<i64> = item.get("max_attempts");
                let initial_backoff: Option<i64> = item.get("initial_backoff_seconds");
                let backoff_cap: Option<i64> = item.get("backoff_cap_seconds");
                let (Some(snapshot), Some(max_attempts), Some(initial_backoff), Some(backoff_cap)) = (
                    encrypted_snapshot,
                    max_attempts,
                    initial_backoff,
                    backoff_cap,
                ) else {
                    bail!("ready batch item is missing its immutable execution snapshot or policy");
                };
                let run_id = Uuid::new_v4().to_string();
                sqlx::query(
                    "INSERT INTO runs(id,schedule_id,state,trigger_kind,scheduled_at,not_before,encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,backoff_cap_seconds,idempotency_key,created_at,updated_at,batch_id,batch_item_id) VALUES (?,?,'queued','batch_item',?,?,?,?,?,?,?,NULL,?,?,?,?)",
                )
                .bind(&run_id)
                .bind(&schedule_id)
                .bind(&scheduled_at)
                .bind(&scheduled_at)
                .bind(snapshot)
                .bind(item.get::<String, _>("key_id"))
                .bind(max_attempts)
                .bind(initial_backoff)
                .bind(backoff_cap)
                .bind(&now)
                .bind(&now)
                .bind(&id)
                .bind(&item_id)
                .execute(&mut *tx)
                .await?;
                append_audit_tx(
                    &mut tx,
                    "run",
                    &run_id,
                    "run.queued",
                    serde_json::json!({
                        "trigger": "batch_item",
                        "batch_id": batch_id,
                        "batch_item_id": item_id,
                    }),
                )
                .await?;
                sqlx::query(
                    "UPDATE batch_items SET state='queued',run_id=?,updated_at=? WHERE id=? AND state='ready'",
                )
                .bind(&run_id)
                .bind(&now)
                .bind(&item_id)
                .execute(&mut *tx)
                .await?;
                materialized_runs += 1;
            }
        }
        let errors = batch.get::<i64, _>("invalid_item_count")
            + batch.get::<i64, _>("poisoned_item_count")
            + batch.get::<i64, _>("held_item_count");
        let next_state = if materialized_runs == 0 {
            if errors > 0 {
                BatchState::CompletedWithErrors
            } else {
                BatchState::Succeeded
            }
        } else {
            BatchState::Running
        };
        sqlx::query(
            "UPDATE batches SET state=?,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,finalized_at=?,updated_at=? WHERE id=? AND state='collecting'",
        )
        .bind(next_state.as_str())
        .bind(&now)
        .bind(&now)
        .bind(&id)
        .execute(&mut *tx)
        .await?;
        append_audit_tx(
            &mut tx,
            "batch",
            &id,
            "batch.finalized",
            serde_json::json!({"runs": materialized_runs, "state": next_state.as_str()}),
        )
        .await?;
        tx.commit().await?;
        Ok(FinalizeBatchOutcome::Finalized)
    }

    pub async fn fail_batch(&self, batch_id: Uuid, failure_code: &str) -> Result<bool> {
        validate_failure_code(failure_code)?;
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE batches SET state='failed',failure_code=?,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=? WHERE id=? AND state IN ('scheduled','collecting')",
        )
        .bind(failure_code)
        .bind(now_string())
        .bind(batch_id.to_string())
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 1 {
            append_audit_tx(
                &mut tx,
                "batch",
                &batch_id.to_string(),
                "batch.failed",
                serde_json::json!({"failure_code": failure_code}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn cancel_batch(&self, batch_id: Uuid) -> Result<Vec<(String, Uuid)>> {
        let id = batch_id.to_string();
        let cancelled_at = Utc::now();
        let now = format_time(cancelled_at);
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE batches SET state='cancelled',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=? WHERE id=? AND state IN ('scheduled','collecting','running')",
        )
        .bind(&now)
        .bind(&id)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            tx.rollback().await?;
            bail!("batch is already terminal or does not exist");
        }
        sqlx::query(
            "INSERT INTO daily_schedule_stats(schedule_id,day_utc,cancelled,updated_at) \
             SELECT schedule_id,?,COUNT(*),? FROM runs \
             WHERE batch_id=? AND state IN ('queued','running') GROUP BY schedule_id \
             ON CONFLICT(schedule_id,day_utc) DO UPDATE SET \
             cancelled=cancelled+excluded.cancelled,updated_at=excluded.updated_at",
        )
        .bind(cancelled_at.format("%Y-%m-%d").to_string())
        .bind(&now)
        .bind(&id)
        .execute(&mut *tx)
        .await?;
        let cancelled_runs = sqlx::query(
            "UPDATE runs SET state='cancelled',updated_at=? \
             WHERE batch_id=? AND state IN ('queued','running') RETURNING id",
        )
        .bind(&now)
        .bind(&id)
        .fetch_all(&mut *tx)
        .await?;
        for run in cancelled_runs {
            append_audit_tx(
                &mut tx,
                "run",
                run.get::<String, _>("id").as_str(),
                "run.cancelled",
                serde_json::Value::Null,
            )
            .await?;
        }
        sqlx::query("UPDATE batch_items SET state='cancelled',updated_at=? WHERE batch_id=? AND state IN ('ready','queued','running')")
            .bind(&now)
            .bind(&id)
            .execute(&mut *tx)
            .await?;
        let attempts = sqlx::query(
            "SELECT a.agent_id,a.id FROM attempts a JOIN runs r ON r.id=a.run_id WHERE r.batch_id=? AND a.state='accepted'",
        )
        .bind(&id)
        .fetch_all(&mut *tx)
        .await?;
        append_audit_tx(
            &mut tx,
            "batch",
            &id,
            "batch.cancelled",
            serde_json::Value::Null,
        )
        .await?;
        tx.commit().await?;
        attempts
            .into_iter()
            .map(|row| {
                Ok((
                    row.get("agent_id"),
                    Uuid::parse_str(row.get::<String, _>("id").as_str())?,
                ))
            })
            .collect()
    }
}

async fn insert_batch_tx(
    tx: &mut Transaction<'_, Sqlite>,
    new: &NewBatch,
    trigger_id: &str,
    now: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO batches(id,trigger_identity_id,schedule_id,schedule_revision,state,trigger_kind,scheduled_at,encrypted_snapshot,encrypted_trigger_overrides,snapshot_digest,key_id,page_size,max_items,max_active_runs,poison_distinct_nodes,created_at,updated_at) VALUES (?,?,?,?,'scheduled',?,?,?,?,?,?,?,?,?,?,?,?)",
    )
    .bind(new.id.to_string())
    .bind(trigger_id)
    .bind(new.schedule_id.to_string())
    .bind(new.schedule_revision)
    .bind(&new.trigger_kind)
    .bind(format_time(new.scheduled_at))
    .bind(&new.encrypted_snapshot)
    .bind(&new.encrypted_trigger_overrides)
    .bind(&new.snapshot_digest)
    .bind(&new.key_id)
    .bind(i64::from(new.page_size))
    .bind(i64::from(new.max_items))
    .bind(i64::from(new.max_active_runs))
    .bind(i64::from(new.poison_distinct_nodes))
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_batch_item(
    tx: &mut Transaction<'_, Sqlite>,
    batch_id: &str,
    item: NewBatchItem,
    now: &str,
) -> Result<()> {
    if item.state == BatchItemState::Ready
        && (item.encrypted_snapshot.is_none()
            || item.max_attempts.is_none()
            || item.initial_backoff_seconds.is_none()
            || item.backoff_cap_seconds.is_none())
    {
        bail!("ready batch item must include an execution snapshot and retry policy");
    }
    if item.state != BatchItemState::Ready && item.failure_code.is_none() {
        bail!("quarantined batch item must include a safe failure code");
    }
    if let Some(code) = item.failure_code.as_deref() {
        validate_failure_code(code)?;
    }
    sqlx::query(
        "INSERT INTO batch_items(id,batch_id,item_index,provider_key_encrypted,provider_key_hmac,encrypted_parameters,encrypted_snapshot,key_id,parameters_digest,max_attempts,initial_backoff_seconds,backoff_cap_seconds,state,failure_code,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    )
    .bind(item.id.to_string())
    .bind(batch_id)
    .bind(i64::from(item.item_index))
    .bind(item.provider_key_encrypted)
    .bind(item.provider_key_hmac)
    .bind(item.encrypted_parameters)
    .bind(item.encrypted_snapshot)
    .bind(item.key_id)
    .bind(item.parameters_digest)
    .bind(item.max_attempts.map(i64::from))
    .bind(item.initial_backoff_seconds.map(|value| value as i64))
    .bind(item.backoff_cap_seconds.map(|value| value as i64))
    .bind(item.state.as_str())
    .bind(item.failure_code)
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[derive(Default)]
struct PageCounts {
    total: i64,
    ready: i64,
    invalid: i64,
    poisoned: i64,
    held: i64,
}

impl PageCounts {
    fn observe(&mut self, state: BatchItemState) {
        self.total += 1;
        match state {
            BatchItemState::Ready => self.ready += 1,
            BatchItemState::Invalid => self.invalid += 1,
            BatchItemState::Poisoned | BatchItemState::SuspectedPoison => self.poisoned += 1,
            BatchItemState::Held => self.held += 1,
            _ => unreachable!("page validation rejects execution states"),
        }
    }

    fn reclassify_as_invalid(&mut self, state: BatchItemState) {
        match state {
            BatchItemState::Ready => self.ready -= 1,
            BatchItemState::Invalid => return,
            BatchItemState::Poisoned | BatchItemState::SuspectedPoison => self.poisoned -= 1,
            BatchItemState::Held => self.held -= 1,
            _ => unreachable!("collection ingestion cannot contain an execution state"),
        }
        self.invalid += 1;
    }
}

fn validate_batch_limits(batch: &NewBatch) -> Result<()> {
    if !(1..=1_000).contains(&batch.page_size) {
        bail!("batch page_size must be between 1 and 1000");
    }
    if !(1..=10_000).contains(&batch.max_items) {
        bail!("batch max_items must be between 1 and 10000");
    }
    if !(1..=1_000).contains(&batch.max_active_runs) {
        bail!("batch max_active_runs must be between 1 and 1000");
    }
    if !(2..=32).contains(&batch.poison_distinct_nodes) {
        bail!("batch poison_distinct_nodes must be between 2 and 32");
    }
    Ok(())
}

fn validate_page(page: &CommitCollectionPage) -> Result<()> {
    if page.request_cursor_digest.trim().is_empty()
        || page.page_digest.trim().is_empty()
        || page.collection_snapshot_digest.trim().is_empty()
        || page.next_cursor_digest.trim().is_empty()
    {
        bail!("collection page digests must not be empty");
    }
    if page.expected_generation == 0 && page.request_cursor_digest != START_CURSOR_DIGEST {
        bail!("the first collection page must use the start cursor digest");
    }
    if page.is_final && page.next_cursor_encrypted.is_some() {
        bail!("a final collection page cannot carry a next cursor");
    }
    if !page.is_final && page.next_cursor_encrypted.is_none() {
        bail!("a non-final collection page must carry an encrypted next cursor");
    }
    let mut indexes = HashSet::with_capacity(page.items.len());
    for item in &page.items {
        if item.provider_key_hmac.trim().is_empty() || item.parameters_digest.trim().is_empty() {
            bail!("batch item digests must not be empty");
        }
        if !indexes.insert(item.item_index) {
            bail!("collection page contains duplicate item indexes");
        }
        if !matches!(
            item.state,
            BatchItemState::Ready
                | BatchItemState::Invalid
                | BatchItemState::SuspectedPoison
                | BatchItemState::Poisoned
                | BatchItemState::Held
        ) {
            bail!("collection page contains an item in an execution state");
        }
    }
    Ok(())
}

fn validate_failure_code(code: &str) -> Result<()> {
    if code.is_empty()
        || code.len() > 64
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        bail!("failure code must contain 1-64 lowercase ASCII letters, digits, or underscores");
    }
    Ok(())
}

fn batch_view_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<BatchView> {
    Ok(BatchView {
        id: Uuid::parse_str(row.get::<String, _>("id").as_str())?,
        schedule_id: Uuid::parse_str(row.get::<String, _>("schedule_id").as_str())?,
        schedule_revision: row.get("schedule_revision"),
        state: BatchState::parse(row.get::<String, _>("state").as_str())?,
        trigger_kind: row.get("trigger_kind"),
        scheduled_at: parse_time(row.get("scheduled_at"))?,
        item_count: row.get::<i64, _>("item_count") as u32,
        valid_item_count: row.get::<i64, _>("valid_item_count") as u32,
        invalid_item_count: row.get::<i64, _>("invalid_item_count") as u32,
        poisoned_item_count: row.get::<i64, _>("poisoned_item_count") as u32,
        held_item_count: row.get::<i64, _>("held_item_count") as u32,
        failure_code: row.get("failure_code"),
        created_at: parse_time(row.get("created_at"))?,
        updated_at: parse_time(row.get("updated_at"))?,
    })
}

fn batch_record_from_row(row: sqlx::sqlite::SqliteRow) -> Result<BatchRecord> {
    let view = batch_view_from_row(&row)?;
    Ok(BatchRecord {
        view,
        encrypted_snapshot: row.get("encrypted_snapshot"),
        encrypted_trigger_overrides: row.get("encrypted_trigger_overrides"),
        snapshot_digest: row.get("snapshot_digest"),
        key_id: row.get("key_id"),
        page_size: row.get::<i64, _>("page_size") as u32,
        max_items: row.get::<i64, _>("max_items") as u32,
        max_active_runs: row.get::<i64, _>("max_active_runs") as u32,
        poison_distinct_nodes: row.get::<i64, _>("poison_distinct_nodes") as u32,
        collection_snapshot_encrypted: row.get("collection_snapshot_encrypted"),
        collection_snapshot_digest: row.get("collection_snapshot_digest"),
        next_cursor_encrypted: row.get("next_cursor_encrypted"),
        next_cursor_digest: row.get("next_cursor_digest"),
        cursor_generation: row.get::<i64, _>("cursor_generation") as u64,
        ingestion_complete: row.get("ingestion_complete"),
        lease_owner: row.get("lease_owner"),
        lease_token: row.get("lease_token"),
        lease_expires_at: row
            .get::<Option<String>, _>("lease_expires_at")
            .map(parse_time)
            .transpose()?,
    })
}

fn batch_item_view_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<BatchItemView> {
    Ok(BatchItemView {
        id: Uuid::parse_str(row.get::<String, _>("id").as_str())?,
        batch_id: Uuid::parse_str(row.get::<String, _>("batch_id").as_str())?,
        item_index: row.get::<i64, _>("item_index") as u32,
        parameters_digest: row.get("parameters_digest"),
        state: BatchItemState::parse(row.get::<String, _>("state").as_str())?,
        failure_code: row.get("failure_code"),
        run_id: row
            .get::<Option<String>, _>("run_id")
            .map(|id| Uuid::parse_str(&id))
            .transpose()?,
        created_at: parse_time(row.get("created_at"))?,
        updated_at: parse_time(row.get("updated_at"))?,
    })
}

pub(crate) async fn sync_batch_run_state_tx(
    tx: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    run_state: &str,
    now: &str,
) -> Result<()> {
    let link = sqlx::query("SELECT batch_id,batch_item_id FROM runs WHERE id=?")
        .bind(run_id)
        .fetch_optional(&mut **tx)
        .await?;
    let Some(link) = link else {
        return Ok(());
    };
    let batch_id: Option<String> = link.get("batch_id");
    let item_id: Option<String> = link.get("batch_item_id");
    let (Some(batch_id), Some(item_id)) = (batch_id, item_id) else {
        return Ok(());
    };
    let item_state = match run_state {
        "queued" => "queued",
        "running" => "running",
        "succeeded" => "succeeded",
        "failed" => "failed",
        "cancelled" => "cancelled",
        _ => bail!("unknown linked run state {run_state}"),
    };
    sqlx::query("UPDATE batch_items SET state=?,updated_at=? WHERE id=? AND batch_id=?")
        .bind(item_state)
        .bind(now)
        .bind(item_id)
        .bind(&batch_id)
        .execute(&mut **tx)
        .await?;

    if matches!(run_state, "queued" | "running") {
        let reopened = sqlx::query(
            "UPDATE batches SET state='running',updated_at=? WHERE id=? AND state IN ('succeeded','completed_with_errors')",
        )
        .bind(now)
        .bind(&batch_id)
        .execute(&mut **tx)
        .await?;
        if reopened.rows_affected() == 1 {
            append_audit_tx(
                tx,
                "batch",
                &batch_id,
                "batch.reopened",
                serde_json::json!({"run_id": run_id}),
            )
            .await?;
        }
    }

    if matches!(run_state, "succeeded" | "failed" | "cancelled") {
        let aggregate = sqlx::query(
            "SELECT COUNT(*) AS total,SUM(CASE WHEN state IN ('succeeded','failed','cancelled') THEN 1 ELSE 0 END) AS terminal,SUM(CASE WHEN state='succeeded' THEN 1 ELSE 0 END) AS succeeded FROM runs WHERE batch_id=?",
        )
        .bind(&batch_id)
        .fetch_one(&mut **tx)
        .await?;
        let total: i64 = aggregate.get("total");
        let terminal: i64 = aggregate.get("terminal");
        if total > 0 && terminal == total {
            let succeeded: i64 = aggregate.get("succeeded");
            let quarantined: i64 = sqlx::query_scalar(
                "SELECT invalid_item_count+poisoned_item_count+held_item_count FROM batches WHERE id=?",
            )
            .bind(&batch_id)
            .fetch_one(&mut **tx)
            .await?;
            let batch_state = if succeeded == total && quarantined == 0 {
                "succeeded"
            } else {
                "completed_with_errors"
            };
            let result = sqlx::query(
                "UPDATE batches SET state=?,updated_at=? WHERE id=? AND state='running'",
            )
            .bind(batch_state)
            .bind(now)
            .bind(&batch_id)
            .execute(&mut **tx)
            .await?;
            if result.rows_affected() == 1 {
                append_audit_tx(
                    tx,
                    "batch",
                    &batch_id,
                    "batch.completed",
                    serde_json::json!({"state": batch_state}),
                )
                .await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use scheduler_core::{
        ArtifactRef, ExecutionOutcome, ExecutionResult, OutputMetadata, ScheduleSpec,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::{NewRun, NewSchedule};

    struct TestDatabase {
        _directory: TempDir,
        url: String,
        store: Store,
    }

    impl TestDatabase {
        async fn new() -> Self {
            let directory = tempfile::tempdir().expect("tempdir");
            let path = directory.path().join("collections.db");
            let url = format!("sqlite://{}", path.display());
            let store = Store::connect(&url, None).await.expect("store");
            Self {
                _directory: directory,
                url,
                store,
            }
        }

        async fn reopen(&mut self) {
            self.store.pool().close().await;
            self.store = Store::connect(&self.url, None).await.expect("reopen store");
        }
    }

    fn schedule_spec() -> ScheduleSpec {
        ScheduleSpec {
            name: "collection schedule".into(),
            blueprint_ref: ArtifactRef {
                uri: "file:///blueprint.yaml".into(),
            },
            parameters_ref: ArtifactRef {
                uri: "file:///base.json".into(),
            },
            parameter_collection: None,
            observability: Default::default(),
            required_labels: BTreeMap::new(),
            cron: None,
            webhook_enabled: true,
            enabled: true,
        }
    }

    async fn create_schedule(store: &Store) -> Uuid {
        let id = Uuid::new_v4();
        store
            .create_schedule(NewSchedule {
                id,
                spec: schedule_spec(),
                encrypted_snapshot: vec![1],
                snapshot_digest: "schedule-digest".into(),
                key_id: "v1".into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("schedule");
        id
    }

    fn new_batch(
        id: Uuid,
        schedule_id: Uuid,
        idempotency_key: Option<&str>,
        max_active_runs: u32,
    ) -> NewBatch {
        NewBatch {
            id,
            schedule_id,
            schedule_revision: 1,
            trigger_kind: "webhook".into(),
            scheduled_at: Utc::now(),
            idempotency_key: idempotency_key.map(str::to_owned),
            encrypted_snapshot: vec![2],
            encrypted_trigger_overrides: Some(vec![3]),
            snapshot_digest: "batch-digest".into(),
            key_id: "v1".into(),
            page_size: 500,
            max_items: 10_000,
            max_active_runs,
            poison_distinct_nodes: 2,
        }
    }

    fn ready_item(index: u32, key: &str) -> NewBatchItem {
        NewBatchItem {
            id: Uuid::new_v4(),
            item_index: index,
            provider_key_encrypted: vec![4, index as u8],
            provider_key_hmac: key.into(),
            encrypted_parameters: vec![5, index as u8],
            encrypted_snapshot: Some(vec![6, index as u8]),
            key_id: "v1".into(),
            parameters_digest: format!("parameters-{index}"),
            state: BatchItemState::Ready,
            failure_code: None,
            max_attempts: Some(3),
            initial_backoff_seconds: Some(1),
            backoff_cap_seconds: Some(3),
        }
    }

    fn invalid_item(index: u32, key: &str) -> NewBatchItem {
        NewBatchItem {
            id: Uuid::new_v4(),
            item_index: index,
            provider_key_encrypted: vec![7, index as u8],
            provider_key_hmac: key.into(),
            encrypted_parameters: vec![8, index as u8],
            encrypted_snapshot: None,
            key_id: "v1".into(),
            parameters_digest: format!("invalid-{index}"),
            state: BatchItemState::Invalid,
            failure_code: Some("schema_validation_failed".into()),
            max_attempts: None,
            initial_backoff_seconds: None,
            backoff_cap_seconds: None,
        }
    }

    fn page(
        batch_id: Uuid,
        lease_token: &str,
        generation: u64,
        request_cursor: &str,
        next_cursor: &str,
        is_final: bool,
        items: Vec<NewBatchItem>,
    ) -> CommitCollectionPage {
        CommitCollectionPage {
            batch_id,
            lease_token: lease_token.into(),
            expected_generation: generation,
            request_cursor_digest: request_cursor.into(),
            page_digest: format!("page-{generation}"),
            collection_snapshot_encrypted: vec![9],
            collection_snapshot_digest: "provider-snapshot".into(),
            next_cursor_encrypted: (!is_final).then(|| vec![10]),
            next_cursor_digest: next_cursor.into(),
            is_final,
            items,
        }
    }

    fn completion(outcome: ExecutionOutcome) -> ExecutionResult {
        let now = Utc::now();
        ExecutionResult {
            outcome,
            exit_code: Some(if outcome == ExecutionOutcome::Succeeded {
                0
            } else {
                1
            }),
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

    #[tokio::test]
    async fn page_commit_and_finalization_survive_restarts_and_replays() {
        let mut database = TestDatabase::new().await;
        let schedule_id = create_schedule(&database.store).await;
        let batch_id = Uuid::new_v4();
        let batch = new_batch(batch_id, schedule_id, Some("request-1"), 32);
        database
            .store
            .create_batch(batch.clone())
            .await
            .expect("create batch");
        let batch_trace = scheduler_telemetry::TraceContextFields {
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
            tracestate: Some("vendor=value".into()),
        };
        database
            .store
            .capture_batch_trace_context(batch_id, &batch_trace)
            .await
            .expect("capture batch trace");
        let duplicate = database
            .store
            .create_batch(NewBatch {
                id: Uuid::new_v4(),
                ..batch
            })
            .await
            .expect("idempotent batch create");
        assert_eq!(duplicate.id, batch_id);

        let claimed = database
            .store
            .claim_collection_batches("collector-a", 60, 10)
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);
        let token = claimed[0].lease_token.as_deref().expect("lease token");
        let committed = page(
            batch_id,
            token,
            0,
            START_CURSOR_DIGEST,
            "end",
            true,
            vec![ready_item(0, "key-a"), invalid_item(1, "key-b")],
        );
        assert_eq!(
            database
                .store
                .commit_collection_page(committed.clone())
                .await
                .expect("commit page"),
            CommitPageOutcome::Applied
        );
        assert_eq!(
            database
                .store
                .commit_collection_page(committed)
                .await
                .expect("replay page"),
            CommitPageOutcome::Replayed
        );
        let item_events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_events \
             WHERE entity_type='batch_item' AND event_type='batch_item.created'",
        )
        .fetch_one(database.store.pool())
        .await
        .expect("collection item event count");
        assert_eq!(
            item_events, 2,
            "each durable item insertion must have exactly one state event"
        );
        let item_outbox_events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM observability_outbox o JOIN audit_events a \
             ON a.id=o.audit_event_id WHERE a.entity_type='batch_item' \
             AND a.event_type='batch_item.created'",
        )
        .fetch_one(database.store.pool())
        .await
        .expect("collection item outbox count");
        assert_eq!(item_outbox_events, item_events);

        let token = token.to_owned();
        database.reopen().await;
        let recovered = database
            .store
            .get_batch(batch_id)
            .await
            .expect("read batch")
            .expect("batch");
        assert!(recovered.ingestion_complete);
        assert_eq!(
            (recovered.view.item_count, recovered.view.valid_item_count),
            (2, 1)
        );
        assert_eq!(recovered.view.invalid_item_count, 1);
        sqlx::query("UPDATE batches SET lease_expires_at=? WHERE id=?")
            .bind("2000-01-01T00:00:00.000Z")
            .bind(batch_id.to_string())
            .execute(database.store.pool())
            .await
            .expect("expire interrupted collector lease");
        let reclaimed = database
            .store
            .claim_collection_batches("collector-b", 60, 1)
            .await
            .expect("reclaim completed ingestion");
        assert_eq!(reclaimed.len(), 1);
        let replacement_token = reclaimed[0]
            .lease_token
            .as_deref()
            .expect("replacement token")
            .to_owned();
        assert!(
            database
                .store
                .finalize_batch(batch_id, &token)
                .await
                .is_err(),
            "an expired worker must be fenced from finalization"
        );
        assert_eq!(
            database
                .store
                .finalize_batch(batch_id, &replacement_token)
                .await
                .expect("finalize"),
            FinalizeBatchOutcome::Finalized
        );
        assert_eq!(
            database
                .store
                .finalize_batch(batch_id, &replacement_token)
                .await
                .expect("replay finalization"),
            FinalizeBatchOutcome::AlreadyFinalized
        );
        let runs = database.store.queued_runs(10).await.expect("queued runs");
        assert_eq!(runs.len(), 1);
        let child_trace = database
            .store
            .run_trace_context(runs[0].view.id)
            .await
            .expect("child trace context");
        assert_eq!(child_trace.traceparent, batch_trace.traceparent);
        assert_eq!(child_trace.tracestate, batch_trace.tracestate);
        let attempt = database
            .store
            .create_attempt(runs[0].view.id, "node-a", 60)
            .await
            .expect("attempt")
            .expect("offered");
        database
            .store
            .accept_attempt(attempt.id, &attempt.lease_token, 60)
            .await
            .expect("accept");
        database
            .store
            .finish_attempt(
                attempt.id,
                &attempt.lease_token,
                &completion(ExecutionOutcome::Succeeded),
                vec![11],
                "v1",
            )
            .await
            .expect("finish");
        let finished = database
            .store
            .get_batch(batch_id)
            .await
            .expect("batch")
            .expect("batch");
        assert_eq!(finished.view.state, BatchState::CompletedWithErrors);
    }

    #[tokio::test]
    async fn maximum_size_batch_keeps_item_events_durable_and_bounded() {
        let database = TestDatabase::new().await;
        let schedule_id = create_schedule(&database.store).await;
        let batch_id = Uuid::new_v4();
        database
            .store
            .create_batch(new_batch(batch_id, schedule_id, None, 32))
            .await
            .expect("batch");
        let claimed = database
            .store
            .claim_collection_batches("load-test", 60, 1)
            .await
            .expect("claim");
        let token = claimed[0].lease_token.as_deref().expect("token");
        let mut request_cursor = START_CURSOR_DIGEST.to_owned();
        for generation in 0_u64..20 {
            let first_index = u32::try_from(generation * 500).expect("index");
            let items = (first_index..first_index + 500)
                .map(|index| ready_item(index, &format!("load-key-{index}")))
                .collect();
            let next_cursor = format!("load-cursor-{}", generation + 1);
            database
                .store
                .commit_collection_page(page(
                    batch_id,
                    token,
                    generation,
                    &request_cursor,
                    &next_cursor,
                    generation == 19,
                    items,
                ))
                .await
                .expect("commit maximum batch page");
            request_cursor = next_cursor;
        }

        let batch = database
            .store
            .get_batch(batch_id)
            .await
            .expect("batch")
            .expect("batch");
        assert_eq!(batch.view.item_count, 10_000);
        let item_events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM observability_outbox \
             WHERE entity_type='batch_item' AND event_name='batch_item.created'",
        )
        .fetch_one(database.store.pool())
        .await
        .expect("item event count");
        assert_eq!(item_events, 10_000);
        assert_eq!(
            database
                .store
                .claim_observability_events(10_000)
                .await
                .expect("bounded publication claim")
                .len(),
            512,
            "publication must enforce its fixed batch bound"
        );
    }

    #[tokio::test]
    async fn duplicate_keys_roll_back_the_entire_page_and_cursor() {
        let database = TestDatabase::new().await;
        let schedule_id = create_schedule(&database.store).await;
        let batch_id = Uuid::new_v4();
        database
            .store
            .create_batch(new_batch(batch_id, schedule_id, None, 32))
            .await
            .expect("batch");
        let claimed = database
            .store
            .claim_collection_batches("collector", 60, 1)
            .await
            .expect("claim");
        let token = claimed[0].lease_token.as_deref().expect("token");
        database
            .store
            .commit_collection_page(page(
                batch_id,
                token,
                0,
                START_CURSOR_DIGEST,
                "cursor-1",
                false,
                vec![ready_item(0, "same-key")],
            ))
            .await
            .expect("first page");
        assert!(
            database
                .store
                .commit_collection_page(page(
                    batch_id,
                    token,
                    1,
                    "cursor-1",
                    "end",
                    true,
                    vec![ready_item(1, "same-key")],
                ))
                .await
                .is_err()
        );
        let unchanged = database
            .store
            .get_batch(batch_id)
            .await
            .expect("batch")
            .expect("batch");
        assert_eq!(unchanged.cursor_generation, 1);
        assert_eq!(unchanged.view.item_count, 1);
        assert!(!unchanged.ingestion_complete);
    }

    #[tokio::test]
    async fn identical_duplicate_keys_are_quarantined_without_stopping_valid_work() {
        let database = TestDatabase::new().await;
        let schedule_id = create_schedule(&database.store).await;
        let batch_id = Uuid::new_v4();
        database
            .store
            .create_batch(new_batch(batch_id, schedule_id, None, 32))
            .await
            .expect("batch");
        let claimed = database
            .store
            .claim_collection_batches("collector", 60, 1)
            .await
            .expect("claim");
        let token = claimed[0].lease_token.as_deref().expect("token");
        let original = ready_item(0, "duplicate-key");
        let duplicate_digest = original.parameters_digest.clone();
        database
            .store
            .commit_collection_page(page(
                batch_id,
                token,
                0,
                START_CURSOR_DIGEST,
                "cursor-1",
                false,
                vec![original],
            ))
            .await
            .expect("first page");
        let mut duplicate = ready_item(1, "duplicate-key");
        duplicate.parameters_digest = duplicate_digest;
        database
            .store
            .commit_collection_page(page(
                batch_id,
                token,
                1,
                "cursor-1",
                "end",
                true,
                vec![duplicate, ready_item(2, "valid-key")],
            ))
            .await
            .expect("duplicate page");
        let batch = database
            .store
            .get_batch(batch_id)
            .await
            .expect("batch")
            .expect("batch");
        assert_eq!((batch.view.item_count, batch.view.valid_item_count), (3, 1));
        assert_eq!(batch.view.invalid_item_count, 2);
        let items = database
            .store
            .list_batch_items(batch_id, 10)
            .await
            .expect("items");
        assert_eq!(items[0].state, BatchItemState::Invalid);
        assert_eq!(items[1].state, BatchItemState::Invalid);
        assert_eq!(
            items[0].failure_code.as_deref(),
            Some("collection_duplicate_key")
        );
        assert_eq!(items[2].state, BatchItemState::Ready);
    }

    #[tokio::test]
    async fn cron_batch_creation_fences_revision_and_is_idempotent() {
        let database = TestDatabase::new().await;
        let schedule_id = Uuid::new_v4();
        let cron = CronSpec {
            expression: "0 0 6 * * * *".into(),
            timezone: "UTC".into(),
        };
        let mut spec = schedule_spec();
        spec.cron = Some(cron.clone());
        database
            .store
            .create_schedule(NewSchedule {
                id: schedule_id,
                spec,
                encrypted_snapshot: vec![1],
                snapshot_digest: "schedule-digest".into(),
                key_id: "v1".into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("schedule");
        let occurrence = Utc::now();
        let first = new_batch(Uuid::new_v4(), schedule_id, None, 32);
        let first = NewBatch {
            trigger_kind: "cron".into(),
            scheduled_at: occurrence,
            ..first
        };
        let first_id = first.id;
        match database
            .store
            .create_cron_batch(first, 1, &cron)
            .await
            .expect("first occurrence")
        {
            CronBatchOccurrenceResult::Applied(batch) => assert_eq!(batch.id, first_id),
            CronBatchOccurrenceResult::StaleSchedule => panic!("current schedule rejected"),
        }
        let replay = NewBatch {
            id: Uuid::new_v4(),
            trigger_kind: "cron".into(),
            scheduled_at: occurrence,
            ..new_batch(Uuid::new_v4(), schedule_id, None, 32)
        };
        match database
            .store
            .create_cron_batch(replay, 1, &cron)
            .await
            .expect("replay occurrence")
        {
            CronBatchOccurrenceResult::Applied(batch) => assert_eq!(batch.id, first_id),
            CronBatchOccurrenceResult::StaleSchedule => panic!("replay rejected"),
        }
        assert!(matches!(
            database
                .store
                .create_cron_batch(
                    NewBatch {
                        trigger_kind: "cron".into(),
                        scheduled_at: occurrence + Duration::seconds(1),
                        schedule_revision: 2,
                        ..new_batch(Uuid::new_v4(), schedule_id, None, 32)
                    },
                    2,
                    &cron,
                )
                .await
                .expect("stale result"),
            CronBatchOccurrenceResult::StaleSchedule
        ));
    }

    #[tokio::test]
    async fn trigger_identity_prevents_run_and_batch_for_the_same_request() {
        let database = TestDatabase::new().await;
        let schedule_id = create_schedule(&database.store).await;
        let scheduled_at = Utc::now();
        database
            .store
            .create_batch(NewBatch {
                scheduled_at,
                ..new_batch(Uuid::new_v4(), schedule_id, Some("same-request"), 32)
            })
            .await
            .expect("batch");
        let run = NewRun {
            id: Uuid::new_v4(),
            schedule_id,
            trigger_kind: "webhook".into(),
            scheduled_at,
            encrypted_snapshot: vec![1],
            key_id: "v1".into(),
            max_attempts: 3,
            initial_backoff_seconds: 1,
            backoff_cap_seconds: 3,
            idempotency_key: Some("same-request".into()),
        };
        let error = database
            .store
            .create_run(run)
            .await
            .expect_err("batch identity must fence a run");
        assert!(error.to_string().contains("collection batch"));
    }

    #[tokio::test]
    async fn historical_batch_retrigger_preserves_snapshot_after_schedule_edit() {
        let database = TestDatabase::new().await;
        let schedule_id = create_schedule(&database.store).await;
        let source_id = Uuid::new_v4();
        database
            .store
            .create_batch(new_batch(source_id, schedule_id, None, 17))
            .await
            .expect("source batch");
        sqlx::query("UPDATE schedules SET revision=2 WHERE id=?")
            .bind(schedule_id.to_string())
            .execute(database.store.pool())
            .await
            .expect("edit schedule");

        let replay_id = Uuid::new_v4();
        let replay = database
            .store
            .retrigger_batch_snapshot(source_id, replay_id, Utc::now())
            .await
            .expect("historical replay");
        assert_eq!(replay.id, replay_id);
        assert_eq!(replay.schedule_revision, 1);
        let replay_record = database
            .store
            .get_batch(replay_id)
            .await
            .expect("replay lookup")
            .expect("replay batch");
        assert_eq!(replay_record.encrypted_snapshot, vec![2]);
        assert_eq!(replay_record.max_active_runs, 17);
    }

    #[tokio::test]
    async fn queued_selection_respects_batch_cap_and_interleaves_schedules() {
        let database = TestDatabase::new().await;
        let collection_schedule = create_schedule(&database.store).await;
        let ordinary_schedule = create_schedule(&database.store).await;
        let batch_id = Uuid::new_v4();
        database
            .store
            .create_batch(new_batch(batch_id, collection_schedule, None, 1))
            .await
            .expect("batch");
        let claimed = database
            .store
            .claim_collection_batches("collector", 60, 1)
            .await
            .expect("claim");
        let token = claimed[0].lease_token.as_deref().expect("token");
        database
            .store
            .commit_collection_page(page(
                batch_id,
                token,
                0,
                START_CURSOR_DIGEST,
                "end",
                true,
                vec![ready_item(0, "a"), ready_item(1, "b"), ready_item(2, "c")],
            ))
            .await
            .expect("page");
        database
            .store
            .finalize_batch(batch_id, token)
            .await
            .expect("finalize");
        database
            .store
            .create_run(NewRun {
                id: Uuid::new_v4(),
                schedule_id: ordinary_schedule,
                trigger_kind: "manual".into(),
                scheduled_at: Utc::now(),
                encrypted_snapshot: vec![1],
                key_id: "v1".into(),
                max_attempts: 3,
                initial_backoff_seconds: 1,
                backoff_cap_seconds: 3,
                idempotency_key: None,
            })
            .await
            .expect("ordinary run");
        let eligible = database.store.queued_runs(10).await.expect("eligible");
        assert_eq!(eligible.len(), 2, "one capped batch run plus ordinary work");
        assert_ne!(eligible[0].view.schedule_id, eligible[1].view.schedule_id);
    }

    #[tokio::test]
    async fn batch_cancellation_rolls_up_every_cancelled_child_exactly_once() {
        let database = TestDatabase::new().await;
        let schedule_id = create_schedule(&database.store).await;
        let batch_id = Uuid::new_v4();
        database
            .store
            .create_batch(new_batch(batch_id, schedule_id, None, 32))
            .await
            .expect("batch");
        let claimed = database
            .store
            .claim_collection_batches("collector", 60, 1)
            .await
            .expect("claim");
        let token = claimed[0].lease_token.as_deref().expect("token");
        database
            .store
            .commit_collection_page(page(
                batch_id,
                token,
                0,
                START_CURSOR_DIGEST,
                "end",
                true,
                vec![ready_item(0, "a"), ready_item(1, "b"), ready_item(2, "c")],
            ))
            .await
            .expect("page");
        database
            .store
            .finalize_batch(batch_id, token)
            .await
            .expect("finalize");

        database
            .store
            .cancel_batch(batch_id)
            .await
            .expect("cancel batch");
        assert!(database.store.cancel_batch(batch_id).await.is_err());

        let cancelled: i64 = sqlx::query_scalar("SELECT cancelled FROM daily_schedule_stats")
            .fetch_one(database.store.pool())
            .await
            .expect("cancelled rollup");
        assert_eq!(cancelled, 3);
    }

    #[tokio::test]
    async fn batch_state_counts_include_zero_states_and_exact_live_counts() {
        let database = TestDatabase::new().await;
        let empty = database
            .store
            .batch_state_counts()
            .await
            .expect("empty state counts");
        assert_eq!(empty.len(), 7);
        assert!(empty.iter().all(|(_, count)| *count == 0));

        let schedule_id = create_schedule(&database.store).await;
        database
            .store
            .create_batch(new_batch(Uuid::new_v4(), schedule_id, None, 32))
            .await
            .expect("batch");
        let counts = database
            .store
            .batch_state_counts()
            .await
            .expect("state counts");
        assert_eq!(
            counts
                .iter()
                .find(|(state, _)| *state == BatchState::Scheduled)
                .map(|(_, count)| *count),
            Some(1)
        );
        assert_eq!(counts.iter().map(|(_, count)| count).sum::<u64>(), 1);
    }
}
