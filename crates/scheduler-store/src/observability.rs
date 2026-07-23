use anyhow::{Context, Result};
use chrono::{DateTime, Datelike as _, Duration, Utc};
use chrono_tz::Tz;
use scheduler_core::{
    DailyObservabilitySnapshot, DailyScheduleStatus, DailyVerdict, DailyWindow, ScheduleSpec,
};
use sqlx::Row;
use uuid::Uuid;

use crate::{Store, parse_time};

#[derive(Debug, Clone)]
pub struct ProcessingStateCount {
    pub schedule_id: String,
    pub entity: String,
    pub state: String,
    pub count: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ObservabilityOutboxStatus {
    pub pending_events: u64,
    pub oldest_pending_at: Option<DateTime<Utc>>,
    pub delivered_events: u64,
    pub expired_events: u64,
    pub coverage_gap: bool,
    pub gap_reason: Option<String>,
    pub last_snapshot_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct PendingObservabilityEvent {
    pub event_id: String,
    pub sequence: i64,
    pub entity_type: String,
    pub entity_id: String,
    pub event_name: String,
    pub attributes_json: String,
    pub occurred_at: DateTime<Utc>,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

#[derive(Debug, Default)]
struct DayCounts {
    materialized: u64,
    succeeded: u64,
    failed: u64,
    cancelled: u64,
    pending: u64,
    overdue: u64,
    retries: u64,
    attempt_anomalies: u64,
    ready_items: u64,
    queued_items: u64,
    running_items: u64,
    succeeded_items: u64,
    failed_items: u64,
    cancelled_items: u64,
    invalid_items: u64,
    suspected_poison_items: u64,
    poisoned_items: u64,
    held_items: u64,
    coverage_complete: bool,
    captured_deadline_seconds: Option<u64>,
}

impl DayCounts {
    fn merge(&mut self, other: Self) {
        self.materialized = self.materialized.saturating_add(other.materialized);
        self.succeeded = self.succeeded.saturating_add(other.succeeded);
        self.failed = self.failed.saturating_add(other.failed);
        self.cancelled = self.cancelled.saturating_add(other.cancelled);
        self.pending = self.pending.saturating_add(other.pending);
        self.overdue = self.overdue.saturating_add(other.overdue);
        self.retries = self.retries.saturating_add(other.retries);
        self.attempt_anomalies = self
            .attempt_anomalies
            .saturating_add(other.attempt_anomalies);
        self.ready_items = self.ready_items.saturating_add(other.ready_items);
        self.queued_items = self.queued_items.saturating_add(other.queued_items);
        self.running_items = self.running_items.saturating_add(other.running_items);
        self.succeeded_items = self.succeeded_items.saturating_add(other.succeeded_items);
        self.failed_items = self.failed_items.saturating_add(other.failed_items);
        self.cancelled_items = self.cancelled_items.saturating_add(other.cancelled_items);
        self.invalid_items = self.invalid_items.saturating_add(other.invalid_items);
        self.suspected_poison_items = self
            .suspected_poison_items
            .saturating_add(other.suspected_poison_items);
        self.poisoned_items = self.poisoned_items.saturating_add(other.poisoned_items);
        self.held_items = self.held_items.saturating_add(other.held_items);
        self.coverage_complete &= other.coverage_complete;
        self.captured_deadline_seconds = match (
            self.captured_deadline_seconds,
            other.captured_deadline_seconds,
        ) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (value @ Some(_), None) | (None, value @ Some(_)) => value,
            (None, None) => None,
        };
    }
}

impl Store {
    pub async fn processing_state_counts(&self) -> Result<Vec<ProcessingStateCount>> {
        let rows = sqlx::query(
            "SELECT schedule_id,'run' AS entity,state,COUNT(*) AS count FROM runs \
             GROUP BY schedule_id,state \
             UNION ALL \
             SELECT schedule_id,'batch' AS entity,state,COUNT(*) AS count FROM batches \
             GROUP BY schedule_id,state \
             UNION ALL \
             SELECT b.schedule_id,'item' AS entity,bi.state,COUNT(*) AS count \
             FROM batch_items bi JOIN batches b ON b.id=bi.batch_id \
             GROUP BY b.schedule_id,bi.state \
             UNION ALL \
             SELECT r.schedule_id,'attempt' AS entity,a.state,COUNT(*) AS count \
             FROM attempts a JOIN runs r ON r.id=a.run_id \
             GROUP BY r.schedule_id,a.state",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(ProcessingStateCount {
                    schedule_id: row.try_get("schedule_id")?,
                    entity: row.try_get("entity")?,
                    state: row.try_get("state")?,
                    count: count(&row, "count")?,
                })
            })
            .collect()
    }

    pub async fn observability_outbox_status(&self) -> Result<ObservabilityOutboxStatus> {
        let row = sqlx::query(
            "SELECT \
             SUM(CASE WHEN delivered_at IS NULL THEN 1 ELSE 0 END) AS pending_events,\
             MIN(CASE WHEN delivered_at IS NULL THEN occurred_at END) AS oldest_pending_at,\
             SUM(CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END) AS delivered_events \
             FROM observability_outbox",
        )
        .fetch_one(&self.pool)
        .await?;
        let coverage = sqlx::query(
            "SELECT gap_detected,gap_reason,expired_events,last_snapshot_at \
             FROM observability_coverage WHERE id=1",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(ObservabilityOutboxStatus {
            pending_events: u64::try_from(row.get::<Option<i64>, _>("pending_events").unwrap_or(0))
                .unwrap_or_default(),
            oldest_pending_at: row
                .get::<Option<String>, _>("oldest_pending_at")
                .map(parse_time)
                .transpose()?,
            delivered_events: u64::try_from(
                row.get::<Option<i64>, _>("delivered_events").unwrap_or(0),
            )
            .unwrap_or_default(),
            expired_events: u64::try_from(coverage.get::<i64, _>("expired_events"))
                .unwrap_or_default(),
            coverage_gap: coverage.get("gap_detected"),
            gap_reason: coverage.get("gap_reason"),
            last_snapshot_at: coverage
                .get::<Option<String>, _>("last_snapshot_at")
                .map(parse_time)
                .transpose()?,
        })
    }

    pub async fn claim_observability_events(
        &self,
        limit: u32,
    ) -> Result<Vec<PendingObservabilityEvent>> {
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(
            "SELECT event_id,audit_event_id,entity_type,entity_id,event_name,\
             attributes_json,occurred_at,traceparent,tracestate,attempts \
             FROM observability_outbox \
             WHERE delivered_at IS NULL AND next_attempt_at<=? \
             ORDER BY occurred_at,event_id LIMIT ?",
        )
        .bind(now.to_rfc3339())
        .bind(i64::from(limit.clamp(1, 512)))
        .fetch_all(&mut *tx)
        .await?;
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let attempts = row.get::<i64, _>("attempts").max(0) as u32;
            let delay_seconds = 1_i64 << attempts.min(8);
            let next_attempt = now + Duration::seconds(delay_seconds.min(300));
            let event_id: String = row.get("event_id");
            let updated = sqlx::query(
                "UPDATE observability_outbox SET attempts=attempts+1,next_attempt_at=?,\
                 last_error_class='awaiting_export_ack' \
                 WHERE event_id=? AND delivered_at IS NULL AND next_attempt_at<=?",
            )
            .bind(next_attempt.to_rfc3339())
            .bind(&event_id)
            .bind(now.to_rfc3339())
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() == 1 {
                events.push(PendingObservabilityEvent {
                    event_id,
                    sequence: row.get("audit_event_id"),
                    entity_type: row.get("entity_type"),
                    entity_id: row.get("entity_id"),
                    event_name: row.get("event_name"),
                    attributes_json: row.get("attributes_json"),
                    occurred_at: parse_time(row.get("occurred_at"))?,
                    traceparent: row.get("traceparent"),
                    tracestate: row.get("tracestate"),
                });
            }
        }
        tx.commit().await?;
        Ok(events)
    }

    pub async fn acknowledge_observability_event(&self, event_id: &str) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE observability_outbox SET delivered_at=?,last_error_class=NULL \
             WHERE event_id=? AND delivered_at IS NULL",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(event_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn expire_observability_events(&self, retention_days: u32) -> Result<u64> {
        let cutoff = Utc::now() - Duration::days(i64::from(retention_days.max(1)));
        let cutoff = cutoff.to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let expired: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM observability_outbox \
             WHERE delivered_at IS NULL AND occurred_at<?",
        )
        .bind(&cutoff)
        .fetch_one(&mut *tx)
        .await?;
        if expired > 0 {
            sqlx::query(
                "UPDATE observability_coverage SET gap_detected=1,\
                 gap_reason='undelivered_state_events_expired',\
                 expired_events=expired_events+? WHERE id=1",
            )
            .bind(expired)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query("DELETE FROM observability_outbox WHERE occurred_at<?")
            .bind(&cutoff)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(u64::try_from(expired).unwrap_or_default())
    }

    /// Rebuild an operations-day view from authoritative durable state.
    ///
    /// These values are intentionally gauges rather than accumulated counters:
    /// after a restart or telemetry outage, the next snapshot converges to the
    /// same answer as a direct database inspection.
    pub async fn daily_observability_snapshot(
        &self,
        now: DateTime<Utc>,
        window: DailyWindow,
    ) -> Result<DailyObservabilitySnapshot> {
        let settings = self.get_global_settings().await?;
        let coverage_gap: bool =
            sqlx::query_scalar("SELECT gap_detected FROM observability_coverage WHERE id=1")
                .fetch_one(&self.pool)
                .await?;
        let schedule_rows = sqlx::query(
            "SELECT id,name,spec_json,created_at,last_cron_at FROM schedules ORDER BY name,id",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut schedules = Vec::with_capacity(schedule_rows.len());
        for row in schedule_rows {
            let schedule_id = Uuid::parse_str(row.get::<String, _>("id").as_str())?;
            let schedule_name: String = row.get("name");
            let spec: ScheduleSpec = serde_json::from_str(row.get("spec_json"))?;
            let timezone_name = spec
                .cron
                .as_ref()
                .map(|cron| cron.timezone.as_str())
                .unwrap_or(&settings.default_timezone);
            let timezone = timezone_name
                .parse::<Tz>()
                .map_err(|_| anyhow::anyhow!("schedule observability timezone is invalid"))?;
            let local_now = now.with_timezone(&timezone);
            let operations_day = match window {
                DailyWindow::Current => local_now.date_naive(),
                DailyWindow::Previous => local_now
                    .date_naive()
                    .pred_opt()
                    .context("previous operations day underflow")?,
            };
            let day = operations_day.to_string();
            let trigger_days = self
                .trigger_days_for_window(schedule_id, now, window, timezone_name)
                .await?;
            let counts = self
                .daily_counts(schedule_id, &trigger_days, now)
                .await
                .with_context(|| format!("cannot evaluate schedule {schedule_id} for {day}"))?;
            let last_success_at = self.schedule_last_success_at(schedule_id).await?;
            let created_at = parse_time(row.get("created_at"))?;
            let last_cron_at = row
                .get::<Option<String>, _>("last_cron_at")
                .map(parse_time)
                .transpose()?;
            let deadline_seconds = spec
                .observability
                .completion_deadline_seconds
                .unwrap_or(settings.default_completion_deadline_seconds);
            let (missing_due, missing_overdue, backlog_truncated) = missing_cron_occurrences(
                &spec,
                created_at,
                last_cron_at,
                now,
                operations_day.year(),
                operations_day.ordinal(),
                deadline_seconds,
            )?;
            let expected = counts.materialized.saturating_add(missing_due);
            let coverage_complete = !coverage_gap && counts.coverage_complete && !backlog_truncated;
            let (verdict, reasons) = classify(
                &counts,
                missing_due,
                missing_overdue,
                expected,
                coverage_complete,
            );
            schedules.push(DailyScheduleStatus {
                schedule_id,
                schedule_name,
                operations_day: day,
                operations_timezone: timezone_name.to_owned(),
                completion_deadline_seconds: counts
                    .captured_deadline_seconds
                    .unwrap_or(deadline_seconds),
                expected_triggers: expected,
                materialized_triggers: counts.materialized,
                succeeded: counts.succeeded,
                failed: counts.failed,
                cancelled: counts.cancelled,
                pending: counts.pending,
                overdue: counts.overdue.saturating_add(missing_overdue),
                missing_due,
                retries: counts.retries,
                attempt_anomalies: counts.attempt_anomalies,
                ready_items: counts.ready_items,
                queued_items: counts.queued_items,
                running_items: counts.running_items,
                succeeded_items: counts.succeeded_items,
                failed_items: counts.failed_items,
                cancelled_items: counts.cancelled_items,
                invalid_items: counts.invalid_items,
                suspected_poison_items: counts.suspected_poison_items,
                poisoned_items: counts.poisoned_items,
                held_items: counts.held_items,
                coverage_complete,
                verdict,
                reasons,
                last_success_at,
            });
        }
        let cluster_verdict = schedules
            .iter()
            .map(|status| status.verdict)
            .max_by_key(|verdict| verdict.severity())
            .unwrap_or(DailyVerdict::Idle);
        sqlx::query("UPDATE observability_coverage SET last_snapshot_at=? WHERE id=1")
            .bind(now.to_rfc3339())
            .execute(&self.pool)
            .await?;
        Ok(DailyObservabilitySnapshot {
            generated_at: now,
            window,
            cluster_verdict,
            coverage_gap,
            schedules,
        })
    }

    async fn daily_counts(
        &self,
        schedule_id: Uuid,
        trigger_days: &[(String, String)],
        now: DateTime<Utc>,
    ) -> Result<DayCounts> {
        let mut total = DayCounts {
            coverage_complete: true,
            ..DayCounts::default()
        };
        for (timezone, operations_day) in trigger_days {
            total.merge(
                self.daily_counts_for_day(schedule_id, timezone, operations_day, now)
                    .await?,
            );
        }
        Ok(total)
    }

    async fn daily_counts_for_day(
        &self,
        schedule_id: Uuid,
        operations_timezone: &str,
        operations_day: &str,
        now: DateTime<Utc>,
    ) -> Result<DayCounts> {
        let row = sqlx::query(
            "SELECT \
             COUNT(*) AS materialized,\
             COALESCE(SUM(CASE WHEN \
               (t.target_kind='run' AND r.state='succeeded') OR \
               (t.target_kind='batch' AND b.state='succeeded') THEN 1 ELSE 0 END),0) AS succeeded,\
             COALESCE(SUM(CASE WHEN \
               (t.target_kind='run' AND r.state='failed') OR \
               (t.target_kind='batch' AND b.state IN ('failed','completed_with_errors')) \
               THEN 1 ELSE 0 END),0) AS failed,\
             COALESCE(SUM(CASE WHEN \
               (t.target_kind='run' AND r.state='cancelled') OR \
               (t.target_kind='batch' AND b.state='cancelled') THEN 1 ELSE 0 END),0) AS cancelled,\
             COALESCE(SUM(CASE WHEN \
               (t.target_kind='run' AND r.state IN ('queued','running')) OR \
               (t.target_kind='batch' AND b.state IN ('scheduled','collecting','running')) \
               THEN 1 ELSE 0 END),0) AS pending,\
             COALESCE(SUM(CASE WHEN \
               (t.target_kind='run' AND (\
                 (r.state='succeeded' AND r.updated_at>t.completion_deadline_at) OR \
                 (r.state<>'succeeded' AND t.completion_deadline_at<?))) OR \
               (t.target_kind='batch' AND (\
                 (b.state='succeeded' AND b.updated_at>t.completion_deadline_at) OR \
                 (b.state<>'succeeded' AND t.completion_deadline_at<?))) \
               THEN 1 ELSE 0 END),0) AS overdue,\
             COALESCE(SUM(CASE WHEN t.target_kind='run' THEN MAX(r.attempt_count-1,0) ELSE \
               (SELECT COALESCE(SUM(MAX(br.attempt_count-1,0)),0) FROM runs br \
                WHERE br.batch_id=t.target_id) END),0) AS retries,\
             COALESCE(SUM((SELECT COUNT(*) FROM attempts a JOIN runs ar ON ar.id=a.run_id \
               WHERE ((t.target_kind='run' AND ar.id=t.target_id) OR \
                      (t.target_kind='batch' AND ar.batch_id=t.target_id)) \
                 AND ((a.outcome IS NOT NULL AND a.outcome<>'succeeded') OR \
                      a.state IN ('offer_failed','late_result','expiring')))),0) + \
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN \
               (SELECT COUNT(*) FROM audit_events ae WHERE ae.entity_type='batch' \
                AND ae.entity_id=t.target_id \
                AND ae.event_type='batch.collection_lease_recovered') ELSE 0 END),0) \
             AS attempt_anomalies,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN \
               (SELECT COUNT(*) FROM batch_items bi WHERE bi.batch_id=t.target_id \
                AND bi.state='ready') ELSE 0 END),0) AS ready_items,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN \
               (SELECT COUNT(*) FROM batch_items bi WHERE bi.batch_id=t.target_id \
                AND bi.state='queued') ELSE 0 END),0) AS queued_items,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN \
               (SELECT COUNT(*) FROM batch_items bi WHERE bi.batch_id=t.target_id \
                AND bi.state='running') ELSE 0 END),0) AS running_items,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN \
               (SELECT COUNT(*) FROM batch_items bi WHERE bi.batch_id=t.target_id \
                AND bi.state='succeeded') ELSE 0 END),0) AS succeeded_items,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN \
               (SELECT COUNT(*) FROM batch_items bi WHERE bi.batch_id=t.target_id \
                AND bi.state='failed') ELSE 0 END),0) AS failed_items,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN \
               (SELECT COUNT(*) FROM batch_items bi WHERE bi.batch_id=t.target_id \
                AND bi.state='cancelled') ELSE 0 END),0) AS cancelled_items,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN b.invalid_item_count ELSE 0 END),0) AS invalid_items,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN \
               (SELECT COUNT(*) FROM batch_items bi WHERE bi.batch_id=t.target_id \
                AND bi.state='suspected_poison') ELSE 0 END),0) AS suspected_poison_items,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN b.poisoned_item_count ELSE 0 END),0) AS poisoned_items,\
             COALESCE(SUM(CASE WHEN t.target_kind='batch' THEN b.held_item_count ELSE 0 END),0) AS held_items,\
             COALESCE(MIN(t.observability_coverage_complete),1) AS coverage_complete,\
             MIN(CAST(strftime('%s',t.completion_deadline_at) AS INTEGER) - \
                 CAST(strftime('%s',t.scheduled_at) AS INTEGER)) AS deadline_seconds \
             FROM trigger_identities t \
             LEFT JOIN runs r ON t.target_kind='run' AND r.id=t.target_id \
             LEFT JOIN batches b ON t.target_kind='batch' AND b.id=t.target_id \
             WHERE t.schedule_id=? AND t.operations_timezone=? AND t.operations_day=?",
        )
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(schedule_id.to_string())
        .bind(operations_timezone)
        .bind(operations_day)
        .fetch_one(&self.pool)
        .await?;
        Ok(DayCounts {
            materialized: count(&row, "materialized")?,
            succeeded: count(&row, "succeeded")?,
            failed: count(&row, "failed")?,
            cancelled: count(&row, "cancelled")?,
            pending: count(&row, "pending")?,
            overdue: count(&row, "overdue")?,
            retries: count(&row, "retries")?,
            attempt_anomalies: count(&row, "attempt_anomalies")?,
            ready_items: count(&row, "ready_items")?,
            queued_items: count(&row, "queued_items")?,
            running_items: count(&row, "running_items")?,
            succeeded_items: count(&row, "succeeded_items")?,
            failed_items: count(&row, "failed_items")?,
            cancelled_items: count(&row, "cancelled_items")?,
            invalid_items: count(&row, "invalid_items")?,
            suspected_poison_items: count(&row, "suspected_poison_items")?,
            poisoned_items: count(&row, "poisoned_items")?,
            held_items: count(&row, "held_items")?,
            coverage_complete: row.get::<i64, _>("coverage_complete") != 0,
            captured_deadline_seconds: row
                .get::<Option<i64>, _>("deadline_seconds")
                .and_then(|value| u64::try_from(value).ok()),
        })
    }

    async fn trigger_days_for_window(
        &self,
        schedule_id: Uuid,
        now: DateTime<Utc>,
        window: DailyWindow,
        current_timezone: &str,
    ) -> Result<Vec<(String, String)>> {
        let mut timezones = sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT operations_timezone FROM trigger_identities \
             WHERE schedule_id=? AND operations_timezone IS NOT NULL",
        )
        .bind(schedule_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        if !timezones
            .iter()
            .any(|timezone| timezone == current_timezone)
        {
            timezones.push(current_timezone.to_owned());
        }
        let mut days = Vec::with_capacity(timezones.len());
        for timezone_name in timezones {
            let timezone = timezone_name
                .parse::<Tz>()
                .map_err(|_| anyhow::anyhow!("captured observability timezone is invalid"))?;
            let local_now = now.with_timezone(&timezone);
            let day = match window {
                DailyWindow::Current => local_now.date_naive(),
                DailyWindow::Previous => local_now
                    .date_naive()
                    .pred_opt()
                    .context("previous operations day underflow")?,
            };
            days.push((timezone_name, day.to_string()));
        }
        Ok(days)
    }

    async fn schedule_last_success_at(&self, schedule_id: Uuid) -> Result<Option<DateTime<Utc>>> {
        let value: Option<String> = sqlx::query_scalar(
            "SELECT MAX(CASE WHEN \
               (t.target_kind='run' AND r.state='succeeded') OR \
               (t.target_kind='batch' AND b.state='succeeded') \
               THEN COALESCE(r.updated_at,b.updated_at) END) \
             FROM trigger_identities t \
             LEFT JOIN runs r ON t.target_kind='run' AND r.id=t.target_id \
             LEFT JOIN batches b ON t.target_kind='batch' AND b.id=t.target_id \
             WHERE t.schedule_id=?",
        )
        .bind(schedule_id.to_string())
        .fetch_one(&self.pool)
        .await?;
        value.map(parse_time).transpose()
    }
}

fn count(row: &sqlx::sqlite::SqliteRow, column: &str) -> Result<u64> {
    Ok(u64::try_from(row.try_get::<i64, _>(column)?).unwrap_or_default())
}

#[allow(clippy::too_many_arguments)]
fn missing_cron_occurrences(
    spec: &ScheduleSpec,
    created_at: DateTime<Utc>,
    last_cron_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    operations_year: i32,
    operations_ordinal: u32,
    deadline_seconds: u64,
) -> Result<(u64, u64, bool)> {
    let Some(cron) = spec.cron.as_ref().filter(|_| spec.enabled) else {
        return Ok((0, 0, false));
    };
    let timezone = cron
        .timezone
        .parse::<Tz>()
        .map_err(|_| anyhow::anyhow!("schedule cron timezone is invalid"))?;
    let occurrences = scheduler_core::schedule::next_occurrences(
        cron,
        last_cron_at.unwrap_or(created_at),
        1_001,
    )?;
    let mut missing = 0_u64;
    let mut overdue = 0_u64;
    let mut due_count = 0_usize;
    for occurrence in occurrences {
        if occurrence > now {
            break;
        }
        due_count += 1;
        let local = occurrence.with_timezone(&timezone);
        if local.year() == operations_year && local.ordinal() == operations_ordinal {
            missing += 1;
            if occurrence + Duration::seconds(i64::try_from(deadline_seconds).unwrap_or(i64::MAX))
                < now
            {
                overdue += 1;
            }
        }
    }
    Ok((missing, overdue, due_count == 1_001))
}

fn classify(
    counts: &DayCounts,
    missing_due: u64,
    _missing_overdue: u64,
    expected: u64,
    coverage_complete: bool,
) -> (DailyVerdict, Vec<String>) {
    let mut reasons = Vec::new();
    if !coverage_complete {
        reasons.push("authoritative event coverage is incomplete".into());
        return (DailyVerdict::Unknown, reasons);
    }
    if missing_due > 0 {
        reasons.push(format!(
            "{missing_due} due cron triggers were not materialized"
        ));
    }
    if counts.overdue > 0 {
        reasons.push(format!(
            "{} materialized triggers are overdue",
            counts.overdue
        ));
    }
    if counts.failed > 0 {
        reasons.push(format!("{} triggers failed", counts.failed));
    }
    if counts.cancelled > 0 {
        reasons.push(format!("{} triggers were cancelled", counts.cancelled));
    }
    let bad_items = counts
        .invalid_items
        .saturating_add(counts.suspected_poison_items)
        .saturating_add(counts.poisoned_items)
        .saturating_add(counts.held_items);
    if bad_items > 0 {
        reasons.push(format!("{bad_items} collection items require attention"));
    }
    if !reasons.is_empty() {
        return (DailyVerdict::Red, reasons);
    }
    if counts.pending > 0 {
        reasons.push(format!(
            "{} triggers are still within their deadline",
            counts.pending
        ));
        return (DailyVerdict::Pending, reasons);
    }
    if counts.attempt_anomalies > 0 || counts.retries > 0 {
        reasons.push(format!(
            "{} retries and {} attempt anomalies recovered",
            counts.retries, counts.attempt_anomalies
        ));
        return (DailyVerdict::Degraded, reasons);
    }
    if expected == 0 {
        reasons.push("no triggers were expected or observed".into());
        return (DailyVerdict::Idle, reasons);
    }
    (DailyVerdict::Green, reasons)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use scheduler_core::{ArtifactRef, CronSpec, ScheduleObservabilityPolicy};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn strict_verdict_precedence_is_stable() {
        let mut counts = DayCounts {
            coverage_complete: true,
            materialized: 1,
            succeeded: 1,
            ..DayCounts::default()
        };
        assert_eq!(classify(&counts, 0, 0, 1, true).0, DailyVerdict::Green);
        counts.retries = 1;
        assert_eq!(classify(&counts, 0, 0, 1, true).0, DailyVerdict::Degraded);
        counts.failed = 1;
        assert_eq!(classify(&counts, 0, 0, 1, true).0, DailyVerdict::Red);
        assert_eq!(classify(&counts, 0, 0, 1, false).0, DailyVerdict::Unknown);
    }

    #[test]
    fn no_work_is_idle_and_unfinished_work_is_pending() {
        let counts = DayCounts {
            coverage_complete: true,
            ..DayCounts::default()
        };
        assert_eq!(classify(&counts, 0, 0, 0, true).0, DailyVerdict::Idle);
        assert_eq!(classify(&counts, 1, 0, 1, true).0, DailyVerdict::Red);
        let pending = DayCounts {
            coverage_complete: true,
            materialized: 1,
            pending: 1,
            retries: 1,
            ..DayCounts::default()
        };
        assert_eq!(classify(&pending, 0, 0, 1, true).0, DailyVerdict::Pending);
    }

    #[tokio::test]
    async fn trigger_metadata_and_daily_state_are_authoritative() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = directory.path().join("observability.db");
        let store = Store::connect(&format!("sqlite://{}", database.display()), None)
            .await
            .expect("store");
        let schedule_id = Uuid::new_v4();
        store
            .create_schedule(crate::NewSchedule {
                id: schedule_id,
                spec: ScheduleSpec {
                    name: "daily report".into(),
                    blueprint_ref: ArtifactRef {
                        uri: "file:///blueprint".into(),
                    },
                    parameters_ref: ArtifactRef {
                        uri: "file:///parameters".into(),
                    },
                    parameter_collection: None,
                    observability: ScheduleObservabilityPolicy {
                        completion_deadline_seconds: Some(1),
                    },
                    required_labels: BTreeMap::new(),
                    cron: None,
                    webhook_enabled: false,
                    enabled: true,
                },
                encrypted_snapshot: vec![1],
                snapshot_digest: "snapshot".into(),
                key_id: "test".into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("schedule");
        let scheduled_at = Utc::now();
        store
            .create_run(crate::NewRun {
                id: Uuid::new_v4(),
                schedule_id,
                trigger_kind: "manual".into(),
                scheduled_at,
                encrypted_snapshot: vec![2],
                key_id: "test".into(),
                max_attempts: 1,
                initial_backoff_seconds: 1,
                backoff_cap_seconds: 1,
                idempotency_key: None,
            })
            .await
            .expect("run");
        let captured = sqlx::query(
            "SELECT operations_timezone,operations_day,completion_deadline_at,\
             observability_coverage_complete FROM trigger_identities WHERE schedule_id=?",
        )
        .bind(schedule_id.to_string())
        .fetch_one(store.pool())
        .await
        .expect("trigger metadata");
        assert_eq!(captured.get::<String, _>("operations_timezone"), "UTC");
        assert_eq!(
            captured.get::<String, _>("operations_day"),
            scheduled_at.date_naive().to_string()
        );
        assert!(captured.get::<bool, _>("observability_coverage_complete"));

        let pending = store
            .daily_observability_snapshot(scheduled_at, DailyWindow::Current)
            .await
            .expect("pending snapshot");
        assert_eq!(pending.schedules[0].verdict, DailyVerdict::Pending);
        assert_eq!(pending.schedules[0].completion_deadline_seconds, 1);
        let projected = store
            .processing_state_counts()
            .await
            .expect("current state projection");
        assert!(projected.iter().any(|count| {
            count.schedule_id == schedule_id.to_string()
                && count.entity == "run"
                && count.state == "queued"
                && count.count == 1
        }));
        let overdue = store
            .daily_observability_snapshot(scheduled_at + Duration::seconds(2), DailyWindow::Current)
            .await
            .expect("overdue snapshot");
        assert_eq!(overdue.schedules[0].verdict, DailyVerdict::Red);
        assert_eq!(overdue.schedules[0].overdue, 1);

        sqlx::query("UPDATE runs SET state='succeeded',updated_at=? WHERE schedule_id=?")
            .bind((scheduled_at + Duration::seconds(2)).to_rfc3339())
            .bind(schedule_id.to_string())
            .execute(store.pool())
            .await
            .expect("late success");
        let late_success = store
            .daily_observability_snapshot(scheduled_at + Duration::seconds(3), DailyWindow::Current)
            .await
            .expect("late-success snapshot");
        assert_eq!(late_success.schedules[0].verdict, DailyVerdict::Red);
        assert_eq!(late_success.schedules[0].overdue, 1);

        let outbox = store
            .observability_outbox_status()
            .await
            .expect("outbox status");
        assert!(outbox.pending_events >= 2);

        let audit_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_events")
            .fetch_one(store.pool())
            .await
            .expect("audit count");
        let linked_outbox_count: i64 =
            sqlx::query_scalar("SELECT COUNT(DISTINCT audit_event_id) FROM observability_outbox")
                .fetch_one(store.pool())
                .await
                .expect("linked outbox count");
        assert_eq!(linked_outbox_count, audit_count);

        let first = store
            .claim_observability_events(1)
            .await
            .expect("first delivery claim")
            .pop()
            .expect("state event");
        sqlx::query("UPDATE observability_outbox SET next_attempt_at=? WHERE event_id=?")
            .bind((Utc::now() - Duration::seconds(1)).to_rfc3339())
            .bind(&first.event_id)
            .execute(store.pool())
            .await
            .expect("make delivery retry eligible");
        let replay = store
            .claim_observability_events(1)
            .await
            .expect("replay delivery claim")
            .pop()
            .expect("replayed state event");
        assert_eq!(replay.event_id, first.event_id);
        assert_eq!(replay.sequence, first.sequence);
        assert!(
            store
                .acknowledge_observability_event(&replay.event_id)
                .await
                .expect("acknowledge state event")
        );

        store
            .append_audit(
                "test",
                "safe-entity",
                "test.redaction",
                serde_json::json!({
                    "run_id": "safe-run-id",
                    "failure_code": "safe_failure_code",
                    "parameters": {"password": "PARAMETER_SECRET"},
                    "provider_key": "PROVIDER_SECRET",
                    "cursor": "CURSOR_SECRET",
                    "diagnostic": {"summary": "FREE_FORM_DIAGNOSTIC"},
                    "error": "FREE_FORM_ERROR",
                    "secret_name": "SECRET_NAME",
                    "secret_value": "SECRET_VALUE"
                }),
            )
            .await
            .expect("redaction audit");
        let exported: String = sqlx::query_scalar(
            "SELECT attributes_json FROM observability_outbox \
             WHERE event_name='test.redaction'",
        )
        .fetch_one(store.pool())
        .await
        .expect("redacted state event");
        assert!(exported.contains("safe-run-id"));
        assert!(exported.contains("safe_failure_code"));
        for forbidden in [
            "PARAMETER_SECRET",
            "PROVIDER_SECRET",
            "CURSOR_SECRET",
            "FREE_FORM_DIAGNOSTIC",
            "FREE_FORM_ERROR",
            "SECRET_NAME",
            "SECRET_VALUE",
        ] {
            assert!(!exported.contains(forbidden), "{forbidden} leaked");
        }

        sqlx::query("UPDATE observability_outbox SET occurred_at=? WHERE delivered_at IS NULL")
            .bind((Utc::now() - Duration::days(100)).to_rfc3339())
            .execute(store.pool())
            .await
            .expect("age undelivered events");
        assert!(
            store
                .expire_observability_events(90)
                .await
                .expect("retention")
                > 0
        );
        let coverage = store.observability_outbox_status().await.expect("coverage");
        assert!(coverage.coverage_gap);
        assert!(coverage.expired_events > 0);
        let unknown = store
            .daily_observability_snapshot(scheduled_at, DailyWindow::Current)
            .await
            .expect("unknown snapshot");
        assert_eq!(unknown.schedules[0].verdict, DailyVerdict::Unknown);
    }

    #[tokio::test]
    async fn operations_days_and_deadlines_are_stable_across_dst_transitions() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = directory.path().join("observability-dst.db");
        let store = Store::connect(&format!("sqlite://{}", database.display()), None)
            .await
            .expect("store");
        let schedule_id = Uuid::new_v4();
        store
            .create_schedule(crate::NewSchedule {
                id: schedule_id,
                spec: ScheduleSpec {
                    name: "Vienna DST".into(),
                    blueprint_ref: ArtifactRef {
                        uri: "file:///blueprint".into(),
                    },
                    parameters_ref: ArtifactRef {
                        uri: "file:///parameters".into(),
                    },
                    parameter_collection: None,
                    observability: ScheduleObservabilityPolicy {
                        completion_deadline_seconds: Some(86_400),
                    },
                    required_labels: BTreeMap::new(),
                    cron: Some(CronSpec {
                        expression: "0 30 2 * * * *".into(),
                        timezone: "Europe/Vienna".into(),
                    }),
                    webhook_enabled: false,
                    enabled: true,
                },
                encrypted_snapshot: vec![1],
                snapshot_digest: "snapshot".into(),
                key_id: "test".into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("schedule");

        for scheduled_at in [
            "2026-03-29T00:30:00Z",
            "2026-03-29T01:30:00Z",
            "2026-10-25T00:30:00Z",
            "2026-10-25T01:30:00Z",
        ] {
            let scheduled_at = DateTime::parse_from_rfc3339(scheduled_at)
                .expect("timestamp")
                .with_timezone(&Utc);
            let run_id = Uuid::new_v4();
            store
                .create_run(crate::NewRun {
                    id: run_id,
                    schedule_id,
                    trigger_kind: "manual".into(),
                    scheduled_at,
                    encrypted_snapshot: vec![2],
                    key_id: "test".into(),
                    max_attempts: 1,
                    initial_backoff_seconds: 1,
                    backoff_cap_seconds: 1,
                    idempotency_key: None,
                })
                .await
                .expect("run");
            let row = sqlx::query(
                "SELECT operations_timezone,operations_day,completion_deadline_at \
                 FROM trigger_identities WHERE target_id=?",
            )
            .bind(run_id.to_string())
            .fetch_one(store.pool())
            .await
            .expect("trigger");
            assert_eq!(row.get::<String, _>("operations_timezone"), "Europe/Vienna");
            assert!(matches!(
                row.get::<String, _>("operations_day").as_str(),
                "2026-03-29" | "2026-10-25"
            ));
            let deadline = parse_time(row.get("completion_deadline_at")).expect("deadline");
            assert_eq!((deadline - scheduled_at).num_seconds(), 86_400);
        }
    }
}
