use std::collections::HashSet;

use chrono::{Duration, SecondsFormat, TimeZone, Utc};
use scheduler_store::Store;
use sqlx::Row;
use tempfile::TempDir;
use uuid::Uuid;

const HISTORY_SIZE: usize = 240;
const PAGE_SIZE: u32 = 37;

struct TestDatabase {
    _directory: TempDir,
    store: Store,
}

impl TestDatabase {
    async fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("pagination.db");
        let store = Store::connect(&format!("sqlite://{}", path.display()), None)
            .await
            .expect("store");
        Self {
            _directory: directory,
            store,
        }
    }
}

fn timestamp(index: usize) -> String {
    // Ten rows deliberately share each timestamp so the immutable ID
    // tie-breaker is exercised on every page boundary.
    (Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("base time")
        + Duration::seconds((index / 10) as i64))
    .to_rfc3339_opts(SecondsFormat::Millis, true)
}

async fn seed_large_history(database: &TestDatabase) {
    let spec = serde_json::json!({
        "name": "pagination fixture",
        "blueprint_ref": {"uri": "file:///blueprint.yaml"},
        "parameters_ref": {"uri": "file:///parameters.json"},
        "required_labels": {},
        "cron": null,
        "webhook_enabled": false,
        "enabled": true
    })
    .to_string();
    let mut transaction = database.store.pool().begin().await.expect("transaction");
    for index in 0..HISTORY_SIZE {
        let created_at = timestamp(index);
        let schedule_id = Uuid::from_u128(index as u128 + 1).to_string();
        let run_id = Uuid::from_u128(index as u128 + 10_001).to_string();
        let evidence_id = Uuid::from_u128(index as u128 + 20_001).to_string();
        let blueprint_digest = format!("{index:064x}");
        sqlx::query(
            "INSERT INTO schedules(id,name,spec_json,encrypted_snapshot,snapshot_digest,key_id,\
             revision,enabled,webhook_enabled,created_at,updated_at) \
             VALUES (?,?,?,?,?,'v1',1,1,0,?,?)",
        )
        .bind(&schedule_id)
        .bind(format!("schedule-{index}"))
        .bind(&spec)
        .bind(vec![1_u8])
        .bind(format!("snapshot-{index}"))
        .bind(&created_at)
        .bind(&created_at)
        .execute(&mut *transaction)
        .await
        .expect("schedule");
        sqlx::query(
            "INSERT INTO runs(id,schedule_id,state,trigger_kind,scheduled_at,not_before,\
             encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,backoff_cap_seconds,\
             created_at,updated_at) VALUES (?,?,'queued','manual',?,?,?,'v1',3,5,300,?,?)",
        )
        .bind(&run_id)
        .bind(&schedule_id)
        .bind(&created_at)
        .bind(&created_at)
        .bind(vec![2_u8])
        .bind(&created_at)
        .bind(&created_at)
        .execute(&mut *transaction)
        .await
        .expect("run");
        sqlx::query(
            "INSERT INTO blueprint_revisions(digest,source_ref,loaded_at,executor_kind,\
             required_labels_json,execution_policy_json,parameter_schema_json,\
             binding_declarations_json,created_at) VALUES (?,'file:///blueprint.yaml',?,\
             'command','{}','{}','{}','{}',?)",
        )
        .bind(&blueprint_digest)
        .bind(&created_at)
        .bind(&created_at)
        .execute(&mut *transaction)
        .await
        .expect("blueprint");
        sqlx::query(
            "INSERT INTO health_evidence(id,run_id,schedule_id,agent_id,blueprint_digest,\
             input_fingerprint,classifier_version,evidence_class,failure_family,node_was_healthy,\
             cluster_suppressed,retracted,occurred_at,created_at) \
             VALUES (?,?,?,?,?,?,1,'functional','functional',1,0,0,?,?)",
        )
        .bind(&evidence_id)
        .bind(&run_id)
        .bind(&schedule_id)
        .bind(if index % 2 == 0 { "node-a" } else { "node-b" })
        .bind(&blueprint_digest)
        .bind(format!("input-{index}"))
        .bind(&created_at)
        .bind(&created_at)
        .execute(&mut *transaction)
        .await
        .expect("health evidence");
    }
    transaction.commit().await.expect("commit fixture");
}

fn next_time_cursor<T>(items: &[T], value: impl Fn(&T) -> (String, String)) -> (String, String) {
    value(items.last().expect("non-empty page"))
}

#[tokio::test]
async fn large_keyset_pages_are_complete_and_never_overlap() {
    let database = TestDatabase::new().await;
    seed_large_history(&database).await;

    let mut schedule_ids = HashSet::new();
    let mut schedule_cursor: Option<(String, String)> = None;
    loop {
        let page = database
            .store
            .list_schedules_page(
                schedule_cursor.as_ref().map(|cursor| cursor.0.as_str()),
                schedule_cursor.as_ref().map(|cursor| cursor.1.as_str()),
                PAGE_SIZE,
            )
            .await
            .expect("schedule page");
        if page.is_empty() {
            break;
        }
        for item in &page {
            assert!(schedule_ids.insert(item.id), "schedule page overlapped");
        }
        schedule_cursor = Some(next_time_cursor(&page, |item| {
            (
                item.created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
                item.id.to_string(),
            )
        }));
    }
    assert_eq!(schedule_ids.len(), HISTORY_SIZE);

    let mut run_ids = HashSet::new();
    let mut run_cursor: Option<(String, String)> = None;
    loop {
        let page = database
            .store
            .list_runs_page(
                run_cursor.as_ref().map(|cursor| cursor.0.as_str()),
                run_cursor.as_ref().map(|cursor| cursor.1.as_str()),
                PAGE_SIZE,
            )
            .await
            .expect("run page");
        if page.is_empty() {
            break;
        }
        for item in &page {
            assert!(run_ids.insert(item.id), "run page overlapped");
        }
        run_cursor = Some(next_time_cursor(&page, |item| {
            (
                item.created_at.to_rfc3339_opts(SecondsFormat::Millis, true),
                item.id.to_string(),
            )
        }));
    }
    assert_eq!(run_ids.len(), HISTORY_SIZE);

    let mut blueprint_ids = HashSet::new();
    let mut blueprint_cursor: Option<(String, String)> = None;
    loop {
        let page = database
            .store
            .list_blueprint_revisions_page(
                blueprint_cursor.as_ref().map(|cursor| cursor.0.as_str()),
                blueprint_cursor.as_ref().map(|cursor| cursor.1.as_str()),
                PAGE_SIZE,
            )
            .await
            .expect("blueprint page");
        if page.is_empty() {
            break;
        }
        for item in &page {
            assert!(
                blueprint_ids.insert(item.digest.clone()),
                "blueprint page overlapped"
            );
        }
        blueprint_cursor = Some(next_time_cursor(&page, |item| {
            (
                item.loaded_at.to_rfc3339_opts(SecondsFormat::Millis, true),
                item.digest.clone(),
            )
        }));
    }
    assert_eq!(blueprint_ids.len(), HISTORY_SIZE);

    let mut evidence_ids = HashSet::new();
    let mut evidence_cursor: Option<(String, String)> = None;
    loop {
        let page = database
            .store
            .list_health_evidence_page(
                Some("node-a"),
                evidence_cursor.as_ref().map(|cursor| cursor.0.as_str()),
                evidence_cursor.as_ref().map(|cursor| cursor.1.as_str()),
                PAGE_SIZE,
            )
            .await
            .expect("health evidence page");
        if page.is_empty() {
            break;
        }
        for item in &page {
            assert!(
                evidence_ids.insert(item.id),
                "health evidence page overlapped"
            );
        }
        evidence_cursor = Some(next_time_cursor(&page, |item| {
            (
                item.occurred_at
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
                item.id.to_string(),
            )
        }));
    }
    assert_eq!(evidence_ids.len(), HISTORY_SIZE / 2);
}

async fn explain(database: &TestDatabase, sql: &str) -> String {
    sqlx::query(sql)
        .fetch_all(database.store.pool())
        .await
        .expect("query plan")
        .into_iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn sqlite_uses_the_composite_keyset_indexes() {
    let database = TestDatabase::new().await;
    seed_large_history(&database).await;

    let cases = [
        (
            "idx_schedules_keyset",
            "EXPLAIN QUERY PLAN SELECT id FROM schedules \
             WHERE created_at<'9999-12-31T23:59:59.999Z' \
             ORDER BY created_at DESC,id DESC LIMIT 50",
        ),
        (
            "idx_runs_keyset",
            "EXPLAIN QUERY PLAN SELECT id FROM runs \
             WHERE created_at<'9999-12-31T23:59:59.999Z' \
             ORDER BY created_at DESC,id DESC LIMIT 50",
        ),
        (
            "idx_blueprint_revisions_keyset",
            "EXPLAIN QUERY PLAN SELECT digest FROM blueprint_revisions \
             WHERE loaded_at<'9999-12-31T23:59:59.999Z' \
             ORDER BY loaded_at DESC,digest DESC LIMIT 50",
        ),
        (
            "idx_health_evidence_keyset",
            "EXPLAIN QUERY PLAN SELECT id FROM health_evidence \
             WHERE occurred_at<'9999-12-31T23:59:59.999Z' \
             ORDER BY occurred_at DESC,id DESC LIMIT 50",
        ),
        (
            "idx_health_evidence_agent_keyset",
            "EXPLAIN QUERY PLAN SELECT id FROM health_evidence WHERE agent_id='node-a' \
             AND occurred_at<'9999-12-31T23:59:59.999Z' \
             ORDER BY occurred_at DESC,id DESC LIMIT 50",
        ),
        (
            "idx_batches_keyset",
            "EXPLAIN QUERY PLAN SELECT id FROM batches \
             WHERE created_at<'9999-12-31T23:59:59.999Z' \
             ORDER BY created_at DESC,id DESC LIMIT 50",
        ),
        (
            "idx_audit_events_keyset",
            "EXPLAIN QUERY PLAN SELECT id FROM audit_events \
             WHERE occurred_at<'9999-12-31T23:59:59.999Z' \
             ORDER BY occurred_at DESC,id DESC LIMIT 50",
        ),
        (
            "idx_audit_entity",
            "EXPLAIN QUERY PLAN SELECT id FROM audit_events \
             WHERE entity_type='run' AND entity_id='fixture' AND id<999999 \
             ORDER BY id DESC LIMIT 50",
        ),
        (
            "idx_attempts_run_keyset",
            "EXPLAIN QUERY PLAN SELECT id FROM attempts \
             WHERE run_id='fixture' AND attempt_number>0 \
             ORDER BY attempt_number,id LIMIT 50",
        ),
        (
            "sqlite_autoindex_agents_1",
            "EXPLAIN QUERY PLAN SELECT id FROM agents WHERE id>'node' ORDER BY id LIMIT 50",
        ),
    ];
    for (expected_index, sql) in cases {
        let plan = explain(&database, sql).await;
        assert!(
            plan.contains(expected_index),
            "expected {expected_index} in query plan:\n{plan}"
        );
    }

    // Also verify the nullable-cursor predicate used by the store, since it is
    // deliberately shaped to serve both the first and subsequent page.
    let cursor = "2026-01-01T00:00:20.000Z";
    let cursor_id = Uuid::from_u128(200).to_string();
    let schedule_plan = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT id,spec_json,revision,created_at,updated_at,webhook_public_id \
         FROM schedules WHERE (? IS NULL OR created_at<? OR (created_at=? AND id<?)) \
         ORDER BY created_at DESC,id DESC LIMIT ?",
    )
    .bind(cursor)
    .bind(cursor)
    .bind(cursor)
    .bind(&cursor_id)
    .bind(50_i64)
    .fetch_all(database.store.pool())
    .await
    .expect("schedule store query plan")
    .into_iter()
    .map(|row| row.get::<String, _>("detail"))
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        schedule_plan.contains("idx_schedules_keyset"),
        "store query must use idx_schedules_keyset:\n{schedule_plan}"
    );

    let health_plan = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT * FROM health_evidence WHERE agent_id=? \
         AND (? IS NULL OR occurred_at<? OR (occurred_at=? AND id<?)) \
         ORDER BY occurred_at DESC,id DESC LIMIT ?",
    )
    .bind("node-a")
    .bind(cursor)
    .bind(cursor)
    .bind(cursor)
    .bind(&cursor_id)
    .bind(50_i64)
    .fetch_all(database.store.pool())
    .await
    .expect("health store query plan")
    .into_iter()
    .map(|row| row.get::<String, _>("detail"))
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        health_plan.contains("idx_health_evidence_agent_keyset"),
        "store query must use idx_health_evidence_agent_keyset:\n{health_plan}"
    );

    let blueprint_plan = sqlx::query(
        "EXPLAIN QUERY PLAN SELECT b.*,\
         (SELECT COUNT(DISTINCT current.schedule_id) FROM schedule_blueprint_revisions current \
          WHERE current.blueprint_digest=b.digest AND current.is_current=1) AS current_count,\
         (SELECT COUNT(*) FROM schedule_blueprint_revisions retained \
          WHERE retained.blueprint_digest=b.digest) AS retained_count \
         FROM blueprint_revisions b \
         WHERE (? IS NULL OR b.loaded_at<? OR (b.loaded_at=? AND b.digest<?)) \
         ORDER BY b.loaded_at DESC,b.digest DESC LIMIT ?",
    )
    .bind(cursor)
    .bind(cursor)
    .bind(cursor)
    .bind(format!("{:064x}", 200))
    .bind(50_i64)
    .fetch_all(database.store.pool())
    .await
    .expect("blueprint store query plan")
    .into_iter()
    .map(|row| row.get::<String, _>("detail"))
    .collect::<Vec<_>>()
    .join("\n");
    assert!(
        blueprint_plan.contains("idx_blueprint_revisions_keyset"),
        "store query must use idx_blueprint_revisions_keyset:\n{blueprint_plan}"
    );
    assert!(
        blueprint_plan.contains("idx_schedule_blueprint_digest"),
        "blueprint counts must use idx_schedule_blueprint_digest:\n{blueprint_plan}"
    );
}
