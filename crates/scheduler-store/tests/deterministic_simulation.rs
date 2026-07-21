use std::collections::{BTreeMap, HashMap};

use chrono::{Duration, TimeZone, Utc};
use rand::{Rng, SeedableRng, rngs::StdRng};
use scheduler_core::{
    ArtifactRef, BatchItemState, ExecutionOutcome, ExecutionResult, OutputMetadata, RunState,
    ScheduleSpec,
};
use scheduler_store::{
    AttemptRecord, CommitCollectionPage, CommitPageOutcome, FinalizeBatchOutcome, NewBatch,
    NewBatchItem, NewRun, NewSchedule, Store,
};
use tempfile::TempDir;
use tokio::task::JoinSet;
use uuid::Uuid;

const DEFAULT_SEED_COUNT: u64 = 4;
const DEFAULT_STEPS: usize = 120;
const MAX_SEED_COUNT: u64 = 256;
const MAX_STEPS: usize = 5_000;

struct Database {
    _directory: TempDir,
    url: String,
    store: Store,
}

impl Database {
    async fn new(seed: u64) -> Self {
        let directory = tempfile::tempdir().expect("create simulation directory");
        let path = directory.path().join(format!("simulation-{seed}.db"));
        let url = format!("sqlite://{}", path.display());
        let store = Store::connect(&url, None)
            .await
            .expect("connect simulation store");
        Self {
            _directory: directory,
            url,
            store,
        }
    }

    async fn reopen(&mut self) {
        self.store.pool().close().await;
        // Windows can retain the final SQLite/WAL file lock for a few
        // scheduler ticks after every pool handle has closed. A real process
        // restart naturally has this delay; the in-process crash simulator
        // must model it explicitly instead of treating an OS cleanup race as
        // a scheduler invariant failure.
        let mut delay = std::time::Duration::from_millis(10);
        for attempt in 0..8 {
            match Store::connect(&self.url, None).await {
                Ok(store) => {
                    self.store = store;
                    return;
                }
                Err(error)
                    if attempt < 7
                        && (error.to_string().contains("database is locked")
                            || error.to_string().contains("database is busy")) =>
                {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_millis(500));
                }
                Err(error) => panic!("reopen simulation store: {error:#}"),
            }
        }
        unreachable!("bounded reconnect loop always returns or panics")
    }
}

fn configured_u64(name: &str, default: u64, maximum: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .clamp(1, maximum)
}

fn simulation_seeds() -> std::ops::Range<u64> {
    let start = std::env::var("SCHEDULER_STORE_SIM_SEED_START")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let count = configured_u64(
        "SCHEDULER_STORE_SIM_SEEDS",
        DEFAULT_SEED_COUNT,
        MAX_SEED_COUNT,
    );
    start..start.saturating_add(count)
}

fn simulation_steps() -> usize {
    configured_u64(
        "SCHEDULER_STORE_SIM_STEPS",
        DEFAULT_STEPS as u64,
        MAX_STEPS as u64,
    ) as usize
}

fn deterministic_uuid(seed: u64, sequence: u64) -> Uuid {
    Uuid::from_u128(((seed as u128) << 64) | sequence as u128)
}

fn fixed_time(offset_seconds: i64) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
        .single()
        .expect("valid fixed time")
        + Duration::seconds(offset_seconds)
}

fn schedule_spec(name: impl Into<String>) -> ScheduleSpec {
    ScheduleSpec {
        name: name.into(),
        blueprint_ref: ArtifactRef {
            uri: "file:///simulation/blueprint.json".into(),
        },
        parameters_ref: ArtifactRef {
            uri: "file:///simulation/parameters.json".into(),
        },
        parameter_collection: None,
        required_labels: BTreeMap::new(),
        cron: None,
        webhook_enabled: true,
        enabled: true,
    }
}

async fn create_schedule(store: &Store, id: Uuid, name: &str) {
    store
        .create_schedule(NewSchedule {
            id,
            spec: schedule_spec(name),
            encrypted_snapshot: vec![0xa5, 0x5a],
            snapshot_digest: format!("digest-{id}"),
            key_id: "simulation-key".into(),
            webhook_public_id: Some(format!("public-{id}")),
            webhook_secret_hash: Some("argon2-hash-placeholder".into()),
        })
        .await
        .expect("create schedule");
}

fn new_run(
    id: Uuid,
    schedule_id: Uuid,
    trigger_kind: &str,
    scheduled_at: chrono::DateTime<Utc>,
    idempotency_key: Option<String>,
) -> NewRun {
    NewRun {
        id,
        schedule_id,
        trigger_kind: trigger_kind.into(),
        scheduled_at,
        encrypted_snapshot: vec![0x12, 0x34],
        key_id: "simulation-key".into(),
        max_attempts: 3,
        initial_backoff_seconds: 0,
        backoff_cap_seconds: 0,
        idempotency_key,
    }
}

fn result(outcome: ExecutionOutcome, sequence: u64) -> ExecutionResult {
    let started_at = fixed_time(sequence as i64);
    ExecutionResult {
        outcome,
        exit_code: match outcome {
            ExecutionOutcome::Succeeded => Some(0),
            ExecutionOutcome::Failed => Some(1),
            _ => None,
        },
        signal: None,
        stdout: String::new(),
        stderr: String::new(),
        started_at,
        finished_at: started_at + Duration::milliseconds(5),
        error: None,
        output: OutputMetadata::default(),
        diagnostic: None,
    }
}

fn sim_unwrap<T, E: std::fmt::Display>(
    value: Result<T, E>,
    seed: u64,
    step: usize,
    trace: &[String],
) -> T {
    value.unwrap_or_else(|error| {
        panic!(
            "seed={seed} step={step} error={error}\ntrace:\n{}",
            trace.join("\n")
        )
    })
}

#[tokio::test]
async fn paged_collection_replay_is_deterministic_across_crash_boundaries() {
    for seed in simulation_seeds() {
        let mut database = Database::new(seed + 50_000).await;
        let schedule_id = deterministic_uuid(seed + 50_000, 1);
        create_schedule(&database.store, schedule_id, "collection-simulation").await;
        let batch_id = deterministic_uuid(seed + 50_000, 2);
        database
            .store
            .create_batch(NewBatch {
                id: batch_id,
                schedule_id,
                schedule_revision: 1,
                trigger_kind: "manual".into(),
                scheduled_at: fixed_time(seed as i64),
                idempotency_key: Some(format!("collection-{seed}")),
                encrypted_snapshot: vec![1],
                encrypted_trigger_overrides: None,
                snapshot_digest: format!("snapshot-{seed}"),
                key_id: "simulation-key".into(),
                page_size: 7,
                max_items: 10_000,
                max_active_runs: 32,
                poison_distinct_nodes: 2,
            })
            .await
            .expect("create collection batch");
        let claimed = database
            .store
            .claim_collection_batches("simulation-collector", 300, 1)
            .await
            .expect("claim batch");
        let lease_token = claimed[0]
            .lease_token
            .as_deref()
            .expect("lease token")
            .to_owned();
        let mut rng = StdRng::seed_from_u64(seed ^ 0xa11c_e5ed);
        let mut cursor = "start".to_owned();
        let mut item_index = 0_u32;
        for generation in 0..4_u64 {
            let is_final = generation == 3;
            let page_len = if is_final { 4 } else { 7 };
            let next_cursor = if is_final {
                "end".to_owned()
            } else {
                format!("cursor-{}", generation + 1)
            };
            let items = (0..page_len)
                .map(|_| {
                    let index = item_index;
                    item_index += 1;
                    NewBatchItem {
                        id: deterministic_uuid(seed + 50_000, 100 + u64::from(index)),
                        item_index: index,
                        provider_key_encrypted: vec![index as u8],
                        provider_key_hmac: format!("key-{seed}-{index}"),
                        encrypted_parameters: vec![index as u8, 1],
                        encrypted_snapshot: Some(vec![index as u8, 2]),
                        key_id: "simulation-key".into(),
                        parameters_digest: format!("parameters-{seed}-{index}"),
                        state: BatchItemState::Ready,
                        failure_code: None,
                        max_attempts: Some(3),
                        initial_backoff_seconds: Some(1),
                        backoff_cap_seconds: Some(3),
                    }
                })
                .collect();
            let page = CommitCollectionPage {
                batch_id,
                lease_token: lease_token.clone(),
                expected_generation: generation,
                request_cursor_digest: cursor.clone(),
                page_digest: format!("page-{seed}-{generation}"),
                collection_snapshot_encrypted: vec![3],
                collection_snapshot_digest: format!("provider-snapshot-{seed}"),
                next_cursor_encrypted: (!is_final).then(|| vec![generation as u8]),
                next_cursor_digest: next_cursor.clone(),
                is_final,
                items,
            };
            assert_eq!(
                database
                    .store
                    .commit_collection_page(page.clone())
                    .await
                    .expect("commit page"),
                CommitPageOutcome::Applied
            );
            if rng.gen_bool(0.75) {
                database.reopen().await;
            }
            assert_eq!(
                database
                    .store
                    .commit_collection_page(page)
                    .await
                    .expect("replay committed page"),
                CommitPageOutcome::Replayed
            );
            cursor = next_cursor;
        }
        database.reopen().await;
        assert_eq!(
            database
                .store
                .finalize_batch(batch_id, &lease_token)
                .await
                .expect("finalize batch"),
            FinalizeBatchOutcome::Finalized
        );
        database.reopen().await;
        assert_eq!(
            database
                .store
                .finalize_batch(batch_id, &lease_token)
                .await
                .expect("replay finalization"),
            FinalizeBatchOutcome::AlreadyFinalized
        );
        assert_eq!(
            database
                .store
                .list_batch_items(batch_id, 100)
                .await
                .expect("items")
                .len(),
            25
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runs WHERE batch_id=?")
                .bind(batch_id.to_string())
                .fetch_one(database.store.pool())
                .await
                .expect("run count"),
            25,
            "seed {seed} produced duplicate or missing child runs"
        );
    }
}

async fn assert_database_invariants(store: &Store, seed: u64, step: usize, trace: &[String]) {
    let integrity: String = sim_unwrap(
        sqlx::query_scalar("PRAGMA integrity_check")
            .fetch_one(store.pool())
            .await,
        seed,
        step,
        trace,
    );
    assert_eq!(
        integrity,
        "ok",
        "seed={seed} step={step} SQLite integrity failure; trace:\n{}",
        trace.join("\n")
    );

    let foreign_key_violations = sim_unwrap(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(store.pool())
            .await,
        seed,
        step,
        trace,
    );
    assert!(
        foreign_key_violations.is_empty(),
        "seed={seed} step={step} foreign-key violations; trace:\n{}",
        trace.join("\n")
    );

    let duplicate_idempotency_keys: i64 = sim_unwrap(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM (SELECT schedule_id,idempotency_key FROM runs WHERE idempotency_key IS NOT NULL GROUP BY schedule_id,idempotency_key HAVING COUNT(*) > 1)",
        )
        .fetch_one(store.pool())
        .await,
        seed,
        step,
        trace,
    );
    let duplicate_cron_occurrences: i64 = sim_unwrap(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM (SELECT schedule_id,scheduled_at FROM runs WHERE trigger_kind='cron' GROUP BY schedule_id,scheduled_at HAVING COUNT(*) > 1)",
        )
        .fetch_one(store.pool())
        .await,
        seed,
        step,
        trace,
    );
    assert_eq!(
        (duplicate_idempotency_keys, duplicate_cron_occurrences),
        (0, 0),
        "seed={seed} step={step} duplicate durable trigger; trace:\n{}",
        trace.join("\n")
    );

    let bad_attempt_counts: i64 = sim_unwrap(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM runs r WHERE r.attempt_count != MAX(0,(SELECT COUNT(*) FROM attempts a WHERE a.run_id=r.id AND a.accepted_at IS NOT NULL)-r.attempt_offset)",
        )
        .fetch_one(store.pool())
        .await,
        seed,
        step,
        trace,
    );
    assert_eq!(
        bad_attempt_counts,
        0,
        "seed={seed} step={step} retry accounting mismatch; trace:\n{}",
        trace.join("\n")
    );

    let missing_queued_audits: i64 = sim_unwrap(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM runs r WHERE NOT EXISTS (SELECT 1 FROM audit_events e WHERE e.entity_type='run' AND e.entity_id=r.id AND e.event_type='run.queued')",
        )
        .fetch_one(store.pool())
        .await,
        seed,
        step,
        trace,
    );
    let missing_offer_audits: i64 = sim_unwrap(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM attempts a WHERE NOT EXISTS (SELECT 1 FROM audit_events e WHERE e.entity_type='run' AND e.entity_id=a.run_id AND e.event_type='attempt.offered' AND json_extract(e.metadata_json,'$.attempt_id')=a.id)",
        )
        .fetch_one(store.pool())
        .await,
        seed,
        step,
        trace,
    );
    assert_eq!(
        (missing_queued_audits, missing_offer_audits),
        (0, 0),
        "seed={seed} step={step} state transition lacks audit event; trace:\n{}",
        trace.join("\n")
    );
}

async fn expire_attempt(store: &Store, attempt: &AttemptRecord) {
    sqlx::query("UPDATE attempts SET lease_expires_at=? WHERE id=?")
        .bind(fixed_time(-1).to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .bind(attempt.id.to_string())
        .execute(store.pool())
        .await
        .expect("inject expired lease");
}

#[tokio::test]
async fn seeded_delivery_state_machine_survives_restarts_and_reordering() {
    for seed in simulation_seeds() {
        let mut database = Database::new(seed).await;
        let mut rng = StdRng::seed_from_u64(seed);
        let mut sequence = 1_u64;
        let schedule_ids = [
            deterministic_uuid(seed, sequence),
            deterministic_uuid(seed, sequence + 1),
        ];
        sequence += 2;
        for (index, schedule_id) in schedule_ids.iter().enumerate() {
            create_schedule(&database.store, *schedule_id, &format!("schedule-{index}")).await;
        }

        let mut attempts: HashMap<Uuid, AttemptRecord> = HashMap::new();
        let mut trace = Vec::new();

        for step in 0..simulation_steps() {
            let action = rng.gen_range(0..12);
            match action {
                0..=2 => {
                    let schedule_id = schedule_ids[rng.gen_range(0..schedule_ids.len())];
                    let key_slot = rng.gen_range(0..12);
                    let key = format!("seed-{seed}-key-{key_slot}");
                    let proposed_id = deterministic_uuid(seed, sequence);
                    sequence += 1;
                    trace.push(format!(
                        "{step}: create/duplicate webhook schedule={schedule_id} key={key} proposed={proposed_id}"
                    ));
                    sim_unwrap(
                        database
                            .store
                            .create_run(new_run(
                                proposed_id,
                                schedule_id,
                                "webhook",
                                fixed_time(key_slot),
                                Some(key),
                            ))
                            .await,
                        seed,
                        step,
                        &trace,
                    );
                }
                3 => {
                    let schedule_id = schedule_ids[rng.gen_range(0..schedule_ids.len())];
                    let occurrence = rng.gen_range(0..12);
                    let proposed_id = deterministic_uuid(seed, sequence);
                    sequence += 1;
                    trace.push(format!(
                        "{step}: create/duplicate cron schedule={schedule_id} occurrence={occurrence} proposed={proposed_id}"
                    ));
                    sim_unwrap(
                        database
                            .store
                            .create_run(new_run(
                                proposed_id,
                                schedule_id,
                                "cron",
                                fixed_time(occurrence * 60),
                                None,
                            ))
                            .await,
                        seed,
                        step,
                        &trace,
                    );
                }
                4 => {
                    trace.push(format!("{step}: close all connections and reopen database"));
                    database.reopen().await;
                }
                _ => {
                    let runs =
                        sim_unwrap(database.store.list_runs(1_000).await, seed, step, &trace);
                    if runs.is_empty() {
                        continue;
                    }
                    let run = &runs[rng.gen_range(0..runs.len())];
                    let views = sim_unwrap(
                        database.store.run_attempts(run.id).await,
                        seed,
                        step,
                        &trace,
                    );
                    let active = views.iter().rev().find(|attempt| {
                        matches!(attempt.state.as_str(), "offered" | "accepted" | "expiring")
                    });

                    match run.state {
                        RunState::Queued if rng.gen_bool(0.85) => {
                            trace.push(format!("{step}: offer run={}", run.id));
                            if let Some(attempt) = sim_unwrap(
                                database.store.create_attempt(run.id, "agent-sim", 60).await,
                                seed,
                                step,
                                &trace,
                            ) {
                                attempts.insert(attempt.id, attempt);
                            }
                        }
                        RunState::Queued => {
                            trace.push(format!("{step}: cancel queued run={}", run.id));
                            sim_unwrap(database.store.cancel_run(run.id).await, seed, step, &trace);
                        }
                        RunState::Running => {
                            let Some(active) = active else {
                                panic!(
                                    "seed={seed} step={step} running run {} has no active attempt\ntrace:\n{}",
                                    run.id,
                                    trace.join("\n")
                                );
                            };
                            let attempt = attempts.get(&active.id).unwrap_or_else(|| {
                                panic!(
                                    "seed={seed} step={step} missing token for attempt {}\ntrace:\n{}",
                                    active.id,
                                    trace.join("\n")
                                )
                            });
                            match active.state.as_str() {
                                "offered" if rng.gen_bool(0.75) => {
                                    trace.push(format!("{step}: accept attempt={}", active.id));
                                    sim_unwrap(
                                        database
                                            .store
                                            .accept_attempt(active.id, &attempt.lease_token, 60)
                                            .await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                    if rng.gen_bool(0.25) {
                                        trace.push(format!(
                                            "{step}: redeliver acceptance attempt={}",
                                            active.id
                                        ));
                                        sim_unwrap(
                                            database
                                                .store
                                                .accept_attempt(active.id, &attempt.lease_token, 60)
                                                .await,
                                            seed,
                                            step,
                                            &trace,
                                        );
                                    }
                                }
                                "offered" if rng.gen_bool(0.5) => {
                                    trace.push(format!(
                                        "{step}: release offer attempt={}",
                                        active.id
                                    ));
                                    sim_unwrap(
                                        database.store.release_offer(active.id).await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                }
                                "offered" => {
                                    trace.push(format!(
                                        "{step}: expire unaccepted offer={}",
                                        active.id
                                    ));
                                    expire_attempt(&database.store, attempt).await;
                                    let claimed = sim_unwrap(
                                        database.store.claim_expired_attempts(500, 0).await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                    assert!(
                                        claimed.iter().any(|found| found.id == active.id),
                                        "seed={seed} step={step} expired offer was not claimed\ntrace:\n{}",
                                        trace.join("\n")
                                    );
                                    sim_unwrap(
                                        database
                                            .store
                                            .finish_expired_attempt(
                                                active.id,
                                                &attempt.lease_token,
                                                &result(ExecutionOutcome::LeaseExpired, sequence),
                                                vec![0xee],
                                                "simulation-key",
                                            )
                                            .await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                    sequence += 1;
                                }
                                "accepted" if rng.gen_bool(0.15) => {
                                    trace.push(format!(
                                        "{step}: cancel accepted attempt={}",
                                        active.id
                                    ));
                                    sim_unwrap(
                                        database.store.cancel_run(run.id).await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                }
                                "accepted" if rng.gen_bool(0.20) => {
                                    trace.push(format!(
                                        "{step}: expire accepted attempt={}",
                                        active.id
                                    ));
                                    expire_attempt(&database.store, attempt).await;
                                    let claimed = sim_unwrap(
                                        database.store.claim_expired_attempts(500, 0).await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                    assert!(
                                        claimed.iter().any(|found| found.id == active.id),
                                        "seed={seed} step={step} expired acceptance was not claimed\ntrace:\n{}",
                                        trace.join("\n")
                                    );
                                    let stale = sim_unwrap(
                                        database
                                            .store
                                            .finish_attempt(
                                                active.id,
                                                &attempt.lease_token,
                                                &result(ExecutionOutcome::Succeeded, sequence),
                                                vec![0x99],
                                                "simulation-key",
                                            )
                                            .await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                    assert_eq!(stale, RunState::Running);
                                    sequence += 1;
                                    sim_unwrap(
                                        database
                                            .store
                                            .finish_expired_attempt(
                                                active.id,
                                                &attempt.lease_token,
                                                &result(ExecutionOutcome::LeaseExpired, sequence),
                                                vec![0xee],
                                                "simulation-key",
                                            )
                                            .await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                    sequence += 1;
                                }
                                "accepted" if rng.gen_bool(0.20) => {
                                    trace.push(format!("{step}: renew attempt={}", active.id));
                                    sim_unwrap(
                                        database
                                            .store
                                            .renew_attempts(
                                                "agent-sim",
                                                &[active.id.to_string()],
                                                60,
                                                1,
                                            )
                                            .await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                }
                                "accepted" => {
                                    let outcome = if rng.gen_bool(0.35) {
                                        ExecutionOutcome::Succeeded
                                    } else {
                                        ExecutionOutcome::Failed
                                    };
                                    trace.push(format!(
                                        "{step}: finish and redeliver attempt={} outcome={outcome:?}",
                                        active.id
                                    ));
                                    let completion = result(outcome, sequence);
                                    sequence += 1;
                                    let first = sim_unwrap(
                                        database
                                            .store
                                            .finish_attempt(
                                                active.id,
                                                &attempt.lease_token,
                                                &completion,
                                                vec![0xcc],
                                                "simulation-key",
                                            )
                                            .await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                    let duplicate = sim_unwrap(
                                        database
                                            .store
                                            .finish_attempt(
                                                active.id,
                                                &attempt.lease_token,
                                                &completion,
                                                vec![0xcc],
                                                "simulation-key",
                                            )
                                            .await,
                                        seed,
                                        step,
                                        &trace,
                                    );
                                    assert_eq!(first, duplicate);
                                }
                                state => panic!(
                                    "seed={seed} step={step} unexpected active state {state}\ntrace:\n{}",
                                    trace.join("\n")
                                ),
                            }
                        }
                        RunState::Failed if rng.gen_bool(0.4) => {
                            trace.push(format!("{step}: manually retry failed run={}", run.id));
                            sim_unwrap(database.store.retry_run(run.id).await, seed, step, &trace);
                        }
                        RunState::Succeeded | RunState::Failed | RunState::Cancelled => {
                            trace.push(format!(
                                "{step}: observe terminal run={} state={:?}",
                                run.id, run.state
                            ));
                        }
                    }
                }
            }

            assert_database_invariants(&database.store, seed, step, &trace).await;
        }

        database.reopen().await;
        assert_database_invariants(&database.store, seed, simulation_steps(), &trace).await;
    }
}

#[tokio::test]
async fn concurrent_duplicate_triggers_commit_exactly_one_run_and_audit_event() {
    for seed in simulation_seeds() {
        let mut database = Database::new(seed ^ 0xc011_c011).await;
        let schedule_id = deterministic_uuid(seed, 1);
        create_schedule(&database.store, schedule_id, "concurrent-trigger").await;
        let scheduled_at = fixed_time(seed as i64);

        let mut tasks = JoinSet::new();
        for sequence in 2..34 {
            let store = database.store.clone();
            tasks.spawn(async move {
                store
                    .create_run(new_run(
                        deterministic_uuid(seed, sequence),
                        schedule_id,
                        "webhook",
                        scheduled_at,
                        Some("same-delivery".into()),
                    ))
                    .await
            });
        }

        let mut returned_ids = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            returned_ids.push(
                joined
                    .unwrap_or_else(|error| panic!("seed={seed}: trigger task panicked: {error}"))
                    .unwrap_or_else(|error| panic!("seed={seed}: trigger failed: {error}"))
                    .id,
            );
        }
        assert!(
            returned_ids.windows(2).all(|ids| ids[0] == ids[1]),
            "seed={seed}: concurrent duplicates returned different runs: {returned_ids:?}"
        );

        database.reopen().await;
        let run_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM runs WHERE schedule_id=? AND idempotency_key='same-delivery'",
        )
        .bind(schedule_id.to_string())
        .fetch_one(database.store.pool())
        .await
        .expect("count runs");
        let audit_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_events WHERE entity_type='run' AND event_type='run.queued'",
        )
        .fetch_one(database.store.pool())
        .await
        .expect("count audits");
        assert_eq!((run_count, audit_count), (1, 1), "seed={seed}");
    }
}

#[tokio::test]
async fn cancellation_wins_over_late_and_conflicting_result_redelivery_after_restart() {
    for seed in simulation_seeds() {
        let mut database = Database::new(seed ^ 0xca11_ce11).await;
        let schedule_id = deterministic_uuid(seed, 1);
        let run_id = deterministic_uuid(seed, 2);
        create_schedule(&database.store, schedule_id, "cancel-race").await;
        database
            .store
            .create_run(new_run(run_id, schedule_id, "manual", fixed_time(0), None))
            .await
            .expect("create run");
        let attempt = database
            .store
            .create_attempt(run_id, "agent-cancel", 60)
            .await
            .expect("offer")
            .expect("attempt");
        database
            .store
            .accept_attempt(attempt.id, &attempt.lease_token, 60)
            .await
            .expect("accept");
        let cancelled = database.store.cancel_run(run_id).await.expect("cancel");
        assert_eq!(cancelled, vec![("agent-cancel".into(), attempt.id)]);

        database.reopen().await;
        let late_failure = result(ExecutionOutcome::Failed, 3);
        let state = database
            .store
            .finish_attempt(
                attempt.id,
                &attempt.lease_token,
                &late_failure,
                vec![0xfa],
                "simulation-key",
            )
            .await
            .expect("late result");
        assert_eq!(state, RunState::Cancelled);

        let conflicting_success = result(ExecutionOutcome::Succeeded, 4);
        let duplicate_state = database
            .store
            .finish_attempt(
                attempt.id,
                &attempt.lease_token,
                &conflicting_success,
                vec![0xfb],
                "simulation-key",
            )
            .await
            .expect("conflicting duplicate");
        assert_eq!(duplicate_state, RunState::Cancelled);

        let attempts = database.store.run_attempts(run_id).await.expect("attempts");
        assert_eq!(attempts[0].state, "late_result");
        assert_eq!(attempts[0].outcome.as_deref(), Some("failed"));
        let late_audits: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_events WHERE entity_type='run' AND entity_id=? AND event_type='attempt.late_result'",
        )
        .bind(run_id.to_string())
        .fetch_one(database.store.pool())
        .await
        .expect("audit count");
        assert_eq!(
            late_audits, 1,
            "seed={seed}: duplicate result must be idempotent"
        );
    }
}

#[tokio::test]
async fn lease_expiry_claim_is_single_winner_and_preserves_at_least_once_retry() {
    for seed in simulation_seeds() {
        let mut database = Database::new(seed ^ 0x1ea5_e001).await;
        let schedule_id = deterministic_uuid(seed, 1);
        let run_id = deterministic_uuid(seed, 2);
        create_schedule(&database.store, schedule_id, "lease-expiry").await;
        database
            .store
            .create_run(new_run(run_id, schedule_id, "manual", fixed_time(0), None))
            .await
            .expect("create run");
        let first = database
            .store
            .create_attempt(run_id, "agent-lease", 60)
            .await
            .expect("offer")
            .expect("attempt");
        database
            .store
            .accept_attempt(first.id, &first.lease_token, 60)
            .await
            .expect("accept");
        expire_attempt(&database.store, &first).await;

        let claimed = database
            .store
            .claim_expired_attempts(100, 0)
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "seed={seed}");
        assert!(
            database
                .store
                .claim_expired_attempts(100, 0)
                .await
                .expect("duplicate claim")
                .is_empty(),
            "seed={seed}: expiry must have a single claimant"
        );

        database.reopen().await;
        let state = database
            .store
            .finish_expired_attempt(
                first.id,
                &first.lease_token,
                &result(ExecutionOutcome::LeaseExpired, 3),
                vec![0xee],
                "simulation-key",
            )
            .await
            .expect("finish expiry");
        assert_eq!(state, Some(RunState::Queued));

        let stale_state = database
            .store
            .finish_attempt(
                first.id,
                &first.lease_token,
                &result(ExecutionOutcome::Succeeded, 4),
                vec![0xdd],
                "simulation-key",
            )
            .await
            .expect("late original result");
        assert_eq!(stale_state, RunState::Queued);

        let second = database
            .store
            .create_attempt(run_id, "agent-lease", 60)
            .await
            .expect("second offer")
            .expect("second attempt");
        database
            .store
            .accept_attempt(second.id, &second.lease_token, 60)
            .await
            .expect("accept second");
        let final_state = database
            .store
            .finish_attempt(
                second.id,
                &second.lease_token,
                &result(ExecutionOutcome::Succeeded, 5),
                vec![0xcc],
                "simulation-key",
            )
            .await
            .expect("finish second");
        assert_eq!(final_state, RunState::Succeeded);
        assert_eq!(
            database
                .store
                .get_run(run_id)
                .await
                .unwrap()
                .unwrap()
                .attempt_count,
            2,
            "seed={seed}: both accepted deliveries must remain accounted"
        );
        let attempts = database.store.run_attempts(run_id).await.expect("attempts");
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].outcome.as_deref(), Some("lease_expired"));
        assert_eq!(attempts[1].outcome.as_deref(), Some("succeeded"));
    }
}

#[tokio::test]
async fn settings_locks_and_revisions_survive_restarts_and_stale_writers() {
    for seed in simulation_seeds() {
        let mut database = Database::new(seed ^ 0x5e77_1a65).await;
        let original = database
            .store
            .get_global_settings()
            .await
            .expect("global settings");
        let first = database
            .store
            .acquire_lock("global", "writer-a")
            .await
            .expect("first lock");
        assert!(
            database
                .store
                .acquire_lock("global", "writer-b")
                .await
                .is_err()
        );

        database.reopen().await;
        assert!(
            database
                .store
                .acquire_lock("global", "writer-b")
                .await
                .is_err()
        );
        database
            .store
            .renew_lock("global", &first.lock_token)
            .await
            .expect("renew after reopen");

        let mut updated = serde_json::to_value(&original).expect("serialize settings");
        updated["default_timeout_seconds"] = (seed + 10).into();
        let new_revision = database
            .store
            .update_settings(
                "global",
                original.revision,
                &updated.to_string(),
                &first.lock_token,
            )
            .await
            .expect("first update");
        assert_eq!(new_revision, original.revision + 1);
        assert!(
            database
                .store
                .update_settings(
                    "global",
                    original.revision,
                    &updated.to_string(),
                    &first.lock_token,
                )
                .await
                .is_err(),
            "seed={seed}: stale ETag/revision must be rejected"
        );

        database
            .store
            .release_lock("global", "wrong-token", false)
            .await
            .expect("wrong-token release is idempotent");
        assert!(
            database
                .store
                .acquire_lock("global", "writer-b")
                .await
                .is_err()
        );
        database
            .store
            .release_lock("global", &first.lock_token, false)
            .await
            .expect("release");
        let second = database
            .store
            .acquire_lock("global", "writer-b")
            .await
            .expect("second writer lock");

        sqlx::query("UPDATE edit_locks SET expires_at=? WHERE document_key='global'")
            .bind(fixed_time(-1).to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .execute(database.store.pool())
            .await
            .expect("inject abandoned lock");
        let recovered = database
            .store
            .acquire_lock("global", "writer-c")
            .await
            .expect("recover expired lock");
        assert_ne!(second.lock_token, recovered.lock_token);

        database.reopen().await;
        let stored = database
            .store
            .get_global_settings()
            .await
            .expect("settings after restart");
        assert_eq!(stored.revision, new_revision);
        assert_eq!(stored.default_timeout_seconds, seed + 10);
        let audit_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_events WHERE entity_type='settings' AND entity_id='global' AND event_type='settings.updated'",
        )
        .fetch_one(database.store.pool())
        .await
        .expect("settings audits");
        assert_eq!(audit_count, 1, "seed={seed}");
    }
}

#[tokio::test]
async fn expected_lock_conflicts_release_sqlite_writer_before_immediate_update() {
    // Regression for an intermittent SQLITE_BUSY observed when this test ran with
    // the full suite: acquire_lock returned its expected business error by dropping
    // a write transaction, but did not await that transaction's rollback.
    for seed in simulation_seeds() {
        let database = Database::new(seed ^ 0xb055_10cc).await;
        for round in 0..20_u64 {
            let settings = database
                .store
                .get_global_settings()
                .await
                .expect("read settings");
            let winner = database
                .store
                .acquire_lock("global", "winner")
                .await
                .expect("winner lock");

            let mut conflicts = JoinSet::new();
            for contender in 0..8 {
                let store = database.store.clone();
                conflicts.spawn(async move {
                    store
                        .acquire_lock("global", &format!("contender-{contender}"))
                        .await
                });
            }
            while let Some(conflict) = conflicts.join_next().await {
                assert!(
                    conflict.expect("contender task").is_err(),
                    "seed={seed} round={round}: contender unexpectedly acquired lock"
                );
            }

            let mut document = serde_json::to_value(&settings).expect("settings JSON");
            document["default_timeout_seconds"] = (round + 1).into();
            let revision = database
                .store
                .update_settings(
                    "global",
                    settings.revision,
                    &document.to_string(),
                    &winner.lock_token,
                )
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "seed={seed} round={round}: immediate update after expected lock conflicts failed: {error}"
                    )
                });
            assert_eq!(revision, settings.revision + 1);
            database
                .store
                .release_lock("global", &winner.lock_token, false)
                .await
                .expect("release winner lock");
        }
    }
}

#[tokio::test]
async fn interrupted_transaction_rolls_back_completely_before_reopen() {
    for seed in simulation_seeds() {
        let mut database = Database::new(seed ^ 0x7a11_bacc).await;
        let mut transaction = database.store.pool().begin().await.expect("begin");
        sqlx::query("INSERT INTO audit_events(entity_type,entity_id,event_type,metadata_json,occurred_at) VALUES ('fault','in-flight','must.rollback','{}',?)")
            .bind(fixed_time(seed as i64).to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .execute(&mut *transaction)
            .await
            .expect("uncommitted insert");
        drop(transaction);

        database.reopen().await;
        let leaked: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM audit_events WHERE event_type='must.rollback'",
        )
        .fetch_one(database.store.pool())
        .await
        .expect("count rolled back rows");
        assert_eq!(leaked, 0, "seed={seed}: partial transaction became durable");
        let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
            .fetch_one(database.store.pool())
            .await
            .expect("integrity check");
        assert_eq!(integrity, "ok", "seed={seed}");
    }
}

#[tokio::test]
async fn cron_occurrence_cursor_is_monotonic_across_out_of_order_delivery_and_restart() {
    for seed in simulation_seeds() {
        let mut database = Database::new(seed ^ 0xc205_0a11).await;
        let schedule_id = deterministic_uuid(seed, 1);
        create_schedule(&database.store, schedule_id, "cron-cursor").await;
        let occurrences = [
            fixed_time(600),
            fixed_time(60),
            fixed_time(300),
            fixed_time(900),
        ];
        for occurrence in occurrences {
            database
                .store
                .advance_cron_cursor(schedule_id, occurrence)
                .await
                .expect("advance cursor");
        }
        database.reopen().await;
        let record = database
            .store
            .get_schedule_record(schedule_id)
            .await
            .expect("schedule")
            .expect("present");
        assert_eq!(record.last_cron_at, Some(fixed_time(900)), "seed={seed}");
    }
}
