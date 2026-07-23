//! Deterministic crash/replay simulation for the coordinator/agent durability protocol.
//!
//! Replay a failure with, for example:
//!
//! ```text
//! SCHEDULER_SIM_SEED=0x5EED5EED SCHEDULER_SIM_CASES=1 \
//!   cargo test -p coordinator --test protocol_simulation -- --nocapture
//! ```
//!
//! The default is intentionally fast enough for a normal `cargo test`; CI or a
//! soak runner can raise `SCHEDULER_SIM_CASES` up to the bounded maximum.

#[allow(dead_code)]
#[path = "../../agent/src/ledger.rs"]
mod agent_ledger;

use std::{collections::BTreeMap, path::PathBuf};

use agent_ledger::Ledger;
use chrono::{Duration, Utc};
use scheduler_core::{
    ArtifactRef, Blueprint, CommandSpec, ExecutionAssignment, ExecutionOutcome, ExecutionPolicy,
    ExecutionResult, ExecutionSnapshot, ExecutorSpec, FailureCode, FailureDiagnostic,
    FailureOrigin, FailureStage, FailureStatus, OutputMetadata, RunState, ScheduleSpec,
};
use scheduler_store::{NewRun, NewSchedule, Store};
use uuid::Uuid;

const DEFAULT_SEED: u64 = 0x5EED_5EED_D15C_A11E;
const DEFAULT_CASES: usize = 96;
const MAX_CASES: usize = 2_048;

#[derive(Clone, Copy)]
struct SimConfig {
    seed: u64,
    cases: usize,
}

impl SimConfig {
    fn from_env() -> Self {
        let seed = std::env::var("SCHEDULER_SIM_SEED")
            .ok()
            .and_then(|value| parse_seed(&value))
            .unwrap_or(DEFAULT_SEED);
        let cases = std::env::var("SCHEDULER_SIM_CASES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_CASES)
            .clamp(1, MAX_CASES);
        Self { seed, cases }
    }
}

#[derive(Clone, Copy)]
struct CasePlan {
    duplicate_offer: bool,
    restart_after_accept: bool,
    duplicate_acceptance: bool,
    duplicate_local_result: bool,
    restart_before_result_delivery: bool,
    restart_coordinator_before_result: bool,
    lose_first_result_ack: bool,
    restart_coordinator_after_result: bool,
    result_kind: ResultKind,
}

#[derive(Clone, Copy)]
enum ResultKind {
    Success,
    CommandExit,
    CommandCrash,
    ExecutorCrash,
    ExcelMacroFailure,
    ExcelComFailure,
}

impl CasePlan {
    fn generate(random: &mut DeterministicRandom) -> Self {
        Self {
            duplicate_offer: random.boolean(),
            restart_after_accept: random.boolean(),
            duplicate_acceptance: random.boolean(),
            duplicate_local_result: random.boolean(),
            restart_before_result_delivery: random.boolean(),
            restart_coordinator_before_result: random.boolean(),
            lose_first_result_ack: random.boolean(),
            restart_coordinator_after_result: random.boolean(),
            result_kind: match random.bounded(6) {
                0 => ResultKind::Success,
                1 => ResultKind::CommandExit,
                2 => ResultKind::CommandCrash,
                3 => ResultKind::ExecutorCrash,
                4 => ResultKind::ExcelMacroFailure,
                _ => ResultKind::ExcelComFailure,
            },
        }
    }
}

struct DeterministicRandom(u64);

impl DeterministicRandom {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        // xorshift64*: tiny, deterministic, and stable across Rust releases.
        let mut value = self.0;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        self.0 = value;
        value.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn boolean(&mut self) -> bool {
        self.next() & 1 == 1
    }

    fn bounded(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}

struct SimulationFiles {
    directory: PathBuf,
    coordinator_url: String,
    agent_url: String,
}

impl SimulationFiles {
    fn new(label: &str, seed: u64) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "task-scheduler-{label}-{}-{seed:016x}-{}",
            std::process::id(),
            Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).expect("create simulation directory");
        let coordinator_url = sqlite_url(directory.join("coordinator.sqlite"));
        let agent_url = sqlite_url(directory.join("agent.sqlite"));
        Self {
            directory,
            coordinator_url,
            agent_url,
        }
    }
}

impl Drop for SimulationFiles {
    fn drop(&mut self) {
        // This directory is generated above and contains only simulation databases.
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// Exercises real coordinator Store and agent Ledger persistence across a broad
/// matrix of deterministic duplicate/loss/restart schedules.
#[tokio::test]
async fn seeded_crash_replay_preserves_every_accepted_result_and_diagnostic() {
    let config = SimConfig::from_env();
    eprintln!(
        "protocol simulation seed=0x{:016X} cases={}",
        config.seed, config.cases
    );
    let files = SimulationFiles::new("protocol-sim", config.seed);
    let mut coordinator = Store::connect(&files.coordinator_url, None)
        .await
        .expect("connect coordinator store");
    let mut agent = Ledger::connect(&files.agent_url)
        .await
        .expect("connect agent ledger");
    let schedule_id = create_schedule(&coordinator, config.seed).await;
    let mut random = DeterministicRandom::new(config.seed);

    for case_index in 0..config.cases {
        let plan = CasePlan::generate(&mut random);
        let run_id = deterministic_uuid(config.seed, case_index as u64, 1);
        coordinator
            .create_run(NewRun {
                id: run_id,
                schedule_id,
                trigger_kind: "simulation".into(),
                scheduled_at: Utc::now(),
                encrypted_snapshot: vec![case_index as u8],
                key_id: "simulation-key".into(),
                max_attempts: 1,
                initial_backoff_seconds: 0,
                backoff_cap_seconds: 0,
                idempotency_key: Some(format!("sim-{case_index}")),
            })
            .await
            .unwrap_or_else(|error| panic!("case {case_index}: create run: {error:#}"));
        let attempt = coordinator
            .create_attempt(run_id, "simulated-node", 60)
            .await
            .unwrap_or_else(|error| panic!("case {case_index}: offer: {error:#}"))
            .expect("queued run must produce an attempt");
        let assignment = assignment(schedule_id, run_id, &attempt);
        let assignment_json = serde_json::to_string(&assignment).expect("serialize assignment");

        assert!(
            agent
                .accept(&assignment, &assignment_json)
                .await
                .unwrap_or_else(|error| panic!("case {case_index}: durable accept: {error:#}")),
            "case {case_index}: first delivery must be durably new"
        );
        if plan.duplicate_offer {
            assert!(
                !agent
                    .accept(&assignment, &assignment_json)
                    .await
                    .unwrap_or_else(|error| panic!(
                        "case {case_index}: duplicate offer: {error:#}"
                    )),
                "case {case_index}: duplicate delivery must not execute twice"
            );
        }
        if plan.restart_after_accept {
            drop(agent);
            agent = Ledger::connect(&files.agent_url)
                .await
                .unwrap_or_else(|error| {
                    panic!("case {case_index}: reopen accepted ledger: {error:#}")
                });
        }

        coordinator
            .accept_attempt(attempt.id, &attempt.lease_token, 60)
            .await
            .unwrap_or_else(|error| panic!("case {case_index}: coordinator acceptance: {error:#}"));
        if plan.duplicate_acceptance {
            coordinator
                .accept_attempt(attempt.id, &attempt.lease_token, 60)
                .await
                .unwrap_or_else(|error| {
                    panic!("case {case_index}: duplicate coordinator acceptance: {error:#}")
                });
        }

        if plan.restart_after_accept {
            assert!(
                coordinator
                    .reauthorize_attempt(attempt.id, "simulated-node", &attempt.lease_token, 60,)
                    .await
                    .unwrap_or_else(|error| panic!(
                        "case {case_index}: authoritative resume validation: {error:#}"
                    )),
                "case {case_index}: live accepted lease must be resumable"
            );
            assert!(
                agent
                    .claim(&assignment, &assignment_json)
                    .await
                    .unwrap_or_else(|error| panic!(
                        "case {case_index}: claim granted recovery: {error:#}"
                    ))
            );
            assert!(
                agent
                    .start(&attempt.id.to_string())
                    .await
                    .unwrap_or_else(|error| panic!(
                        "case {case_index}: start granted recovery: {error:#}"
                    ))
            );
        }

        let expected = simulated_result(plan.result_kind, case_index as u32);
        let result_json = serde_json::to_string(&expected).expect("serialize result");
        agent
            .save_result(&attempt.id.to_string(), &result_json)
            .await
            .unwrap_or_else(|error| panic!("case {case_index}: save result: {error:#}"));
        if plan.duplicate_local_result {
            agent
                .save_result(&attempt.id.to_string(), &result_json)
                .await
                .unwrap_or_else(|error| panic!("case {case_index}: duplicate result: {error:#}"));
        }
        if plan.restart_before_result_delivery {
            drop(agent);
            agent = Ledger::connect(&files.agent_url)
                .await
                .unwrap_or_else(|error| {
                    panic!("case {case_index}: reopen result outbox: {error:#}")
                });
        }
        let pending = agent
            .result(&attempt.id.to_string())
            .await
            .unwrap_or_else(|error| panic!("case {case_index}: read result outbox: {error:#}"))
            .expect("accepted execution result must survive until acknowledgement");
        assert_eq!(pending.lease_token, attempt.lease_token);
        assert_eq!(pending.result_json, result_json);

        if plan.restart_coordinator_before_result {
            drop(coordinator);
            coordinator = Store::connect(&files.coordinator_url, None)
                .await
                .unwrap_or_else(|error| panic!("case {case_index}: reopen coordinator: {error:#}"));
        }
        assert!(
            coordinator
                .finish_attempt(
                    attempt.id,
                    "intentionally-wrong-lease-token",
                    &expected,
                    vec![0xFF],
                    "simulation-key",
                )
                .await
                .is_err(),
            "case {case_index}: a stale or forged lease token must never finish work"
        );
        let expected_run_state = terminal_state(&expected);
        let first_state = coordinator
            .finish_attempt(
                attempt.id,
                &pending.lease_token,
                &expected,
                result_json.as_bytes().to_vec(),
                "simulation-key",
            )
            .await
            .unwrap_or_else(|error| panic!("case {case_index}: finish attempt: {error:#}"));
        assert_eq!(first_state, expected_run_state);

        if plan.restart_coordinator_after_result {
            drop(coordinator);
            coordinator = Store::connect(&files.coordinator_url, None)
                .await
                .unwrap_or_else(|error| {
                    panic!("case {case_index}: reopen finished coordinator: {error:#}")
                });
        }
        if plan.lose_first_result_ack {
            let conflicting_replay = conflicting_result(&expected);
            let replay_state = coordinator
                .finish_attempt(
                    attempt.id,
                    &pending.lease_token,
                    &conflicting_replay,
                    vec![0xBA, 0xD0],
                    "simulation-key",
                )
                .await
                .unwrap_or_else(|error| panic!("case {case_index}: replay result: {error:#}"));
            assert_eq!(
                replay_state, expected_run_state,
                "case {case_index}: a replay must not revise the terminal run"
            );
        }

        agent
            .acknowledge(&attempt.id.to_string())
            .await
            .unwrap_or_else(|error| panic!("case {case_index}: acknowledge result: {error:#}"));
        agent
            .acknowledge(&attempt.id.to_string())
            .await
            .unwrap_or_else(|error| panic!("case {case_index}: duplicate ack: {error:#}"));
        assert!(
            agent
                .result(&attempt.id.to_string())
                .await
                .unwrap_or_else(|error| panic!(
                    "case {case_index}: check drained result: {error:#}"
                ))
                .is_none(),
            "case {case_index}: acknowledged output must leave the resend outbox"
        );

        assert_persisted_result(
            &coordinator,
            run_id,
            &expected,
            expected_run_state,
            case_index,
        )
        .await;
    }
}

/// Active regression for the acceptance/start crash window. This follows the
/// production ordering rather than the simulation-only `accept` convenience:
/// record, coordinator acceptance, restart, resume grant, fenced claim, start.
#[tokio::test]
async fn accepted_assignment_is_recovered_after_agent_restart_before_executor_spawn() {
    let config = SimConfig::from_env();
    let files = SimulationFiles::new("accept-start-gap", config.seed);
    let coordinator = Store::connect(&files.coordinator_url, None)
        .await
        .expect("coordinator");
    let schedule_id = create_schedule(&coordinator, config.seed).await;
    let run_id = deterministic_uuid(config.seed, 10, 1);
    coordinator
        .create_run(NewRun {
            id: run_id,
            schedule_id,
            trigger_kind: "recovery-regression".into(),
            scheduled_at: Utc::now(),
            encrypted_snapshot: vec![1],
            key_id: "simulation-key".into(),
            max_attempts: 1,
            initial_backoff_seconds: 0,
            backoff_cap_seconds: 0,
            idempotency_key: None,
        })
        .await
        .expect("run");
    let attempt = coordinator
        .create_attempt(run_id, "simulated-node", 60)
        .await
        .expect("offer")
        .expect("attempt");
    let assignment = assignment(schedule_id, run_id, &attempt);
    let assignment_json = serde_json::to_string(&assignment).expect("serialize assignment");

    let ledger = Ledger::connect(&files.agent_url)
        .await
        .expect("connect initial ledger");
    assert!(
        ledger
            .record(&assignment, &assignment_json)
            .await
            .expect("persist acceptance")
    );
    coordinator
        .accept_attempt(attempt.id, &attempt.lease_token, 60)
        .await
        .expect("coordinator acceptance");
    drop(ledger); // crash before `execute_assignment` is spawned

    let restarted = Ledger::connect(&files.agent_url)
        .await
        .expect("restart agent ledger");
    assert_eq!(
        restarted
            .recoverable_assignments()
            .await
            .expect("scan")
            .len(),
        1
    );
    assert!(
        coordinator
            .reauthorize_attempt(attempt.id, "simulated-node", &attempt.lease_token, 60)
            .await
            .expect("resume validation")
    );
    assert!(
        restarted
            .claim(&assignment, &assignment_json)
            .await
            .expect("claim granted recovery")
    );
    let should_spawn_after_grant = restarted
        .start(&attempt.id.to_string())
        .await
        .expect("start recovered assignment");
    assert!(
        should_spawn_after_grant,
        "seed=0x{:016X}: durable accepted work must be resumed after restart; otherwise a max_attempts=1 run can fail lease expiry without ever invoking the task",
        config.seed
    );
}

async fn create_schedule(store: &Store, seed: u64) -> Uuid {
    let id = deterministic_uuid(seed, 0, 0);
    store
        .create_schedule(NewSchedule {
            id,
            spec: ScheduleSpec {
                name: "protocol simulation".into(),
                blueprint_ref: ArtifactRef {
                    uri: "file:///simulation-blueprint".into(),
                },
                parameters_ref: ArtifactRef {
                    uri: "file:///simulation-parameters".into(),
                },
                parameter_collection: None,
                observability: Default::default(),
                required_labels: BTreeMap::new(),
                cron: None,
                webhook_enabled: false,
                enabled: true,
            },
            encrypted_snapshot: vec![0x51],
            snapshot_digest: "simulation-snapshot".into(),
            key_id: "simulation-key".into(),
            webhook_public_id: None,
            webhook_secret_hash: None,
        })
        .await
        .expect("create simulation schedule");
    id
}

fn assignment(
    schedule_id: Uuid,
    run_id: Uuid,
    attempt: &scheduler_store::AttemptRecord,
) -> ExecutionAssignment {
    ExecutionAssignment {
        schedule_id,
        run_id,
        attempt_id: attempt.id,
        attempt_number: attempt.attempt_number,
        lease_token: attempt.lease_token.clone(),
        lease_seconds: 60,
        snapshot: execution_snapshot(),
        traceparent: None,
        tracestate: None,
    }
}

fn execution_snapshot() -> ExecutionSnapshot {
    let blueprint = Blueprint {
        api_version: "scheduler/v1".into(),
        executor: ExecutorSpec::Command(CommandSpec {
            program: "simulation-command".into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            working_directory: None,
        }),
        parameters_schema: serde_json::json!({"type": "object"}),
        parameter_bindings: BTreeMap::new(),
        required_labels: BTreeMap::new(),
        policy: ExecutionPolicy {
            max_attempts: 1,
            timeout_seconds: 60,
            initial_backoff_seconds: 0,
            backoff_cap_seconds: 0,
        },
    };
    ExecutionSnapshot {
        executor: blueprint.executor,
        policy: blueprint.policy,
        required_labels: blueprint.required_labels,
        blueprint_digest: "simulation-blueprint".into(),
        parameters_digest: "simulation-parameters".into(),
        parameters: Some(serde_json::json!({})),
        sensitive_parameter_paths: Vec::new(),
        late_bindings: None,
    }
}

fn simulated_result(kind: ResultKind, case_index: u32) -> ExecutionResult {
    let started_at = Utc::now();
    let finished_at = started_at + Duration::milliseconds(i64::from(case_index % 37) + 1);
    let (outcome, exit_code, signal, diagnostic) = match kind {
        ResultKind::Success => (ExecutionOutcome::Succeeded, Some(0), None, None),
        ResultKind::CommandExit => (
            ExecutionOutcome::Failed,
            Some(23),
            None,
            Some(
                FailureDiagnostic::new(
                    FailureCode::ProcessExitedNonZero,
                    FailureOrigin::CommandProcess,
                    FailureStage::Execution,
                    "command process returned a non-zero status code",
                    false,
                )
                .with_status(status(case_index, Some(23), None, None, None)),
            ),
        ),
        ResultKind::CommandCrash => (
            ExecutionOutcome::Failed,
            None,
            Some("11".into()),
            Some(
                FailureDiagnostic::new(
                    FailureCode::ProcessCrashed,
                    FailureOrigin::CommandProcess,
                    FailureStage::Execution,
                    "command process crashed or was terminated by the operating system",
                    true,
                )
                .with_status(status(case_index, None, Some("11"), None, None)),
            ),
        ),
        ResultKind::ExecutorCrash => (
            ExecutionOutcome::InfrastructureError,
            None,
            Some("9".into()),
            Some(
                FailureDiagnostic::new(
                    FailureCode::ExecutorProcessCrashed,
                    FailureOrigin::TaskExecutor,
                    FailureStage::Execution,
                    "task-executor process crashed before returning a result",
                    true,
                )
                .with_status(status(case_index, None, Some("9"), None, None)),
            ),
        ),
        ResultKind::ExcelMacroFailure => (
            ExecutionOutcome::Failed,
            Some(1),
            None,
            Some(
                FailureDiagnostic::new(
                    FailureCode::ExcelMacroReturnedFailure,
                    FailureOrigin::ExcelMacro,
                    FailureStage::MacroResult,
                    "Excel macro returned 1",
                    false,
                )
                .with_status(status(case_index, Some(1), None, None, None)),
            ),
        ),
        ResultKind::ExcelComFailure => (
            ExecutionOutcome::InfrastructureError,
            Some(3),
            None,
            Some(
                FailureDiagnostic::new(
                    FailureCode::ExcelMacroFailed,
                    FailureOrigin::ExcelMacro,
                    FailureStage::MacroInvoke,
                    "Excel reported a VBA or COM error while invoking the macro",
                    true,
                )
                .with_status(status(
                    case_index,
                    Some(3),
                    None,
                    Some(-2_147_354_523),
                    Some("0x80020009"),
                )),
            ),
        ),
    };
    ExecutionResult {
        outcome,
        exit_code,
        signal,
        stdout: format!("simulated stdout {case_index}"),
        stderr: format!("simulated stderr {case_index}"),
        started_at,
        finished_at,
        error: diagnostic
            .as_ref()
            .map(|_| "encrypted operator detail".into()),
        output: OutputMetadata {
            stdout_bytes: u64::from(case_index) + 10,
            stderr_bytes: u64::from(case_index) + 20,
            stdout_truncated: case_index.is_multiple_of(7),
            stderr_truncated: case_index.is_multiple_of(11),
        },
        diagnostic,
    }
}

fn status(
    case_index: u32,
    status_code: Option<i32>,
    signal: Option<&str>,
    hresult: Option<i64>,
    hresult_hex: Option<&str>,
) -> FailureStatus {
    FailureStatus {
        process_id: Some(10_000 + case_index),
        status_code: status_code.map(i64::from),
        status_code_hex: status_code.map(|code| format!("0x{:08X}", code as u32)),
        signal: signal.map(str::to_owned),
        hresult,
        hresult_hex: hresult_hex.map(str::to_owned),
    }
}

fn conflicting_result(first: &ExecutionResult) -> ExecutionResult {
    let mut conflicting = first.clone();
    conflicting.outcome = if first.outcome == ExecutionOutcome::Succeeded {
        ExecutionOutcome::Failed
    } else {
        ExecutionOutcome::Succeeded
    };
    conflicting.exit_code = Some(99);
    conflicting.signal = None;
    conflicting.diagnostic = None;
    conflicting.stdout = "conflicting replay must not win".into();
    conflicting
}

fn terminal_state(result: &ExecutionResult) -> RunState {
    if result.outcome == ExecutionOutcome::Succeeded {
        RunState::Succeeded
    } else {
        RunState::Failed
    }
}

async fn assert_persisted_result(
    coordinator: &Store,
    run_id: Uuid,
    expected: &ExecutionResult,
    expected_state: RunState,
    case_index: usize,
) {
    let run = coordinator
        .get_run(run_id)
        .await
        .unwrap_or_else(|error| panic!("case {case_index}: query run: {error:#}"))
        .expect("run must remain durable");
    assert_eq!(run.state, expected_state, "case {case_index}: run state");
    assert_eq!(run.attempt_count, 1, "case {case_index}: retry budget");

    let attempts = coordinator
        .run_attempts(run_id)
        .await
        .unwrap_or_else(|error| panic!("case {case_index}: query attempt: {error:#}"));
    assert_eq!(attempts.len(), 1, "case {case_index}: no duplicate attempt");
    let persisted = &attempts[0];
    assert_eq!(
        persisted.state, "finished",
        "case {case_index}: attempt state"
    );
    assert_eq!(
        persisted.outcome.as_deref(),
        Some(expected.outcome.as_str())
    );
    assert_eq!(persisted.exit_code, expected.exit_code);
    assert_eq!(persisted.signal, expected.signal);
    assert_eq!(persisted.diagnostic, expected.diagnostic);
    assert_eq!(persisted.output.as_ref(), Some(&expected.output));
    assert_eq!(
        persisted.duration_ms,
        Some(
            (expected.finished_at - expected.started_at)
                .num_milliseconds()
                .max(0) as u64
        )
    );
}

fn deterministic_uuid(seed: u64, case_index: u64, discriminator: u64) -> Uuid {
    Uuid::from_u128(
        (u128::from(seed) << 64)
            | u128::from(case_index.wrapping_mul(31).wrapping_add(discriminator)),
    )
}

fn sqlite_url(path: PathBuf) -> String {
    format!("sqlite://{}", path.display())
}

fn parse_seed(value: &str) -> Option<u64> {
    let value = value.trim();
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .map_or_else(
            || value.parse().ok(),
            |hex| u64::from_str_radix(hex, 16).ok(),
        )
}

#[test]
fn seed_parser_supports_replay_friendly_decimal_and_hex() {
    assert_eq!(parse_seed("42"), Some(42));
    assert_eq!(parse_seed("0x2A"), Some(42));
    assert_eq!(parse_seed("0X2a"), Some(42));
    assert_eq!(parse_seed("not-a-seed"), None);
}
