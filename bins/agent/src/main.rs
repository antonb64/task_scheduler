mod config;
mod ledger;
mod proxy;

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::Parser;
use config::Config;
use futures::StreamExt;
use ledger::Ledger;
use scheduler_core::{
    ExecutionAssignment, ExecutionOutcome, ExecutionResult, ExecutorSpec, FailureCode,
    FailureDiagnostic, FailureOrigin, FailureStage, FailureStatus, NodeSettings, OutputMetadata,
};
use scheduler_protocol::control::{
    AgentHello, AgentMessage, AttemptAccepted, AttemptResult, Heartbeat, ResumeAttempt,
    SettingsApplied, agent_message, coordinator_message,
    scheduler_control_client::SchedulerControlClient,
};
use tokio::{
    io::AsyncWriteExt,
    process::Command,
    sync::{RwLock, mpsc, watch},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tracing::{error, info, warn};

#[derive(Clone)]
struct Runtime {
    config: Config,
    ledger: Ledger,
    settings: Arc<RwLock<NodeSettings>>,
    active: Arc<RwLock<HashMap<String, ActiveTask>>>,
    recovering: Arc<RwLock<HashSet<String>>>,
    outgoing: Arc<RwLock<Option<mpsc::Sender<AgentMessage>>>>,
    connected: watch::Sender<bool>,
    heartbeat_seconds: watch::Sender<u64>,
}

#[derive(Clone)]
struct ActiveTask {
    cancel: watch::Sender<bool>,
    lease_freshness: watch::Sender<u64>,
    excel: bool,
    started: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    scheduler_core::validate_agent_id(&config.agent_id)
        .context("invalid SCHEDULER_AGENT_ID/--agent-id")?;
    let _telemetry = scheduler_telemetry::init("scheduler-agent", config.otlp_endpoint.as_deref())?;
    let ledger = Ledger::connect(&config.database_url).await?;
    let (connected, _) = watch::channel(false);
    let (heartbeat_seconds, _) = watch::channel(10);
    let initial_settings = match ledger.load_settings().await? {
        Some((revision, json)) => {
            let mut settings: NodeSettings =
                serde_json::from_str(&json).context("persisted node settings are invalid")?;
            settings.revision = revision;
            validate_settings(&settings)?;
            settings
        }
        None => NodeSettings {
            max_parallel: config.capacity,
            ..NodeSettings::default()
        },
    };
    let runtime = Runtime {
        config: config.clone(),
        ledger,
        settings: Arc::new(RwLock::new(initial_settings)),
        active: Arc::new(RwLock::new(HashMap::new())),
        recovering: Arc::new(RwLock::new(HashSet::new())),
        outgoing: Arc::new(RwLock::new(None)),
        connected,
        heartbeat_seconds,
    };

    let proxy_client = Arc::new(RwLock::new(None));
    let ui = proxy::router(proxy::ProxyState {
        client: proxy_client.clone(),
    });
    let ui_addr = config.ui_addr;
    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(ui_addr).await {
            Ok(listener) => {
                info!(%ui_addr, "agent management UI proxy listening");
                if let Err(error) = axum::serve(listener, ui).await {
                    error!(%error, "agent UI stopped");
                }
            }
            Err(error) => error!(%error, "agent UI failed to bind"),
        }
    });

    run_connections(runtime, proxy_client).await
}

async fn run_connections(
    runtime: Runtime,
    proxy_client: Arc<RwLock<Option<SchedulerControlClient<Channel>>>>,
) -> Result<()> {
    let mut backoff = 1_u64;
    loop {
        match connect_channel(&runtime.config).await {
            Ok(channel) => {
                let client = SchedulerControlClient::new(channel);
                *proxy_client.write().await = Some(client.clone());
                match run_stream(&runtime, client).await {
                    Ok(()) => warn!("coordinator closed the agent stream"),
                    Err(error) => warn!(%error, "agent stream failed"),
                }
                *proxy_client.write().await = None;
                *runtime.outgoing.write().await = None;
                publish_connection_state(&runtime.connected, false);
                backoff = 1;
            }
            Err(error) => warn!(%error, retry_seconds = backoff, "cannot connect to coordinator"),
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

async fn run_stream(runtime: &Runtime, mut client: SchedulerControlClient<Channel>) -> Result<()> {
    let (outgoing_tx, outgoing_rx) = mpsc::channel(256);
    *runtime.outgoing.write().await = Some(outgoing_tx.clone());
    let labels = runtime.config.labels()?;
    let recoverable = runtime.ledger.recoverable_assignments().await?;
    {
        let mut recovering = runtime.recovering.write().await;
        for row in &recoverable {
            let assignment: ExecutionAssignment = serde_json::from_str(&row.assignment_json)?;
            recovering.insert(assignment.attempt_id.to_string());
        }
    }
    let hostname = hostname::get()?.to_string_lossy().into_owned();
    outgoing_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Hello(AgentHello {
                agent_id: runtime.config.agent_id.clone(),
                hostname,
                labels,
                capacity: runtime.config.capacity,
                running: recoverable.len() as u32,
                version: env!("CARGO_PKG_VERSION").into(),
            })),
        })
        .await?;
    let mut incoming = client
        .agent_stream(ReceiverStream::new(outgoing_rx))
        .await?
        .into_inner();
    // `send` discards the new value when no receiver exists yet. The first task
    // subscribes only after connecting, so retain connection state explicitly or
    // its executor will never receive keepalives and will expire a healthy lease.
    publish_connection_state(&runtime.connected, true);
    info!(
        agent_id = runtime.config.agent_id,
        "connected to coordinator"
    );
    // The coordinator's first response is authoritative settings. Recovery is
    // deliberately held until it is received, validated, and persisted.
    loop {
        let message = incoming
            .next()
            .await
            .context("coordinator closed before initial settings")??;
        let valid_settings = match &message.payload {
            Some(coordinator_message::Payload::Settings(update))
                if update.heartbeat_seconds > 0 =>
            {
                serde_json::from_str::<NodeSettings>(&update.settings_json)
                    .ok()
                    .is_some_and(|settings| validate_settings(&settings).is_ok())
            }
            _ => false,
        };
        handle_coordinator_message(runtime, message).await?;
        if valid_settings {
            break;
        }
    }
    send_pending(runtime, &outgoing_tx).await?;
    request_recovery(runtime, &outgoing_tx, recoverable).await?;
    let mut heartbeat_seconds = runtime.heartbeat_seconds.subscribe();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(*heartbeat_seconds.borrow()));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let active = runtime.active.read().await.keys().cloned().collect::<Vec<_>>();
                let reserved = runtime.recovering.read().await.len() as u32;
                outgoing_tx.send(AgentMessage { payload: Some(agent_message::Payload::Heartbeat(Heartbeat {
                    agent_id: runtime.config.agent_id.clone(),
                    running: active.len() as u32 + reserved,
                    active_attempt_ids: active,
                })) }).await?;
                send_pending(runtime, &outgoing_tx).await?;
            }
            changed = heartbeat_seconds.changed() => {
                changed?;
                heartbeat = tokio::time::interval(Duration::from_secs(*heartbeat_seconds.borrow()));
            }
            message = incoming.next() => {
                let Some(message) = message else { break; };
                handle_coordinator_message(runtime, message?).await?;
            }
        }
    }
    Ok(())
}

fn publish_connection_state(sender: &watch::Sender<bool>, connected: bool) {
    sender.send_replace(connected);
}

async fn handle_coordinator_message(
    runtime: &Runtime,
    message: scheduler_protocol::control::CoordinatorMessage,
) -> Result<()> {
    match message.payload {
        Some(coordinator_message::Payload::Assignment(offer)) => {
            let assignment: ExecutionAssignment = serde_json::from_str(&offer.assignment_json)?;
            if runtime
                .ledger
                .state(&assignment.attempt_id.to_string())
                .await?
                .as_deref()
                == Some("acknowledged")
            {
                return Ok(());
            }
            if let Some(pending) = runtime
                .ledger
                .result(&assignment.attempt_id.to_string())
                .await?
            {
                send_result(runtime, pending).await?;
                return Ok(());
            }
            runtime
                .ledger
                .record(&assignment, &offer.assignment_json)
                .await?;
            if matches!(
                runtime
                    .ledger
                    .state(&assignment.attempt_id.to_string())
                    .await?
                    .as_deref(),
                Some("cancelled" | "quarantined")
            ) {
                return Ok(());
            }
            send(
                runtime,
                AgentMessage {
                    payload: Some(agent_message::Payload::Accepted(AttemptAccepted {
                        agent_id: runtime.config.agent_id.clone(),
                        attempt_id: assignment.attempt_id.to_string(),
                        lease_token: assignment.lease_token.clone(),
                    })),
                },
            )
            .await?;
            if runtime
                .ledger
                .claim(&assignment, &offer.assignment_json)
                .await?
            {
                let runtime = runtime.clone();
                tokio::spawn(async move {
                    execute_assignment(runtime, assignment).await;
                });
            }
        }
        Some(coordinator_message::Payload::Cancel(cancel)) => {
            runtime.ledger.cancel(&cancel.attempt_id).await?;
            runtime.recovering.write().await.remove(&cancel.attempt_id);
            if let Some(control) = runtime.active.read().await.get(&cancel.attempt_id) {
                let _ = control.cancel.send(true);
            }
        }
        Some(coordinator_message::Payload::Settings(update)) => {
            if update.heartbeat_seconds == 0 {
                bail!("coordinator supplied an invalid zero heartbeat interval");
            }
            let mut settings: NodeSettings = serde_json::from_str(&update.settings_json)?;
            let validation = validate_settings(&settings);
            let error = validation.as_ref().err().map(|error| format!("{error:#}"));
            if validation.is_ok() {
                settings.revision = update.revision;
                runtime
                    .ledger
                    .save_settings(update.revision, &update.settings_json)
                    .await?;
                *runtime.settings.write().await = settings;
                let _ = runtime.heartbeat_seconds.send(update.heartbeat_seconds);
            }
            send(
                runtime,
                AgentMessage {
                    payload: Some(agent_message::Payload::SettingsApplied(SettingsApplied {
                        agent_id: runtime.config.agent_id.clone(),
                        revision: update.revision,
                        error,
                    })),
                },
            )
            .await?;
        }
        Some(coordinator_message::Payload::ResultAcknowledged(ack)) => {
            runtime.ledger.acknowledge(&ack.attempt_id).await?;
        }
        Some(coordinator_message::Payload::ResumeDecision(decision)) => {
            if !decision.granted {
                runtime
                    .recovering
                    .write()
                    .await
                    .remove(&decision.attempt_id);
                runtime.ledger.cancel(&decision.attempt_id).await?;
                warn!(
                    attempt_id = decision.attempt_id,
                    reason = decision.reason.as_deref().unwrap_or("not authorized"),
                    "discarded stale recovered assignment"
                );
                return Ok(());
            }
            let Some(json) = runtime
                .ledger
                .assignment(&decision.attempt_id, &decision.lease_token)
                .await?
            else {
                runtime
                    .recovering
                    .write()
                    .await
                    .remove(&decision.attempt_id);
                warn!(
                    attempt_id = decision.attempt_id,
                    "ignored resume grant for an assignment no longer recoverable locally"
                );
                return Ok(());
            };
            let assignment: ExecutionAssignment = serde_json::from_str(&json)?;
            if runtime.ledger.claim(&assignment, &json).await? {
                let runtime = runtime.clone();
                tokio::spawn(async move { execute_assignment(runtime, assignment).await });
            } else {
                runtime
                    .recovering
                    .write()
                    .await
                    .remove(&decision.attempt_id);
            }
        }
        Some(coordinator_message::Payload::HeartbeatAcknowledged(ack)) => {
            let active = runtime.active.read().await;
            for attempt_id in ack.renewed_attempt_ids {
                if let Some(task) = active.get(&attempt_id) {
                    let next = (*task.lease_freshness.borrow()).wrapping_add(1);
                    task.lease_freshness.send_replace(next);
                }
            }
        }
        None => bail!("empty coordinator message"),
    }
    Ok(())
}

async fn execute_assignment(runtime: Runtime, assignment: ExecutionAssignment) {
    let attempt_id = assignment.attempt_id.to_string();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (lease_tx, lease_rx) = watch::channel(0_u64);
    let is_excel = matches!(&assignment.snapshot.executor, ExecutorSpec::ExcelMacro(_));
    {
        let mut active = runtime.active.write().await;
        active.insert(
            attempt_id.clone(),
            ActiveTask {
                cancel: cancel_tx,
                lease_freshness: lease_tx,
                excel: is_excel,
                started: false,
            },
        );
    }
    runtime.recovering.write().await.remove(&attempt_id);

    let mut settings = runtime.settings.read().await.clone();
    let policy_error = validate_assignment(&settings, &assignment).err();
    let result = if let Some(error) = policy_error {
        if !runtime.ledger.start(&attempt_id).await.unwrap_or(false) {
            runtime.active.write().await.remove(&attempt_id);
            return;
        }
        agent_failure_result(
            error,
            FailureCode::AssignmentRejected,
            FailureStage::Validation,
            "assignment was rejected by the agent's execution policy",
            false,
        )
    } else {
        // Accepted recovery work is a durable reservation, not a placement
        // failure. It waits within capacity and is included in heartbeats so
        // fresh offers cannot overbook this node.
        loop {
            if *cancel_rx.borrow() {
                runtime.active.write().await.remove(&attempt_id);
                return;
            }
            settings = runtime.settings.read().await.clone();
            if let Err(error) = validate_assignment(&settings, &assignment) {
                if !runtime.ledger.start(&attempt_id).await.unwrap_or(false) {
                    runtime.active.write().await.remove(&attempt_id);
                    return;
                }
                break agent_failure_result(
                    error,
                    FailureCode::AssignmentRejected,
                    FailureStage::Validation,
                    "assignment was rejected by updated node settings",
                    false,
                );
            }
            let acquired = {
                let mut active = runtime.active.write().await;
                if capacity_available(&active, &settings, is_excel) {
                    if let Some(task) = active.get_mut(&attempt_id) {
                        task.started = true;
                    }
                    true
                } else {
                    false
                }
            };
            if acquired {
                if !runtime.ledger.start(&attempt_id).await.unwrap_or(false) {
                    runtime.active.write().await.remove(&attempt_id);
                    return;
                }
                break match run_executor(&runtime, &assignment, &cancel_rx, lease_rx).await {
                    Ok(result) => result,
                    Err(error) => {
                        let detail = format!("{error:#}");
                        let (code, stage, summary) = if detail.contains("cannot start executor") {
                            (
                                FailureCode::ExecutorStartFailed,
                                FailureStage::ExecutorStart,
                                "agent could not start the task-executor process",
                            )
                        } else {
                            (
                                FailureCode::ExecutorProtocolError,
                                FailureStage::ResultDecode,
                                "agent could not communicate with the task-executor process",
                            )
                        };
                        agent_failure_result(error, code, stage, summary, true)
                    }
                };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    export_task_observability(&assignment, &result);
    match serde_json::to_string(&result) {
        Ok(json) => {
            let mut retry = Duration::from_millis(100);
            loop {
                match runtime.ledger.save_result(&attempt_id, &json).await {
                    Ok(()) => break,
                    Err(error) => {
                        error!(%error, %attempt_id, "failed to persist task result; retrying before releasing capacity");
                        tokio::time::sleep(retry).await;
                        retry = (retry * 2).min(Duration::from_secs(5));
                    }
                }
            }
            runtime.active.write().await.remove(&attempt_id);
            if let Err(error) = send(
                &runtime,
                AgentMessage {
                    payload: Some(agent_message::Payload::Result(AttemptResult {
                        agent_id: runtime.config.agent_id.clone(),
                        attempt_id: attempt_id.clone(),
                        lease_token: assignment.lease_token,
                        result_json: json,
                    })),
                },
            )
            .await
            {
                warn!(%error, %attempt_id, "result retained for reconnect");
            }
        }
        Err(error) => {
            error!(%error, %attempt_id, "failed to serialize task result");
            runtime.active.write().await.remove(&attempt_id);
        }
    }
}

fn capacity_available(
    active: &HashMap<String, ActiveTask>,
    settings: &NodeSettings,
    is_excel: bool,
) -> bool {
    let running = active.values().filter(|task| task.started).count() as u32;
    let excel_running = active
        .values()
        .filter(|task| task.started && task.excel)
        .count() as u32;
    running < settings.max_parallel && (!is_excel || excel_running < settings.excel_max_parallel)
}

fn export_task_observability(assignment: &ExecutionAssignment, result: &ExecutionResult) {
    use opentelemetry::KeyValue;
    let meter = opentelemetry::global::meter("scheduler-agent");
    meter.u64_counter("scheduler.task.completions").build().add(
        1,
        &[
            KeyValue::new(
                "outcome",
                format!("{:?}", result.outcome).to_ascii_lowercase(),
            ),
            KeyValue::new(
                "executor",
                match &assignment.snapshot.executor {
                    ExecutorSpec::Command(_) => "command",
                    ExecutorSpec::ExcelMacro(_) => "excel_macro",
                },
            ),
        ],
    );
    if let Some(diagnostic) = &result.diagnostic {
        tracing::warn!(
            run_id = %assignment.run_id,
            attempt_id = %assignment.attempt_id,
            failure_code = ?diagnostic.code,
            failure_origin = ?diagnostic.origin,
            failure_stage = ?diagnostic.stage,
            retryable = diagnostic.retryable,
            "task attempt produced a diagnostic"
        );
        meter.u64_counter("scheduler.task.failures").build().add(
            1,
            &[
                KeyValue::new(
                    "code",
                    serde_json::to_value(diagnostic.code)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "unknown".into()),
                ),
                KeyValue::new(
                    "origin",
                    serde_json::to_value(diagnostic.origin)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "unknown".into()),
                ),
                KeyValue::new(
                    "stage",
                    serde_json::to_value(diagnostic.stage)
                        .ok()
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .unwrap_or_else(|| "unknown".into()),
                ),
            ],
        );
    }
    let duration = (result.finished_at - result.started_at)
        .num_milliseconds()
        .max(0) as f64;
    meter
        .f64_histogram("scheduler.task.duration_ms")
        .build()
        .record(duration, &[]);
    for (stream, text) in [("stdout", &result.stdout), ("stderr", &result.stderr)] {
        for (sequence, line) in text.lines().enumerate() {
            let mut end = line.len().min(65_536);
            while !line.is_char_boundary(end) {
                end -= 1;
            }
            let bounded = &line[..end];
            tracing::info!(
                target: "scheduler.task_output",
                run_id = %assignment.run_id,
                attempt_id = %assignment.attempt_id,
                stream,
                sequence,
                message = bounded,
                "task output"
            );
        }
    }
}

async fn run_executor(
    runtime: &Runtime,
    assignment: &ExecutionAssignment,
    cancel: &watch::Receiver<bool>,
    mut lease_freshness: watch::Receiver<u64>,
) -> Result<ExecutionResult> {
    let started_at = Utc::now();
    let mut child = Command::new(&runtime.config.executor_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| {
            format!(
                "cannot start executor {}",
                runtime.config.executor_path.display()
            )
        })?;
    let executor_pid = child.id();
    let mut stdin = child.stdin.take().context("executor stdin unavailable")?;
    stdin
        .write_all(serde_json::to_string(assignment)?.as_bytes())
        .await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    let mut cancel = cancel.clone();
    let controller = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = lease_freshness.changed() => {
                    if changed.is_err() { break; }
                    // Keepalive means the coordinator positively renewed this
                    // exact attempt, not merely that a half-open stream exists.
                    if stdin.write_all(b"{\"keepalive\":true}\n").await.is_err() { break; }
                    let _ = stdin.flush().await;
                }
                changed = cancel.changed() => {
                    if changed.is_err() { break; }
                    if *cancel.borrow() {
                        let _ = stdin.write_all(b"{\"cancel\":true}\n").await;
                        let _ = stdin.flush().await;
                        break;
                    }
                }
            }
        }
    });
    let output = child.wait_with_output().await?;
    controller.abort();
    if !output.status.success() && output.stdout.is_empty() {
        let exit_code = output.status.code();
        let signal = exit_signal(&output.status);
        let crashed = signal.is_some() || exit_code.is_some_and(|code| cfg!(windows) && code < 0);
        let diagnostic = FailureDiagnostic::new(
            if crashed {
                FailureCode::ExecutorProcessCrashed
            } else {
                FailureCode::ExecutorProtocolError
            },
            FailureOrigin::TaskExecutor,
            if crashed {
                FailureStage::Execution
            } else {
                FailureStage::ResultDecode
            },
            if crashed {
                "task-executor process crashed before returning a result"
            } else {
                "task-executor exited without returning a result"
            },
            true,
        )
        .with_status(failure_status(executor_pid, exit_code, signal.clone()));
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        return Ok(ExecutionResult {
            outcome: ExecutionOutcome::InfrastructureError,
            exit_code,
            signal,
            stdout: String::new(),
            stderr: stderr.clone(),
            started_at,
            finished_at: Utc::now(),
            error: Some("task-executor exited before returning a result".into()),
            output: OutputMetadata {
                stdout_bytes: 0,
                stderr_bytes: stderr.len() as u64,
                stdout_truncated: false,
                stderr_truncated: false,
            },
            diagnostic: Some(diagnostic),
        });
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .context("executor returned no result")?;
    match serde_json::from_str(line) {
        Ok(result) => Ok(result),
        Err(error) => {
            let exit_code = output.status.code();
            let signal = exit_signal(&output.status);
            Ok(ExecutionResult {
                outcome: ExecutionOutcome::InfrastructureError,
                exit_code,
                signal: signal.clone(),
                stdout: stdout.into_owned(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                started_at,
                finished_at: Utc::now(),
                error: Some(format!("executor returned invalid result JSON: {error}")),
                output: OutputMetadata {
                    stdout_bytes: output.stdout.len() as u64,
                    stderr_bytes: output.stderr.len() as u64,
                    stdout_truncated: false,
                    stderr_truncated: false,
                },
                diagnostic: Some(
                    FailureDiagnostic::new(
                        FailureCode::ExecutorProtocolError,
                        FailureOrigin::TaskExecutor,
                        FailureStage::ResultDecode,
                        "task-executor returned an invalid result payload",
                        true,
                    )
                    .with_status(failure_status(
                        executor_pid,
                        exit_code,
                        signal,
                    )),
                ),
            })
        }
    }
}

fn agent_failure_result(
    error: anyhow::Error,
    code: FailureCode,
    stage: FailureStage,
    summary: &'static str,
    retryable: bool,
) -> ExecutionResult {
    let now = Utc::now();
    ExecutionResult {
        outcome: ExecutionOutcome::InfrastructureError,
        exit_code: None,
        signal: None,
        stdout: String::new(),
        stderr: String::new(),
        started_at: now,
        finished_at: now,
        error: Some(format!("{error:#}")),
        output: OutputMetadata::default(),
        diagnostic: Some(FailureDiagnostic::new(
            code,
            FailureOrigin::Agent,
            stage,
            summary,
            retryable,
        )),
    }
}

fn failure_status(
    process_id: Option<u32>,
    exit_code: Option<i32>,
    signal: Option<String>,
) -> FailureStatus {
    FailureStatus {
        process_id,
        status_code: exit_code.map(i64::from),
        status_code_hex: exit_code.map(|code| format!("0x{:08X}", code as u32)),
        signal,
        hresult: None,
        hresult_hex: None,
    }
}

#[cfg(unix)]
fn exit_signal(status: &std::process::ExitStatus) -> Option<String> {
    use std::os::unix::process::ExitStatusExt;
    status.signal().map(|signal| signal.to_string())
}

#[cfg(not(unix))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<String> {
    None
}

fn validate_settings(settings: &NodeSettings) -> Result<()> {
    if settings.max_parallel == 0 {
        bail!("max_parallel must be at least one");
    }
    if settings.excel_max_parallel > 1 {
        bail!("excel_max_parallel cannot exceed one");
    }
    for root in settings
        .allowed_command_roots
        .iter()
        .chain(&settings.allowed_workbook_roots)
    {
        if !Path::new(root).is_absolute() {
            bail!("allowlist roots must be absolute");
        }
    }
    Ok(())
}

fn validate_assignment(settings: &NodeSettings, assignment: &ExecutionAssignment) -> Result<()> {
    if !settings.enabled {
        bail!("agent is disabled by node settings");
    }
    if assignment
        .snapshot
        .required_labels
        .iter()
        .any(|(key, value)| settings.labels.get(key) != Some(value))
    {
        bail!("assignment labels no longer match applied node settings");
    }
    match &assignment.snapshot.executor {
        ExecutorSpec::Command(command) => {
            let path = Path::new(&command.program);
            if path.is_absolute() {
                ensure_allowed(path, &settings.allowed_command_roots, "command")?;
            }
            if let Some(directory) = &command.working_directory {
                ensure_allowed(
                    Path::new(directory),
                    &settings.allowed_command_roots,
                    "working directory",
                )?;
            }
        }
        ExecutorSpec::ExcelMacro(excel) => {
            if settings.excel_max_parallel == 0 {
                bail!("Excel execution is disabled by node settings");
            }
            #[cfg(not(windows))]
            {
                let _ = excel;
                bail!("Excel macro was assigned to a non-Windows agent");
            }
            #[cfg(windows)]
            ensure_allowed(
                Path::new(&excel.workbook_path),
                &settings.allowed_workbook_roots,
                "workbook",
            )?;
        }
    }
    Ok(())
}

fn ensure_allowed(path: &Path, roots: &[String], kind: &str) -> Result<()> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("cannot resolve {kind} path {}", path.display()))?;
    let allowed = roots
        .iter()
        .filter_map(|root| PathBuf::from(root).canonicalize().ok())
        .any(|root| canonical.starts_with(root));
    if !allowed {
        bail!(
            "{kind} path {} is outside configured allowlists",
            canonical.display()
        );
    }
    Ok(())
}

async fn send_pending(runtime: &Runtime, tx: &mpsc::Sender<AgentMessage>) -> Result<()> {
    for pending in runtime.ledger.pending_results().await? {
        tx.send(AgentMessage {
            payload: Some(agent_message::Payload::Result(AttemptResult {
                agent_id: runtime.config.agent_id.clone(),
                attempt_id: pending.attempt_id,
                lease_token: pending.lease_token,
                result_json: pending.result_json,
            })),
        })
        .await?;
    }
    Ok(())
}

async fn request_recovery(
    runtime: &Runtime,
    tx: &mpsc::Sender<AgentMessage>,
    recoverable_assignments: Vec<ledger::RecoverableAssignment>,
) -> Result<()> {
    for recoverable in recoverable_assignments {
        let assignment: ExecutionAssignment = serde_json::from_str(&recoverable.assignment_json)
            .context("stored assignment is invalid")?;
        tx.send(AgentMessage {
            payload: Some(agent_message::Payload::Resume(ResumeAttempt {
                agent_id: runtime.config.agent_id.clone(),
                attempt_id: assignment.attempt_id.to_string(),
                lease_token: assignment.lease_token.clone(),
            })),
        })
        .await?;
        info!(
            attempt_id = %assignment.attempt_id,
            run_id = %assignment.run_id,
            "requested authoritative recovery reauthorization"
        );
    }
    Ok(())
}

async fn send_result(runtime: &Runtime, pending: ledger::PendingResult) -> Result<()> {
    send(
        runtime,
        AgentMessage {
            payload: Some(agent_message::Payload::Result(AttemptResult {
                agent_id: runtime.config.agent_id.clone(),
                attempt_id: pending.attempt_id,
                lease_token: pending.lease_token,
                result_json: pending.result_json,
            })),
        },
    )
    .await
}

async fn send(runtime: &Runtime, message: AgentMessage) -> Result<()> {
    let tx = runtime
        .outgoing
        .read()
        .await
        .clone()
        .context("coordinator stream unavailable")?;
    tx.send(message).await.context("coordinator stream closed")
}

async fn connect_channel(config: &Config) -> Result<Channel> {
    let mut endpoint = Endpoint::from_shared(config.coordinator_url.clone())?
        .connect_timeout(Duration::from_secs(10));
    match (&config.tls_ca, &config.tls_cert, &config.tls_key) {
        (Some(ca), Some(cert), Some(key)) => {
            let mut tls = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(tokio::fs::read(ca).await?))
                .identity(Identity::from_pem(
                    tokio::fs::read(cert).await?,
                    tokio::fs::read(key).await?,
                ));
            if let Some(domain) = &config.tls_domain {
                tls = tls.domain_name(domain);
            }
            endpoint = endpoint.tls_config(tls)?;
        }
        (None, None, None) => {}
        _ => bail!("agent TLS requires CA, certificate, and key together"),
    }
    Ok(endpoint.connect().await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_state_is_retained_before_the_first_executor_subscribes() {
        let (sender, receiver) = watch::channel(false);
        drop(receiver);
        publish_connection_state(&sender, true);
        assert!(*sender.subscribe().borrow());
    }

    #[test]
    fn accepted_recovery_reservations_queue_at_capacity_one_instead_of_failing() {
        let (cancel_a, _) = watch::channel(false);
        let (lease_a, _) = watch::channel(0);
        let (cancel_b, _) = watch::channel(false);
        let (lease_b, _) = watch::channel(0);
        let mut active = HashMap::new();
        active.insert(
            "running".into(),
            ActiveTask {
                cancel: cancel_a,
                lease_freshness: lease_a,
                excel: false,
                started: true,
            },
        );
        active.insert(
            "reserved".into(),
            ActiveTask {
                cancel: cancel_b,
                lease_freshness: lease_b,
                excel: false,
                started: false,
            },
        );
        let settings = NodeSettings {
            max_parallel: 1,
            ..NodeSettings::default()
        };
        assert!(!capacity_available(&active, &settings, false));
        active.get_mut("running").expect("running").started = false;
        assert!(capacity_available(&active, &settings, false));
    }
}
