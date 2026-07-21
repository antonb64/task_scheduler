mod config;
mod ledger;
mod parameter_bindings;
mod proxy;

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::Parser;
use config::Config;
use futures::StreamExt;
use ledger::Ledger;
use parameter_bindings::ParameterBindingResolver;
use scheduler_core::{
    ExecutionAssignment, ExecutionOutcome, ExecutionResult, ExecutorSpec, FailureCode,
    FailureDiagnostic, FailureOrigin, FailureStage, FailureStatus, NodeSettings, OutputMetadata,
};
use scheduler_protocol::control::{
    AgentHello, AgentMessage, AttemptAccepted, AttemptRejected, AttemptResult, Heartbeat,
    ResumeAttempt, SettingsApplied, agent_message, coordinator_message,
    scheduler_control_client::SchedulerControlClient,
};
use tokio::{
    io::AsyncWriteExt,
    process::Command,
    sync::{RwLock, mpsc, watch},
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tracing::{error, info, instrument, warn};

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
    parameter_bindings: Arc<ParameterBindingResolver>,
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
    let parameter_bindings = Arc::new(ParameterBindingResolver::new(
        &config.allowed_environment_bindings,
        &config.secret_roots,
        config.binding_max_bytes,
    )?);
    let telemetry = scheduler_telemetry::init("scheduler-agent", config.otlp_endpoint.as_deref())?;
    let telemetry_status = telemetry.status();
    info!(
        configured = telemetry_status.configured,
        protocol = %telemetry_status.protocol,
        "telemetry initialized"
    );
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
        parameter_bindings,
    };

    let proxy_client = Arc::new(RwLock::new(None));
    let ui = proxy::router(proxy::ProxyState {
        client: proxy_client.clone(),
    });
    let ui_addr = config.ui_addr;
    let ui_server: Pin<Box<dyn Future<Output = Result<()>> + Send>> = match (
        config.ui_tls_cert.clone(),
        config.ui_tls_key.clone(),
    ) {
        (Some(cert), Some(key)) => {
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                .await
                .context("agent management UI TLS configuration is invalid")?;
            let listener = std::net::TcpListener::bind(ui_addr)
                .context("agent management UI failed to bind")?;
            listener.set_nonblocking(true)?;
            let server = axum_server::from_tcp_rustls(listener, tls)?;
            Box::pin(async move {
                info!(%ui_addr, "agent HTTPS management UI proxy listening");
                server
                    .serve(ui.into_make_service())
                    .await
                    .context("agent HTTPS management UI stopped")
            })
        }
        (None, None) => {
            let listener = tokio::net::TcpListener::bind(ui_addr)
                .await
                .context("agent management UI failed to bind")?;
            Box::pin(async move {
                info!(%ui_addr, "agent HTTP management UI proxy listening; configure TLS for non-local deployments");
                axum::serve(listener, ui)
                    .await
                    .context("agent HTTP management UI stopped")
            })
        }
        _ => bail!("agent UI TLS requires both certificate and key"),
    };

    tokio::try_join!(run_connections(runtime, proxy_client), ui_server)?;
    Ok(())
}

#[instrument(name = "agent.connection.supervisor", skip_all)]
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

#[instrument(
    name = "agent.control.stream",
    skip_all,
    fields(agent_id = runtime.config.agent_id)
)]
async fn run_stream(runtime: &Runtime, mut client: SchedulerControlClient<Channel>) -> Result<()> {
    let (outgoing_tx, outgoing_rx) = mpsc::channel(256);
    *runtime.outgoing.write().await = Some(outgoing_tx.clone());
    let labels = runtime.config.labels()?;
    let available_bindings = runtime.parameter_bindings.available_bindings().await?;
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
                environment_bindings: available_bindings.environment,
                secret_file_bindings: available_bindings.secret_files,
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
                let active_guard = runtime.active.read().await;
                let active = active_guard.keys().cloned().collect::<Vec<_>>();
                let active_excel = active_guard.values().filter(|task| task.excel).count() as u64;
                drop(active_guard);
                let reserved = runtime.recovering.read().await.len() as u32;
                let settings = runtime.settings.read().await;
                let meter = opentelemetry::global::meter("scheduler-agent");
                let slots = meter.u64_gauge("scheduler.agent.slots").build();
                slots.record(
                    active.len() as u64 + u64::from(reserved),
                    &[opentelemetry::KeyValue::new("state", "active")],
                );
                slots.record(
                    u64::from(settings.max_parallel),
                    &[opentelemetry::KeyValue::new("state", "capacity")],
                );
                let excel_slots = meter.u64_gauge("scheduler.agent.excel_slots").build();
                excel_slots.record(
                    active_excel,
                    &[opentelemetry::KeyValue::new("state", "active")],
                );
                excel_slots.record(
                    u64::from(settings.excel_max_parallel),
                    &[opentelemetry::KeyValue::new("state", "capacity")],
                );
                drop(settings);
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

#[instrument(
    name = "agent.coordinator.message",
    skip_all,
    fields(
        message.kind = tracing::field::Empty,
        run_id = tracing::field::Empty,
        attempt_id = tracing::field::Empty,
        settings.revision = tracing::field::Empty
    )
)]
async fn handle_coordinator_message(
    runtime: &Runtime,
    message: scheduler_protocol::control::CoordinatorMessage,
) -> Result<()> {
    tracing::Span::current().record("message.kind", coordinator_message_kind(&message));
    match message.payload {
        Some(coordinator_message::Payload::Assignment(offer)) => {
            let assignment: ExecutionAssignment = serde_json::from_str(&offer.assignment_json)?;
            tracing::Span::current().record("run_id", tracing::field::display(assignment.run_id));
            tracing::Span::current()
                .record("attempt_id", tracing::field::display(assignment.attempt_id));
            let executor = executor_kind(&assignment);
            if runtime
                .ledger
                .state(&assignment.attempt_id.to_string())
                .await?
                .as_deref()
                == Some("acknowledged")
            {
                record_assignment_offer("already_acknowledged", executor);
                return Ok(());
            }
            if let Some(pending) = runtime
                .ledger
                .result(&assignment.attempt_id.to_string())
                .await?
            {
                send_result(runtime, pending).await?;
                record_assignment_offer("result_replayed", executor);
                return Ok(());
            }
            // Prove local references are currently readable, decodable,
            // schema-valid, and policy-compliant before durable acceptance.
            // Values exist only in this temporary clone and are resolved again
            // immediately before launch to avoid using stale secret material.
            let preflight = if assignment.snapshot.late_bindings.is_some() {
                match runtime
                    .parameter_bindings
                    .resolve_assignment(&assignment)
                    .await
                {
                    Ok(preflight) => preflight,
                    Err(error) => {
                        record_assignment_offer("preflight_rejected", executor);
                        warn!(
                            attempt_id = %assignment.attempt_id,
                            rejection_code = "parameter_binding_failed",
                            error = %error,
                            "assignment rejected before acceptance"
                        );
                        send_assignment_rejection(runtime, &assignment, "parameter_binding_failed")
                            .await?;
                        return Ok(());
                    }
                }
            } else {
                assignment.clone()
            };
            let settings = runtime.settings.read().await.clone();
            if let Err(error) = validate_assignment(&settings, &preflight) {
                record_assignment_offer("policy_rejected", executor);
                warn!(
                    attempt_id = %assignment.attempt_id,
                    rejection_code = "assignment_policy_rejected",
                    error = %error,
                    "assignment rejected before acceptance"
                );
                send_assignment_rejection(runtime, &assignment, "assignment_policy_rejected")
                    .await?;
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
                record_assignment_offer("cancelled", executor);
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
            record_assignment_offer("accepted", executor);
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
            tracing::Span::current().record("settings.revision", update.revision);
            if update.heartbeat_seconds == 0 {
                record_settings_sync("rejected");
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
            record_settings_sync(if validation.is_ok() {
                "applied"
            } else {
                "rejected"
            });
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
            tracing::Span::current().record("attempt_id", ack.attempt_id.as_str());
            runtime.ledger.acknowledge(&ack.attempt_id).await?;
            opentelemetry::global::meter("scheduler-agent")
                .u64_counter("scheduler.agent.results")
                .build()
                .add(1, &[opentelemetry::KeyValue::new("result", "acknowledged")]);
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

fn coordinator_message_kind(
    message: &scheduler_protocol::control::CoordinatorMessage,
) -> &'static str {
    match &message.payload {
        Some(coordinator_message::Payload::Assignment(_)) => "assignment",
        Some(coordinator_message::Payload::Cancel(_)) => "cancel",
        Some(coordinator_message::Payload::Settings(_)) => "settings",
        Some(coordinator_message::Payload::ResultAcknowledged(_)) => "result_acknowledged",
        Some(coordinator_message::Payload::ResumeDecision(_)) => "resume_decision",
        Some(coordinator_message::Payload::HeartbeatAcknowledged(_)) => "heartbeat_acknowledged",
        None => "empty",
    }
}

fn record_assignment_offer(result: &'static str, executor: &'static str) {
    use opentelemetry::KeyValue;

    opentelemetry::global::meter("scheduler-agent")
        .u64_counter("scheduler.agent.assignment_offers")
        .build()
        .add(
            1,
            &[
                KeyValue::new("result", result),
                KeyValue::new("executor", executor),
            ],
        );
}

fn record_settings_sync(result: &'static str) {
    opentelemetry::global::meter("scheduler-agent")
        .u64_counter("scheduler.settings.sync")
        .build()
        .add(1, &[opentelemetry::KeyValue::new("result", result)]);
}

fn executor_kind(assignment: &ExecutionAssignment) -> &'static str {
    match &assignment.snapshot.executor {
        ExecutorSpec::Command(_) => "command",
        ExecutorSpec::ExcelMacro(_) => "excel_macro",
    }
}

#[instrument(
    name = "agent.assignment.execute",
    skip_all,
    fields(
        schedule_id = %assignment.schedule_id,
        run_id = %assignment.run_id,
        attempt_id = %assignment.attempt_id,
        executor = executor_kind(&assignment),
        task.outcome = tracing::field::Empty
    )
)]
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
                let executable_assignment = match runtime
                    .parameter_bindings
                    .resolve_assignment(&assignment)
                    .await
                {
                    Ok(assignment) => assignment,
                    Err(error) => {
                        break agent_failure_result(
                            error,
                            FailureCode::ParameterBindingFailed,
                            FailureStage::ParameterBinding,
                            "agent could not safely resolve required parameter bindings",
                            true,
                        );
                    }
                };
                if let Err(error) = validate_assignment(&settings, &executable_assignment) {
                    break agent_failure_result(
                        error,
                        FailureCode::AssignmentRejected,
                        FailureStage::Validation,
                        "resolved assignment was rejected by the agent's execution policy",
                        false,
                    );
                }
                break match run_executor(&runtime, &executable_assignment, &cancel_rx, lease_rx)
                    .await
                {
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
    let mut result = result;
    suppress_sensitive_task_output(&assignment, &mut result);
    tracing::Span::current().record("task.outcome", result.outcome.as_str());
    export_task_observability(&assignment, &result, &runtime.config.task_output_logging);
    match serde_json::to_string(&result) {
        Ok(json) => {
            let mut retry = Duration::from_millis(100);
            loop {
                match runtime.ledger.save_result(&attempt_id, &json).await {
                    Ok(()) => break,
                    Err(error) => {
                        opentelemetry::global::meter("scheduler-agent")
                            .u64_counter("scheduler.agent.result_persist_retries")
                            .build()
                            .add(1, &[]);
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
                opentelemetry::global::meter("scheduler-agent")
                    .u64_counter("scheduler.agent.results")
                    .build()
                    .add(1, &[opentelemetry::KeyValue::new("result", "retained")]);
                warn!(%error, %attempt_id, "result retained for reconnect");
            } else {
                opentelemetry::global::meter("scheduler-agent")
                    .u64_counter("scheduler.agent.results")
                    .build()
                    .add(1, &[opentelemetry::KeyValue::new("result", "sent")]);
            }
        }
        Err(error) => {
            opentelemetry::global::meter("scheduler-agent")
                .u64_counter("scheduler.agent.results")
                .build()
                .add(
                    1,
                    &[opentelemetry::KeyValue::new(
                        "result",
                        "serialization_failed",
                    )],
                );
            error!(%error, %attempt_id, "failed to serialize task result");
            runtime.active.write().await.remove(&attempt_id);
        }
    }
}

/// A task can deliberately or accidentally echo an environment/secret-file
/// binding. Once a blueprint admits any sensitive late binding, task text is
/// therefore untrusted secret-bearing data: retain only bounded byte metadata
/// and structured status. This runs before logging, ledger persistence, and
/// result transmission.
fn suppress_sensitive_task_output(assignment: &ExecutionAssignment, result: &mut ExecutionResult) {
    let has_sensitive_bindings = assignment
        .snapshot
        .late_bindings
        .as_ref()
        .is_some_and(|late| late.bindings.values().any(|binding| binding.sensitive));
    if !has_sensitive_bindings {
        return;
    }
    result.stdout.clear();
    result.stderr.clear();
    if result.error.is_some() {
        result.error = Some("sensitive task output suppressed by agent policy".into());
    }
    if let Some(diagnostic) = &mut result.diagnostic {
        diagnostic.summary = "sensitive task diagnostic text suppressed by agent policy".into();
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

fn export_task_observability(
    assignment: &ExecutionAssignment,
    result: &ExecutionResult,
    output_logging: &str,
) {
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
        .record(
            duration,
            &[KeyValue::new("executor", executor_kind(assignment))],
        );
    match output_logging {
        "off" => {}
        "metadata" => tracing::info!(
            target: "scheduler.task_output",
            run_id = %assignment.run_id,
            attempt_id = %assignment.attempt_id,
            stdout_bytes = result.output.stdout_bytes,
            stderr_bytes = result.output.stderr_bytes,
            stdout_truncated = result.output.stdout_truncated,
            stderr_truncated = result.output.stderr_truncated,
            "task output metadata"
        ),
        "content" => {
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
        _ => tracing::error!("invalid task output logging mode was accepted by configuration"),
    }
}

#[instrument(
    name = "agent.executor.lifetime",
    skip_all,
    fields(
        run_id = %assignment.run_id,
        attempt_id = %assignment.attempt_id,
        executor = executor_kind(assignment),
        process.status = tracing::field::Empty
    )
)]
async fn run_executor(
    runtime: &Runtime,
    assignment: &ExecutionAssignment,
    cancel: &watch::Receiver<bool>,
    mut lease_freshness: watch::Receiver<u64>,
) -> Result<ExecutionResult> {
    use opentelemetry::KeyValue;

    let lifetime_started = Instant::now();
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
    let process_status = if output.status.success() {
        "success"
    } else {
        "failure"
    };
    tracing::Span::current().record("process.status", process_status);
    opentelemetry::global::meter("scheduler-agent")
        .f64_histogram("scheduler.executor.lifetime_ms")
        .build()
        .record(
            lifetime_started.elapsed().as_secs_f64() * 1_000.0,
            &[
                KeyValue::new("executor", executor_kind(assignment)),
                KeyValue::new("process_status", process_status),
            ],
        );
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
    // Paths and executables containing ordinary parameter templates are fully
    // checked after local bindings have been resolved. The late envelope never
    // contains resolved secret values and is not sent to the task executor.
    if assignment.snapshot.late_bindings.is_some() {
        return Ok(());
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

async fn send_assignment_rejection(
    runtime: &Runtime,
    assignment: &ExecutionAssignment,
    code: &'static str,
) -> Result<()> {
    send(
        runtime,
        assignment_rejection_message(&runtime.config.agent_id, assignment, code),
    )
    .await
}

fn assignment_rejection_message(
    agent_id: &str,
    assignment: &ExecutionAssignment,
    code: &'static str,
) -> AgentMessage {
    AgentMessage {
        payload: Some(agent_message::Payload::Rejected(AttemptRejected {
            agent_id: agent_id.to_owned(),
            attempt_id: assignment.attempt_id.to_string(),
            lease_token: assignment.lease_token.clone(),
            code: code.into(),
        })),
    }
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
    use uuid::Uuid;

    fn assignment(executor: serde_json::Value) -> ExecutionAssignment {
        serde_json::from_value(serde_json::json!({
            "schedule_id": Uuid::new_v4(),
            "run_id": Uuid::new_v4(),
            "attempt_id": Uuid::new_v4(),
            "attempt_number": 1,
            "lease_token": "lease-token",
            "lease_seconds": 60,
            "snapshot": {
                "executor": executor,
                "policy": {},
                "required_labels": {},
                "blueprint_digest": "blueprint-digest",
                "parameters_digest": "parameters-digest"
            }
        }))
        .expect("assignment fixture")
    }

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

    #[test]
    fn ordinary_command_and_excel_offers_are_policy_checked_before_acceptance() {
        let command = assignment(serde_json::json!({
            "kind": "command",
            "program": "runner"
        }));
        let disabled = NodeSettings {
            enabled: false,
            ..NodeSettings::default()
        };
        assert!(validate_assignment(&disabled, &command).is_err());

        let excel = assignment(serde_json::json!({
            "kind": "excel_macro",
            "workbook_path": "C:\\Reports\\test.xlsm",
            "macro_name": "RunReport"
        }));
        let excel_disabled = NodeSettings {
            excel_max_parallel: 0,
            ..NodeSettings::default()
        };
        assert!(validate_assignment(&excel_disabled, &excel).is_err());
    }

    #[test]
    fn assignment_rejection_message_contains_only_safe_references() {
        let assignment = assignment(serde_json::json!({
            "kind": "command",
            "program": "runner"
        }));
        let message =
            assignment_rejection_message("node-a", &assignment, "assignment_policy_rejected");
        let agent_message::Payload::Rejected(rejected) = message.payload.expect("payload") else {
            panic!("rejected payload expected");
        };
        assert_eq!(rejected.agent_id, "node-a");
        assert_eq!(rejected.attempt_id, assignment.attempt_id.to_string());
        assert_eq!(rejected.lease_token, "lease-token");
        assert_eq!(rejected.code, "assignment_policy_rejected");
    }

    #[test]
    fn sensitive_binding_attempts_suppress_all_persisted_task_text() {
        let mut assignment = assignment(serde_json::json!({
            "kind": "command",
            "program": "runner"
        }));
        assignment.snapshot.late_bindings = Some(
            serde_json::from_value(serde_json::json!({
                "executor_template": {
                    "kind": "command",
                    "program": "runner",
                    "env": {"SECRET": "{{params.secret}}"}
                },
                "parameters_schema": {"type": "object"},
                "parameters": {},
                "bindings": {
                    "secret": {
                        "source": "environment",
                        "name": "TASK_TEST_SECRET",
                        "value_type": "string",
                        "sensitive": true
                    }
                }
            }))
            .expect("late-binding fixture"),
        );
        let now = Utc::now();
        let secret = "do-not-persist-this-secret";
        let mut result = ExecutionResult {
            outcome: ExecutionOutcome::InfrastructureError,
            exit_code: Some(1),
            signal: None,
            stdout: format!("stdout {secret}"),
            stderr: format!("stderr {secret}"),
            started_at: now,
            finished_at: now,
            error: Some(format!("error {secret}")),
            output: OutputMetadata {
                stdout_bytes: 99,
                stderr_bytes: 88,
                stdout_truncated: false,
                stderr_truncated: true,
            },
            diagnostic: Some(FailureDiagnostic::new(
                FailureCode::ProcessSpawnFailed,
                FailureOrigin::TaskExecutor,
                FailureStage::Execution,
                format!("diagnostic {secret}"),
                true,
            )),
        };

        suppress_sensitive_task_output(&assignment, &mut result);
        let persisted = serde_json::to_string(&result).expect("result JSON");
        assert!(!persisted.contains(secret));
        assert!(result.stdout.is_empty());
        assert!(result.stderr.is_empty());
        assert_eq!(result.output.stdout_bytes, 99);
        assert_eq!(result.output.stderr_bytes, 88);
        assert_eq!(
            result.diagnostic.expect("diagnostic").code,
            FailureCode::ProcessSpawnFailed
        );
    }
}
