mod config;
mod ledger;
mod proxy;

use std::{
    collections::HashMap,
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
    ExecutionAssignment, ExecutionOutcome, ExecutionResult, ExecutorSpec, NodeSettings,
};
use scheduler_protocol::control::{
    AgentHello, AgentMessage, AttemptAccepted, AttemptResult, Heartbeat, SettingsApplied,
    agent_message, coordinator_message, scheduler_control_client::SchedulerControlClient,
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
    outgoing: Arc<RwLock<Option<mpsc::Sender<AgentMessage>>>>,
    connected: watch::Sender<bool>,
}

#[derive(Clone)]
struct ActiveTask {
    cancel: watch::Sender<bool>,
    excel: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    let _telemetry = scheduler_telemetry::init("scheduler-agent", config.otlp_endpoint.as_deref())?;
    let ledger = Ledger::connect(&config.database_url).await?;
    let (connected, _) = watch::channel(false);
    let runtime = Runtime {
        config: config.clone(),
        ledger,
        settings: Arc::new(RwLock::new(NodeSettings {
            max_parallel: config.capacity,
            ..NodeSettings::default()
        })),
        active: Arc::new(RwLock::new(HashMap::new())),
        outgoing: Arc::new(RwLock::new(None)),
        connected,
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
                let _ = runtime.connected.send(false);
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
    let hostname = hostname::get()?.to_string_lossy().into_owned();
    outgoing_tx
        .send(AgentMessage {
            payload: Some(agent_message::Payload::Hello(AgentHello {
                agent_id: runtime.config.agent_id.clone(),
                hostname,
                labels,
                capacity: runtime.config.capacity,
                running: runtime.active.read().await.len() as u32,
                version: env!("CARGO_PKG_VERSION").into(),
            })),
        })
        .await?;
    let mut incoming = client
        .agent_stream(ReceiverStream::new(outgoing_rx))
        .await?
        .into_inner();
    let _ = runtime.connected.send(true);
    info!(
        agent_id = runtime.config.agent_id,
        "connected to coordinator"
    );
    send_pending(runtime, &outgoing_tx).await?;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let active = runtime.active.read().await.keys().cloned().collect::<Vec<_>>();
                outgoing_tx.send(AgentMessage { payload: Some(agent_message::Payload::Heartbeat(Heartbeat {
                    agent_id: runtime.config.agent_id.clone(),
                    running: active.len() as u32,
                    active_attempt_ids: active,
                })) }).await?;
                send_pending(runtime, &outgoing_tx).await?;
            }
            message = incoming.next() => {
                let Some(message) = message else { break; };
                handle_coordinator_message(runtime, message?).await?;
            }
        }
    }
    Ok(())
}

async fn handle_coordinator_message(
    runtime: &Runtime,
    message: scheduler_protocol::control::CoordinatorMessage,
) -> Result<()> {
    match message.payload {
        Some(coordinator_message::Payload::Assignment(offer)) => {
            let assignment: ExecutionAssignment = serde_json::from_str(&offer.assignment_json)?;
            if let Some(pending) = runtime
                .ledger
                .result(&assignment.attempt_id.to_string())
                .await?
            {
                send_result(runtime, pending).await?;
                return Ok(());
            }
            let inserted = runtime
                .ledger
                .accept(&assignment, &offer.assignment_json)
                .await?;
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
            if inserted {
                let runtime = runtime.clone();
                tokio::spawn(async move {
                    execute_assignment(runtime, assignment).await;
                });
            }
        }
        Some(coordinator_message::Payload::Cancel(cancel)) => {
            if let Some(control) = runtime.active.read().await.get(&cancel.attempt_id) {
                let _ = control.cancel.send(true);
            }
        }
        Some(coordinator_message::Payload::Settings(update)) => {
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
        None => bail!("empty coordinator message"),
    }
    Ok(())
}

async fn execute_assignment(runtime: Runtime, assignment: ExecutionAssignment) {
    let attempt_id = assignment.attempt_id.to_string();
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let settings = runtime.settings.read().await.clone();
    let is_excel = matches!(&assignment.snapshot.executor, ExecutorSpec::ExcelMacro(_));
    let capacity = {
        let mut active = runtime.active.write().await;
        let excel_running = active.values().filter(|task| task.excel).count() as u32;
        if active.len() as u32 >= settings.max_parallel {
            Err(anyhow::anyhow!("agent has no free execution slot"))
        } else if is_excel && excel_running >= settings.excel_max_parallel {
            Err(anyhow::anyhow!("agent has no free Excel execution slot"))
        } else {
            active.insert(
                attempt_id.clone(),
                ActiveTask {
                    cancel: cancel_tx,
                    excel: is_excel,
                },
            );
            Ok(())
        }
    };
    let result = match capacity.and_then(|_| validate_assignment(&settings, &assignment)) {
        Ok(()) => run_executor(&runtime, &assignment, &cancel_rx).await,
        Err(error) => Err(error),
    };
    let result = match result {
        Ok(result) => result,
        Err(error) => {
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
            }
        }
    };
    export_task_observability(&assignment, &result);
    runtime.active.write().await.remove(&attempt_id);
    match serde_json::to_string(&result) {
        Ok(json) => {
            if let Err(error) = runtime.ledger.save_result(&attempt_id, &json).await {
                error!(%error, %attempt_id, "failed to persist task result");
                return;
            }
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
        Err(error) => error!(%error, %attempt_id, "failed to serialize task result"),
    }
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
) -> Result<ExecutionResult> {
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
    let mut stdin = child.stdin.take().context("executor stdin unavailable")?;
    stdin
        .write_all(serde_json::to_string(assignment)?.as_bytes())
        .await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    let mut connected = runtime.connected.subscribe();
    let mut cancel = cancel.clone();
    let controller = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if *connected.borrow() {
                        if stdin.write_all(b"{\"keepalive\":true}\n").await.is_err() { break; }
                        let _ = stdin.flush().await;
                    }
                }
                changed = cancel.changed() => {
                    if changed.is_err() { break; }
                    if *cancel.borrow() {
                        let _ = stdin.write_all(b"{\"cancel\":true}\n").await;
                        let _ = stdin.flush().await;
                        break;
                    }
                }
                changed = connected.changed() => if changed.is_err() { break; },
            }
        }
    });
    let output = child.wait_with_output().await?;
    controller.abort();
    if !output.status.success() && output.stdout.is_empty() {
        bail!(
            "executor failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .context("executor returned no result")?;
    serde_json::from_str(line).context("executor returned invalid result JSON")
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
