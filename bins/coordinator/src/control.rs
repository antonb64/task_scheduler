use std::{
    collections::{BTreeMap, HashMap},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context;
use futures::{Stream, StreamExt};
use scheduler_core::{
    ExecutionOutcome, ExecutionResult, FailureCode, FailureDiagnostic, FailureOrigin, FailureStage,
    OutputMetadata, validate_agent_id,
};
use scheduler_protocol::control::{
    AgentMessage, CoordinatorMessage, HeartbeatAcknowledged, ManagementRequest, ManagementResponse,
    ResumeDecision, SettingsUpdate, agent_message, coordinator_message,
    scheduler_control_server::SchedulerControl,
};
use scheduler_store::{SettingsAckOutcome, SettingsAckStatus};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};
use uuid::Uuid;

use crate::state::{AgentSession, AppState};

#[derive(Clone)]
pub struct ControlService {
    state: AppState,
    client: reqwest::Client,
    agent_gates: AgentGateMap,
    certificate_fingerprints: Option<Arc<HashMap<String, [u8; 32]>>>,
}

type AgentGateMap = Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>;

const MANAGEMENT_PROXY_TIMEOUT: Duration = Duration::from_secs(15);
const MANAGEMENT_PROXY_RESPONSE_LIMIT: usize = 2 * 1024 * 1024;

impl ControlService {
    pub fn new(
        state: AppState,
        certificate_fingerprints: Option<HashMap<String, [u8; 32]>>,
    ) -> Self {
        Self {
            state,
            // Preserve coordinator redirects for the browser. Following them here
            // would render the right page while leaving the agent-UI browser on a
            // POST-only `/ui/...` URL, which then breaks refresh and navigation.
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .build()
                .expect("static management proxy client configuration is valid"),
            agent_gates: Arc::new(Mutex::new(HashMap::new())),
            certificate_fingerprints: certificate_fingerprints.map(Arc::new),
        }
    }

    fn authorize_agent_certificate(
        &self,
        agent_id: &str,
        certificate: Option<&[u8]>,
    ) -> Result<(), Status> {
        authorize_agent_certificate(
            self.certificate_fingerprints.as_deref(),
            agent_id,
            certificate,
        )
    }

    async fn agent_gate(&self, agent_id: &str) -> Arc<Mutex<()>> {
        self.agent_gates
            .lock()
            .await
            .entry(agent_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

fn authorize_agent_certificate(
    allowed: Option<&HashMap<String, [u8; 32]>>,
    agent_id: &str,
    certificate: Option<&[u8]>,
) -> Result<(), Status> {
    let Some(allowed) = allowed else {
        return Ok(());
    };
    let expected = allowed
        .get(agent_id)
        .ok_or_else(|| Status::permission_denied("agent certificate identity is not registered"))?;
    let certificate = certificate
        .ok_or_else(|| Status::unauthenticated("agent client certificate is required"))?;
    let actual: [u8; 32] = Sha256::digest(certificate).into();
    if subtle::ConstantTimeEq::ct_eq(actual.as_slice(), expected.as_slice()).into() {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "agent certificate does not match the registered node identity",
        ))
    }
}

type MessageStream = Pin<Box<dyn Stream<Item = Result<CoordinatorMessage, Status>> + Send>>;

struct SessionPlacement {
    enabled: bool,
    labels: BTreeMap<String, String>,
    capacity: u32,
    revision: i64,
}

fn reconnect_placement(
    desired: &scheduler_core::NodeSettings,
    applied_revision: i64,
    previous: Option<&AgentSession>,
) -> SessionPlacement {
    if let Some(previous) =
        previous.filter(|session| session.applied_settings_revision == applied_revision)
    {
        return SessionPlacement {
            enabled: previous.enabled,
            labels: previous.labels.clone(),
            capacity: previous.capacity,
            revision: applied_revision,
        };
    }
    if applied_revision == desired.revision {
        return SessionPlacement {
            enabled: desired.enabled,
            labels: desired.labels.clone(),
            capacity: desired.max_parallel,
            revision: applied_revision,
        };
    }
    // The coordinator does not retain historical node-setting documents. If
    // this process cannot prove which document the agent has applied, keep the
    // connection visible but placement-ineligible until the current revision
    // is acknowledged.
    SessionPlacement {
        enabled: false,
        labels: BTreeMap::new(),
        capacity: 0,
        revision: applied_revision,
    }
}

fn apply_settings_ack(session: &mut AgentSession, outcome: SettingsAckOutcome) {
    if outcome.status != SettingsAckStatus::Applied {
        return;
    }
    let Some(settings) = outcome.applied_settings else {
        return;
    };
    session.enabled = settings.enabled;
    session.labels = settings.labels;
    session.capacity = settings.max_parallel;
    session.applied_settings_revision = settings.revision;
}

fn is_current_session(
    sessions: &HashMap<String, AgentSession>,
    agent_id: &str,
    connection_id: Uuid,
) -> bool {
    sessions
        .get(agent_id)
        .is_some_and(|session| session.connection_id == connection_id)
}

fn current_session_mut<'a>(
    sessions: &'a mut HashMap<String, AgentSession>,
    agent_id: &str,
    connection_id: Uuid,
) -> Option<&'a mut AgentSession> {
    sessions
        .get_mut(agent_id)
        .filter(|session| session.connection_id == connection_id)
}

fn update_running_for_connection(
    sessions: &mut HashMap<String, AgentSession>,
    agent_id: &str,
    connection_id: Uuid,
    running: u32,
) -> bool {
    let Some(session) = current_session_mut(sessions, agent_id, connection_id) else {
        return false;
    };
    session.running = running;
    true
}

fn release_slot_for_connection(
    sessions: &mut HashMap<String, AgentSession>,
    agent_id: &str,
    connection_id: Uuid,
) -> bool {
    let Some(session) = current_session_mut(sessions, agent_id, connection_id) else {
        return false;
    };
    session.running = session.running.saturating_sub(1);
    true
}

fn apply_settings_ack_for_connection(
    sessions: &mut HashMap<String, AgentSession>,
    agent_id: &str,
    connection_id: Uuid,
    outcome: SettingsAckOutcome,
) -> bool {
    let Some(session) = current_session_mut(sessions, agent_id, connection_id) else {
        return false;
    };
    apply_settings_ack(session, outcome);
    true
}

fn queue_response(
    tx: &mpsc::Sender<Result<CoordinatorMessage, Status>>,
    message: CoordinatorMessage,
) -> anyhow::Result<()> {
    tx.try_send(Ok(message)).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => {
            anyhow::anyhow!("agent response channel is full; closing stream")
        }
        mpsc::error::TrySendError::Closed(_) => {
            anyhow::anyhow!("agent response channel is closed")
        }
    })
}

fn publish_session_after_settings(
    sessions: &mut HashMap<String, AgentSession>,
    agent_id: String,
    session: AgentSession,
    settings_message: CoordinatorMessage,
) -> anyhow::Result<()> {
    // Queue the authoritative settings frame before publishing the session.
    // Any assignment sent after publication is therefore ordered behind it.
    queue_response(&session.tx, settings_message)?;
    sessions.insert(agent_id, session);
    Ok(())
}

async fn prune_agent_gate(
    gates: &AgentGateMap,
    agent_id: &str,
    completed_gate: &Arc<Mutex<()>>,
) -> bool {
    let mut gates = gates.lock().await;
    let removable = gates.get(agent_id).is_some_and(|registered| {
        Arc::ptr_eq(registered, completed_gate) && Arc::strong_count(registered) == 2
    });
    if removable {
        gates.remove(agent_id);
    }
    removable
}

#[tonic::async_trait]
impl SchedulerControl for ControlService {
    type AgentStreamStream = MessageStream;

    #[instrument(name = "coordinator.agent.stream", skip_all)]
    async fn agent_stream(
        &self,
        request: Request<tonic::Streaming<AgentMessage>>,
    ) -> Result<Response<Self::AgentStreamStream>, Status> {
        let peer_certificate = request
            .peer_certs()
            .and_then(|certificates| certificates.first().cloned());
        let mut incoming = request.into_inner();
        let first = incoming
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("agent hello is required"))??;
        let Some(agent_message::Payload::Hello(hello)) = first.payload else {
            return Err(Status::invalid_argument(
                "first agent message must be hello",
            ));
        };
        validate_agent_id(&hello.agent_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.authorize_agent_certificate(
            &hello.agent_id,
            peer_certificate.as_deref().map(AsRef::as_ref),
        )?;
        let agent_gate = self.agent_gate(&hello.agent_id).await;
        let connection_guard = agent_gate.lock().await;
        let registration = self
            .state
            .store
            .upsert_agent(
                &hello.agent_id,
                &hello.hostname,
                &hello.labels,
                hello.capacity,
                hello.running,
            )
            .await
            .map_err(internal)?;
        let settings = registration.desired_settings;
        let health = self
            .state
            .store
            .node_health(&hello.agent_id)
            .await
            .map_err(internal)?
            .map_or(scheduler_core::health::NodeHealthState::Healthy, |view| {
                view.state
            });
        let heartbeat_seconds = self
            .state
            .store
            .get_global_settings()
            .await
            .map_err(internal)?
            .heartbeat_seconds;
        let (tx, rx) = mpsc::channel(256);
        let connection_id = Uuid::new_v4();
        {
            let mut sessions = self.state.sessions.write().await;
            let placement = reconnect_placement(
                &settings,
                registration.applied_settings_revision,
                sessions.get(&hello.agent_id),
            );
            let session = AgentSession {
                connection_id,
                tx: tx.clone(),
                enabled: placement.enabled,
                labels: placement.labels,
                capacity: placement.capacity,
                applied_settings_revision: placement.revision,
                running: hello.running,
                health,
                last_assigned: Instant::now(),
                environment_bindings: hello.environment_bindings.into_iter().collect(),
                secret_file_bindings: hello.secret_file_bindings.into_iter().collect(),
            };
            let settings_message = CoordinatorMessage {
                payload: Some(coordinator_message::Payload::Settings(SettingsUpdate {
                    revision: settings.revision,
                    settings_json: serde_json::to_string(&settings).map_err(internal)?,
                    heartbeat_seconds,
                })),
            };
            publish_session_after_settings(
                &mut sessions,
                hello.agent_id.clone(),
                session,
                settings_message,
            )
            .map_err(internal)?;
        }
        drop(connection_guard);

        let state = self.state.clone();
        let agent_gates = self.agent_gates.clone();
        let agent_id = hello.agent_id.clone();
        tokio::spawn(async move {
            info!(agent_id, "agent connected");
            while let Some(message) = incoming.next().await {
                match message {
                    Ok(message) => {
                        let is_current = {
                            let sessions = state.sessions.read().await;
                            is_current_session(&sessions, &agent_id, connection_id)
                        };
                        if !is_current {
                            info!(agent_id, %connection_id, "superseded agent stream closed");
                            break;
                        }
                        if let Err(error) = handle_agent_message(
                            &state,
                            &agent_id,
                            connection_id,
                            &agent_gate,
                            &tx,
                            message,
                        )
                        .await
                        {
                            warn!(agent_id, error = %error, "agent message rejected");
                            break;
                        }
                    }
                    Err(error) => {
                        warn!(agent_id, error = %error, "agent stream failed");
                        break;
                    }
                }
            }
            let _connection_guard = agent_gate.lock().await;
            let removed_current_connection = {
                let mut sessions = state.sessions.write().await;
                let is_current = sessions
                    .get(&agent_id)
                    .is_some_and(|session| session.connection_id == connection_id);
                if is_current {
                    sessions.remove(&agent_id);
                }
                is_current
            };
            if removed_current_connection {
                let _ = state.store.disconnect_agent(&agent_id).await;
                info!(agent_id, "agent disconnected");
            }
            prune_agent_gate(&agent_gates, &agent_id, &agent_gate).await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    #[instrument(name = "coordinator.management.proxy", skip_all)]
    async fn manage(
        &self,
        request: Request<ManagementRequest>,
    ) -> Result<Response<ManagementResponse>, Status> {
        let request = request.into_inner();
        if !request.path.starts_with('/') || request.path.starts_with("//") {
            return Err(Status::invalid_argument("management path must be local"));
        }
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|_| Status::invalid_argument("invalid HTTP method"))?;
        let url = format!(
            "{}{}",
            self.state.internal_rest_url.trim_end_matches('/'),
            request.path
        );
        let mut outbound = self.client.request(method, url).body(request.body);
        for (name, value) in request.headers {
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "content-type" | "cookie" | "if-match" | "idempotency-key" | "x-request-id"
            ) {
                outbound = outbound.header(name, value);
            }
        }
        if request.path.starts_with("/api/") {
            outbound = outbound.bearer_auth(&self.state.internal_admin_token);
        }
        let (status, headers, body) = tokio::time::timeout(MANAGEMENT_PROXY_TIMEOUT, async move {
            let response = outbound.send().await.map_err(internal)?;
            let status = response.status().as_u16() as u32;
            let mut headers = std::collections::HashMap::new();
            for name in [
                "content-type",
                "location",
                "set-cookie",
                "etag",
                "x-request-id",
            ] {
                if let Some(value) = response
                    .headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                {
                    headers.insert(name.to_owned(), value.to_owned());
                }
            }
            let body = read_management_body(response).await?;
            Ok::<_, Status>((status, headers, body))
        })
        .await
        .map_err(|_| Status::deadline_exceeded("management request timed out"))??;
        Ok(Response::new(ManagementResponse {
            status,
            body,
            headers,
        }))
    }
}

async fn read_management_body(response: reqwest::Response) -> Result<String, Status> {
    if response
        .content_length()
        .is_some_and(|length| length > MANAGEMENT_PROXY_RESPONSE_LIMIT as u64)
    {
        return Err(Status::resource_exhausted(
            "management response exceeded the configured limit",
        ));
    }
    let mut body = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(MANAGEMENT_PROXY_RESPONSE_LIMIT as u64) as usize,
    );
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(internal)?;
        append_management_chunk(&mut body, &chunk)?;
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn append_management_chunk(body: &mut Vec<u8>, chunk: &[u8]) -> Result<(), Status> {
    if body.len().saturating_add(chunk.len()) > MANAGEMENT_PROXY_RESPONSE_LIMIT {
        return Err(Status::resource_exhausted(
            "management response exceeded the configured limit",
        ));
    }
    body.extend_from_slice(chunk);
    Ok(())
}

#[instrument(
    name = "coordinator.agent.message",
    skip_all,
    fields(
        agent_id = expected_agent_id,
        connection_id = %connection_id,
        message.kind = tracing::field::Empty,
        attempt_id = tracing::field::Empty,
        settings.revision = tracing::field::Empty
    )
)]
async fn handle_agent_message(
    state: &AppState,
    expected_agent_id: &str,
    connection_id: Uuid,
    agent_gate: &Mutex<()>,
    tx: &mpsc::Sender<Result<CoordinatorMessage, Status>>,
    message: AgentMessage,
) -> anyhow::Result<()> {
    use opentelemetry::KeyValue;

    let message_kind = agent_message_kind(&message);
    tracing::Span::current().record("message.kind", message_kind);
    // Registration/replacement and all message side effects for one agent ID
    // share this gate. A superseded stream therefore fails the epoch check
    // below before it can touch durable or in-memory state.
    let _connection_guard = agent_gate.lock().await;
    let is_current = {
        let sessions = state.sessions.read().await;
        is_current_session(&sessions, expected_agent_id, connection_id)
    };
    if !is_current {
        anyhow::bail!("agent stream has been superseded by a newer connection");
    }
    match message.payload {
        Some(agent_message::Payload::Heartbeat(heartbeat)) => {
            ensure_agent(expected_agent_id, &heartbeat.agent_id)?;
            let lease_seconds = state.store.get_global_settings().await?.lease_seconds;
            let renewed_attempt_ids = state
                .store
                .renew_attempts(
                    expected_agent_id,
                    &heartbeat.active_attempt_ids,
                    lease_seconds,
                    heartbeat.running,
                )
                .await?;
            queue_response(
                tx,
                CoordinatorMessage {
                    payload: Some(coordinator_message::Payload::HeartbeatAcknowledged(
                        HeartbeatAcknowledged {
                            renewed_attempt_ids,
                        },
                    )),
                },
            )?;
            let mut sessions = state.sessions.write().await;
            update_running_for_connection(
                &mut sessions,
                expected_agent_id,
                connection_id,
                heartbeat.running,
            );
        }
        Some(agent_message::Payload::Accepted(accepted)) => {
            ensure_agent(expected_agent_id, &accepted.agent_id)?;
            tracing::Span::current().record("attempt_id", accepted.attempt_id.as_str());
            let lease_seconds = state.store.get_global_settings().await?.lease_seconds;
            state
                .store
                .accept_attempt(
                    Uuid::parse_str(&accepted.attempt_id)?,
                    &accepted.lease_token,
                    lease_seconds,
                )
                .await?;
            opentelemetry::global::meter("scheduler-coordinator")
                .u64_counter("scheduler.agent.acceptances")
                .build()
                .add(1, &[KeyValue::new("result", "accepted")]);
        }
        Some(agent_message::Payload::Rejected(rejected)) => {
            ensure_agent(expected_agent_id, &rejected.agent_id)?;
            let attempt_id = Uuid::parse_str(&rejected.attempt_id)?;
            tracing::Span::current().record("attempt_id", tracing::field::display(attempt_id));
            let result = preacceptance_rejection_result(&rejected.code, chrono::Utc::now())?;
            let diagnostic = result
                .diagnostic
                .as_ref()
                .context("pre-acceptance rejection is missing its diagnostic")?;
            let health_context = state
                .store
                .attempt_health_context(attempt_id)
                .await?
                .context("attempt health context is missing")?;
            let encrypted = state.cipher.encrypt(&serde_json::to_vec(&result)?)?;
            state
                .store
                .reject_offer(
                    attempt_id,
                    &rejected.lease_token,
                    &result,
                    encrypted,
                    state.cipher.key_id(),
                )
                .await?;
            crate::health::record_attempt_result(state, &health_context, &result).await?;
            let mut sessions = state.sessions.write().await;
            release_slot_for_connection(&mut sessions, expected_agent_id, connection_id);
            drop(sessions);
            opentelemetry::global::meter("scheduler-coordinator")
                .u64_counter("scheduler.agent.acceptances")
                .build()
                .add(1, &[KeyValue::new("result", "rejected")]);
            warn!(
                agent_id = expected_agent_id,
                %attempt_id,
                rejection_code = rejected.code,
                failure_code = ?diagnostic.code,
                "assignment rejected before acceptance"
            );
        }
        Some(agent_message::Payload::Result(result)) => {
            ensure_agent(expected_agent_id, &result.agent_id)?;
            let attempt_id = Uuid::parse_str(&result.attempt_id)?;
            tracing::Span::current().record("attempt_id", tracing::field::display(attempt_id));
            let parsed: ExecutionResult = serde_json::from_str(&result.result_json)?;
            let outcome = parsed.outcome.as_str();
            let health_context = state
                .store
                .attempt_health_context(attempt_id)
                .await?
                .context("attempt health context is missing")?;
            let encrypted = state.cipher.encrypt(result.result_json.as_bytes())?;
            state
                .store
                .finish_attempt(
                    attempt_id,
                    &result.lease_token,
                    &parsed,
                    encrypted,
                    state.cipher.key_id(),
                )
                .await?;
            crate::health::record_attempt_result(state, &health_context, &parsed).await?;
            opentelemetry::global::meter("scheduler-coordinator")
                .u64_counter("scheduler.attempt.results")
                .build()
                .add(1, &[opentelemetry::KeyValue::new("outcome", outcome)]);
            if let Some(diagnostic) = &parsed.diagnostic {
                tracing::warn!(
                    agent_id = expected_agent_id,
                    %attempt_id,
                    failure_code = ?diagnostic.code,
                    failure_origin = ?diagnostic.origin,
                    failure_stage = ?diagnostic.stage,
                    retryable = diagnostic.retryable,
                    "attempt diagnostic received"
                );
            }
            let mut sessions = state.sessions.write().await;
            release_slot_for_connection(&mut sessions, expected_agent_id, connection_id);
            drop(sessions);
            queue_response(
                tx,
                CoordinatorMessage {
                    payload: Some(coordinator_message::Payload::ResultAcknowledged(
                        scheduler_protocol::control::ResultAcknowledged {
                            attempt_id: attempt_id.to_string(),
                        },
                    )),
                },
            )?;
        }
        Some(agent_message::Payload::SettingsApplied(applied)) => {
            ensure_agent(expected_agent_id, &applied.agent_id)?;
            tracing::Span::current().record("settings.revision", applied.revision);
            let outcome = state
                .store
                .settings_applied(
                    expected_agent_id,
                    applied.revision,
                    applied.error.as_deref(),
                )
                .await?;
            let result = match outcome.status {
                SettingsAckStatus::Applied => "applied",
                SettingsAckStatus::Rejected => "rejected",
                SettingsAckStatus::Ignored => "stale",
            };
            opentelemetry::global::meter("scheduler-coordinator")
                .u64_counter("scheduler.settings.sync")
                .build()
                .add(1, &[KeyValue::new("result", result)]);
            let mut sessions = state.sessions.write().await;
            apply_settings_ack_for_connection(
                &mut sessions,
                expected_agent_id,
                connection_id,
                outcome,
            );
        }
        Some(agent_message::Payload::Resume(resume)) => {
            ensure_agent(expected_agent_id, &resume.agent_id)?;
            let attempt_id = Uuid::parse_str(&resume.attempt_id)?;
            let lease_seconds = state.store.get_global_settings().await?.lease_seconds;
            let granted = state
                .store
                .reauthorize_attempt(
                    attempt_id,
                    expected_agent_id,
                    &resume.lease_token,
                    lease_seconds,
                )
                .await?;
            queue_response(
                tx,
                CoordinatorMessage {
                    payload: Some(coordinator_message::Payload::ResumeDecision(
                        ResumeDecision {
                            attempt_id: resume.attempt_id,
                            lease_token: resume.lease_token,
                            granted,
                            reason: (!granted).then(|| {
                                "attempt is stale, expired, cancelled, finished, or replaced".into()
                            }),
                        },
                    )),
                },
            )?;
        }
        Some(agent_message::Payload::Hello(_)) => anyhow::bail!("hello may only be sent once"),
        None => anyhow::bail!("empty agent message"),
    }
    Ok(())
}

fn preacceptance_rejection_result(
    rejection_code: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<ExecutionResult> {
    let (code, stage, message) = match rejection_code {
        "parameter_binding_failed" => (
            FailureCode::ParameterBindingFailed,
            FailureStage::ParameterBinding,
            "agent could not resolve or validate a required parameter binding",
        ),
        "assignment_policy_rejected" => (
            FailureCode::AssignmentRejected,
            FailureStage::Validation,
            "agent rejected the assignment under its current execution policy",
        ),
        _ => anyhow::bail!("agent supplied an unsupported assignment rejection code"),
    };
    Ok(ExecutionResult {
        outcome: ExecutionOutcome::InfrastructureError,
        exit_code: None,
        signal: None,
        stdout: String::new(),
        stderr: String::new(),
        started_at: now,
        finished_at: now,
        error: Some(message.into()),
        output: OutputMetadata::default(),
        diagnostic: Some(FailureDiagnostic::new(
            code,
            FailureOrigin::Agent,
            stage,
            message,
            true,
        )),
    })
}

fn agent_message_kind(message: &AgentMessage) -> &'static str {
    match &message.payload {
        Some(agent_message::Payload::Heartbeat(_)) => "heartbeat",
        Some(agent_message::Payload::Accepted(_)) => "accepted",
        Some(agent_message::Payload::Result(_)) => "result",
        Some(agent_message::Payload::SettingsApplied(_)) => "settings_applied",
        Some(agent_message::Payload::Resume(_)) => "resume",
        Some(agent_message::Payload::Rejected(_)) => "rejected",
        Some(agent_message::Payload::Hello(_)) => "hello",
        None => "empty",
    }
}

fn ensure_agent(expected: &str, supplied: &str) -> anyhow::Result<()> {
    if expected != supplied {
        anyhow::bail!("agent identity changed within stream");
    }
    Ok(())
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use scheduler_core::NodeSettings;
    use scheduler_store::{SettingsAckOutcome, SettingsAckStatus};
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::{
        AgentSession, MANAGEMENT_PROXY_RESPONSE_LIMIT, append_management_chunk, apply_settings_ack,
        apply_settings_ack_for_connection, authorize_agent_certificate, is_current_session,
        preacceptance_rejection_result, reconnect_placement, release_slot_for_connection,
        update_running_for_connection,
    };

    #[test]
    fn preacceptance_rejections_are_safe_structured_infrastructure_results() {
        let now = chrono::Utc::now();
        for (wire_code, expected_code, expected_stage) in [
            (
                "parameter_binding_failed",
                scheduler_core::FailureCode::ParameterBindingFailed,
                scheduler_core::FailureStage::ParameterBinding,
            ),
            (
                "assignment_policy_rejected",
                scheduler_core::FailureCode::AssignmentRejected,
                scheduler_core::FailureStage::Validation,
            ),
        ] {
            let result = preacceptance_rejection_result(wire_code, now).expect("known rejection");
            assert_eq!(
                result.outcome,
                scheduler_core::ExecutionOutcome::InfrastructureError
            );
            assert!(result.stdout.is_empty());
            assert!(result.stderr.is_empty());
            let diagnostic = result.diagnostic.expect("diagnostic");
            assert_eq!(diagnostic.code, expected_code);
            assert_eq!(diagnostic.stage, expected_stage);
            assert_eq!(diagnostic.origin, scheduler_core::FailureOrigin::Agent);
            assert!(diagnostic.retryable);
        }

        assert!(preacceptance_rejection_result("unknown", now).is_err());
    }

    #[test]
    fn mtls_certificate_fingerprint_is_bound_to_the_claimed_agent_id() {
        use sha2::{Digest, Sha256};

        let certificate = b"synthetic DER certificate";
        let fingerprint: [u8; 32] = Sha256::digest(certificate).into();
        let allowed = HashMap::from([("node-a".to_owned(), fingerprint)]);
        assert!(authorize_agent_certificate(Some(&allowed), "node-a", Some(certificate)).is_ok());
        assert_eq!(
            authorize_agent_certificate(Some(&allowed), "node-b", Some(certificate))
                .expect_err("unregistered identity")
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            authorize_agent_certificate(Some(&allowed), "node-a", Some(b"different certificate"))
                .expect_err("mismatched identity")
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            authorize_agent_certificate(Some(&allowed), "node-a", None)
                .expect_err("missing certificate")
                .code(),
            tonic::Code::Unauthenticated
        );
    }

    fn test_session(revision: i64) -> AgentSession {
        let (tx, _rx) = mpsc::channel(1);
        AgentSession {
            connection_id: Uuid::new_v4(),
            tx,
            enabled: true,
            labels: BTreeMap::from([("pool".into(), "known-good".into())]),
            capacity: 2,
            applied_settings_revision: revision,
            running: 0,
            health: scheduler_core::health::NodeHealthState::Healthy,
            last_assigned: std::time::Instant::now(),
            environment_bindings: Default::default(),
            secret_file_bindings: Default::default(),
        }
    }

    #[test]
    fn stale_success_and_current_rejection_never_hot_apply_desired_settings() {
        for status in [SettingsAckStatus::Ignored, SettingsAckStatus::Rejected] {
            let mut session = test_session(1);
            apply_settings_ack(
                &mut session,
                SettingsAckOutcome {
                    status,
                    desired_revision: 3,
                    applied_revision: 1,
                    applied_settings: None,
                },
            );
            assert!(session.enabled);
            assert_eq!(session.capacity, 2);
            assert_eq!(session.applied_settings_revision, 1);
            assert_eq!(session.labels["pool"], "known-good");
        }
    }

    #[test]
    fn reconnect_before_current_ack_preserves_only_proven_placement_settings() {
        let desired = NodeSettings {
            revision: 3,
            labels: BTreeMap::from([("pool".into(), "unacknowledged".into())]),
            max_parallel: 99,
            ..NodeSettings::default()
        };
        let previous = test_session(2);

        let known = reconnect_placement(&desired, 2, Some(&previous));
        assert!(known.enabled);
        assert_eq!(known.capacity, 2);
        assert_eq!(known.revision, 2);
        assert_eq!(known.labels["pool"], "known-good");

        let unknown = reconnect_placement(&desired, 2, None);
        assert!(!unknown.enabled);
        assert_eq!(unknown.capacity, 0);
        assert!(unknown.labels.is_empty());
        assert_eq!(unknown.revision, 2);
    }

    #[test]
    fn current_success_hot_applies_the_exact_acknowledged_document() {
        let mut session = test_session(2);
        let applied = NodeSettings {
            revision: 3,
            labels: BTreeMap::from([("pool".into(), "acknowledged".into())]),
            max_parallel: 4,
            ..NodeSettings::default()
        };
        apply_settings_ack(
            &mut session,
            SettingsAckOutcome {
                status: SettingsAckStatus::Applied,
                desired_revision: 3,
                applied_revision: 3,
                applied_settings: Some(applied),
            },
        );
        assert_eq!(session.capacity, 4);
        assert_eq!(session.applied_settings_revision, 3);
        assert_eq!(session.labels["pool"], "acknowledged");
    }

    #[test]
    fn superseded_stream_cannot_mutate_replacement_session() {
        let old = test_session(1);
        let old_connection_id = old.connection_id;
        let mut replacement = test_session(2);
        replacement.running = 5;
        let replacement_connection_id = replacement.connection_id;
        let mut sessions = HashMap::from([("node-a".to_owned(), old)]);
        sessions.insert("node-a".to_owned(), replacement);

        assert!(!is_current_session(&sessions, "node-a", old_connection_id));
        assert!(is_current_session(
            &sessions,
            "node-a",
            replacement_connection_id
        ));
        assert!(!update_running_for_connection(
            &mut sessions,
            "node-a",
            old_connection_id,
            99
        ));
        assert!(!release_slot_for_connection(
            &mut sessions,
            "node-a",
            old_connection_id
        ));
        assert!(!apply_settings_ack_for_connection(
            &mut sessions,
            "node-a",
            old_connection_id,
            SettingsAckOutcome {
                status: SettingsAckStatus::Applied,
                desired_revision: 3,
                applied_revision: 3,
                applied_settings: Some(NodeSettings {
                    revision: 3,
                    labels: BTreeMap::from([("pool".into(), "stale-stream".into())]),
                    max_parallel: 99,
                    ..NodeSettings::default()
                }),
            }
        ));

        let current = &sessions["node-a"];
        assert_eq!(current.connection_id, replacement_connection_id);
        assert_eq!(current.running, 5);
        assert_eq!(current.capacity, 2);
        assert_eq!(current.applied_settings_revision, 2);
        assert_eq!(current.labels["pool"], "known-good");
    }

    #[test]
    fn management_proxy_rejects_a_chunk_that_crosses_the_response_limit() {
        let mut body = vec![b'a'; MANAGEMENT_PROXY_RESPONSE_LIMIT - 2];
        append_management_chunk(&mut body, b"bc").expect("exact limit");
        let error = append_management_chunk(&mut body, b"d").expect_err("over limit");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert_eq!(body.len(), MANAGEMENT_PROXY_RESPONSE_LIMIT);
    }
}
