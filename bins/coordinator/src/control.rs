use std::{pin::Pin, time::Instant};

use futures::{Stream, StreamExt};
use scheduler_core::ExecutionResult;
use scheduler_protocol::control::{
    AgentMessage, CoordinatorMessage, ManagementRequest, ManagementResponse, SettingsUpdate,
    agent_message, coordinator_message, scheduler_control_server::SchedulerControl,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};
use uuid::Uuid;

use crate::state::{AgentSession, AppState};

#[derive(Clone)]
pub struct ControlService {
    state: AppState,
    client: reqwest::Client,
}

impl ControlService {
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            // Preserve coordinator redirects for the browser. Following them here
            // would render the right page while leaving the agent-UI browser on a
            // POST-only `/ui/...` URL, which then breaks refresh and navigation.
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("static management proxy client configuration is valid"),
        }
    }
}

type MessageStream = Pin<Box<dyn Stream<Item = Result<CoordinatorMessage, Status>> + Send>>;

#[tonic::async_trait]
impl SchedulerControl for ControlService {
    type AgentStreamStream = MessageStream;

    async fn agent_stream(
        &self,
        request: Request<tonic::Streaming<AgentMessage>>,
    ) -> Result<Response<Self::AgentStreamStream>, Status> {
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
        if hello.agent_id.trim().is_empty() {
            return Err(Status::invalid_argument("agent_id cannot be empty"));
        }
        let settings = self
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
        let (tx, rx) = mpsc::channel(256);
        self.state.sessions.write().await.insert(
            hello.agent_id.clone(),
            AgentSession {
                tx: tx.clone(),
                enabled: settings.enabled,
                labels: settings.labels.clone(),
                capacity: settings.max_parallel,
                running: hello.running,
                last_assigned: Instant::now(),
            },
        );
        let heartbeat_seconds = self
            .state
            .store
            .get_global_settings()
            .await
            .map_err(internal)?
            .heartbeat_seconds;
        tx.send(Ok(CoordinatorMessage {
            payload: Some(coordinator_message::Payload::Settings(SettingsUpdate {
                revision: settings.revision,
                settings_json: serde_json::to_string(&settings).map_err(internal)?,
                heartbeat_seconds,
            })),
        }))
        .await
        .map_err(|_| Status::unavailable("agent response stream closed"))?;

        let state = self.state.clone();
        let agent_id = hello.agent_id.clone();
        tokio::spawn(async move {
            info!(agent_id, "agent connected");
            while let Some(message) = incoming.next().await {
                match message {
                    Ok(message) => {
                        if let Err(error) =
                            handle_agent_message(&state, &agent_id, &tx, message).await
                        {
                            warn!(agent_id, error = %error, "agent message rejected");
                        }
                    }
                    Err(error) => {
                        warn!(agent_id, error = %error, "agent stream failed");
                        break;
                    }
                }
            }
            state.sessions.write().await.remove(&agent_id);
            let _ = state.store.disconnect_agent(&agent_id).await;
            info!(agent_id, "agent disconnected");
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

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
                "content-type" | "cookie" | "if-match" | "idempotency-key"
            ) {
                outbound = outbound.header(name, value);
            }
        }
        if request.path.starts_with("/api/") {
            outbound = outbound.bearer_auth(&self.state.internal_admin_token);
        }
        let response = outbound.send().await.map_err(internal)?;
        let status = response.status().as_u16() as u32;
        let mut headers = std::collections::HashMap::new();
        for name in ["content-type", "location", "set-cookie", "etag"] {
            if let Some(value) = response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
            {
                headers.insert(name.to_owned(), value.to_owned());
            }
        }
        let body = response.text().await.map_err(internal)?;
        Ok(Response::new(ManagementResponse {
            status,
            body,
            headers,
        }))
    }
}

async fn handle_agent_message(
    state: &AppState,
    expected_agent_id: &str,
    tx: &mpsc::Sender<Result<CoordinatorMessage, Status>>,
    message: AgentMessage,
) -> anyhow::Result<()> {
    match message.payload {
        Some(agent_message::Payload::Heartbeat(heartbeat)) => {
            ensure_agent(expected_agent_id, &heartbeat.agent_id)?;
            let lease_seconds = state.store.get_global_settings().await?.lease_seconds;
            state
                .store
                .renew_attempts(
                    expected_agent_id,
                    &heartbeat.active_attempt_ids,
                    lease_seconds,
                    heartbeat.running,
                )
                .await?;
            if let Some(session) = state.sessions.write().await.get_mut(expected_agent_id) {
                session.running = heartbeat.running;
            }
        }
        Some(agent_message::Payload::Accepted(accepted)) => {
            ensure_agent(expected_agent_id, &accepted.agent_id)?;
            let lease_seconds = state.store.get_global_settings().await?.lease_seconds;
            state
                .store
                .accept_attempt(
                    Uuid::parse_str(&accepted.attempt_id)?,
                    &accepted.lease_token,
                    lease_seconds,
                )
                .await?;
        }
        Some(agent_message::Payload::Result(result)) => {
            ensure_agent(expected_agent_id, &result.agent_id)?;
            let attempt_id = Uuid::parse_str(&result.attempt_id)?;
            let parsed: ExecutionResult = serde_json::from_str(&result.result_json)?;
            let outcome = parsed.outcome.as_str().to_owned();
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
            opentelemetry::global::meter("scheduler-coordinator")
                .u64_counter("scheduler.attempt.results")
                .build()
                .add(
                    1,
                    &[opentelemetry::KeyValue::new("outcome", outcome.clone())],
                );
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
            state.release_agent_slot(expected_agent_id).await;
            tx.send(Ok(CoordinatorMessage {
                payload: Some(coordinator_message::Payload::ResultAcknowledged(
                    scheduler_protocol::control::ResultAcknowledged {
                        attempt_id: attempt_id.to_string(),
                    },
                )),
            }))
            .await?;
        }
        Some(agent_message::Payload::SettingsApplied(applied)) => {
            ensure_agent(expected_agent_id, &applied.agent_id)?;
            state
                .store
                .settings_applied(
                    expected_agent_id,
                    applied.revision,
                    applied.error.as_deref(),
                )
                .await?;
            if applied.error.is_none() {
                if let Some(settings) = state.store.get_node_settings(expected_agent_id).await? {
                    if let Some(session) = state.sessions.write().await.get_mut(expected_agent_id) {
                        session.enabled = settings.enabled;
                        session.labels = settings.labels;
                        session.capacity = settings.max_parallel;
                    }
                }
            }
        }
        Some(agent_message::Payload::Hello(_)) => anyhow::bail!("hello may only be sent once"),
        None => anyhow::bail!("empty agent message"),
    }
    Ok(())
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
