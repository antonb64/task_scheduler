use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use scheduler_core::{
    ArtifactKind, GlobalSettings, NodeSettings, ResolvedScheduleSnapshot, ScheduleSpec,
    blueprint::{merge_parameters, parse_blueprint_with_defaults},
    resolve_snapshot,
};
use scheduler_protocol::control::{CoordinatorMessage, coordinator_message};
use scheduler_store::{NewRun, NewSchedule, ScheduleRecord};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    auth::{hash_secret, verify_secret},
    state::AppState,
};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route(
            "/api/v1/schedules",
            get(list_schedules).post(create_schedule),
        )
        .route(
            "/api/v1/schedules/{id}",
            get(get_schedule).put(update_schedule),
        )
        .route(
            "/api/v1/schedules/{id}/occurrences",
            get(preview_occurrences),
        )
        .route("/api/v1/schedules/{id}/runs", post(trigger_run))
        .route("/api/v1/schedules/{id}/pause", post(pause_schedule))
        .route("/api/v1/schedules/{id}/resume", post(resume_schedule))
        .route(
            "/api/v1/schedules/{id}/webhook/rotate",
            post(rotate_webhook),
        )
        .route("/api/v1/runs", get(list_runs))
        .route("/api/v1/runs/{id}", get(get_run))
        .route("/api/v1/runs/{id}/attempts", get(run_attempts))
        .route("/api/v1/runs/{id}/events", get(run_events))
        .route("/api/v1/runs/{id}/cancel", post(cancel_run))
        .route("/api/v1/runs/{id}/retry", post(retry_run))
        .route("/api/v1/agents", get(list_agents))
        .route(
            "/api/v1/settings/global",
            get(get_global_settings).put(update_global_settings),
        )
        .route(
            "/api/v1/settings/nodes/{id}",
            get(get_node_settings).put(update_node_settings),
        )
        .route(
            "/api/v1/settings/locks/{key}",
            post(acquire_lock).put(renew_lock).delete(release_lock),
        )
        .route("/hooks/v1/{public_id}", post(webhook))
        .with_state(state)
}

async fn live() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ready(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    sqlx::query("SELECT 1").execute(state.store.pool()).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_schedules(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    Ok(Json(serde_json::to_value(
        state.store.list_schedules().await?,
    )?))
}

async fn get_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let schedule = state
        .store
        .get_schedule(id)
        .await?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(serde_json::to_value(schedule)?))
}

#[derive(Debug, Serialize)]
pub struct ScheduleMutationResponse {
    pub schedule: scheduler_core::ScheduleView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook_secret: Option<String>,
}

async fn create_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(spec): Json<ScheduleSpec>,
) -> Result<(StatusCode, Json<ScheduleMutationResponse>), ApiError> {
    authorize(&state, &headers)?;
    validate_schedule_spec(&spec)?;
    let (encrypted, digest) = resolve_and_encrypt(&state, &spec).await?;
    let (public_id, secret, secret_hash) = if spec.webhook_enabled {
        let secret = new_secret();
        (
            Some(Uuid::new_v4().to_string()),
            Some(secret.clone()),
            Some(hash_secret(&secret)?),
        )
    } else {
        (None, None, None)
    };
    let schedule = state
        .store
        .create_schedule(NewSchedule {
            id: Uuid::new_v4(),
            spec,
            encrypted_snapshot: encrypted,
            snapshot_digest: digest,
            key_id: state.cipher.key_id().into(),
            webhook_public_id: public_id,
            webhook_secret_hash: secret_hash,
        })
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(ScheduleMutationResponse {
            schedule,
            webhook_secret: secret,
        }),
    ))
}

#[derive(Debug, Deserialize)]
struct UpdateScheduleRequest {
    expected_revision: i64,
    spec: ScheduleSpec,
}

async fn update_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateScheduleRequest>,
) -> Result<Json<ScheduleMutationResponse>, ApiError> {
    authorize(&state, &headers)?;
    validate_schedule_spec(&request.spec)?;
    let (encrypted, digest) = resolve_and_encrypt(&state, &request.spec).await?;
    let schedule = state
        .store
        .update_schedule(
            id,
            request.expected_revision,
            request.spec,
            encrypted,
            digest,
            state.cipher.key_id().into(),
        )
        .await?;
    Ok(Json(ScheduleMutationResponse {
        schedule,
        webhook_secret: None,
    }))
}

async fn preview_occurrences(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let schedule = state
        .store
        .get_schedule(id)
        .await?
        .ok_or_else(ApiError::not_found)?;
    let cron = schedule
        .spec
        .cron
        .as_ref()
        .context("schedule has no cron trigger")?;
    let occurrences = scheduler_core::schedule::next_occurrences(cron, Utc::now(), 5)?;
    Ok(Json(serde_json::json!({"occurrences": occurrences})))
}

#[derive(Debug, Default, Deserialize)]
pub struct TriggerRunRequest {
    #[serde(default)]
    pub parameters: Value,
    pub run_at: Option<DateTime<Utc>>,
}

async fn trigger_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(request): Json<TriggerRunRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    authorize(&state, &headers)?;
    let idempotency = idempotency_key(&headers);
    if let Some(key) = &idempotency {
        if let Some(run) = state.store.get_run_by_idempotency(id, key).await? {
            return Ok((StatusCode::OK, Json(serde_json::to_value(run)?)));
        }
    }
    let schedule = state
        .store
        .get_schedule_record(id)
        .await?
        .ok_or_else(ApiError::not_found)?;
    let overrides = normalize_overrides(request.parameters)?;
    let run = create_run_from_schedule(
        &state,
        &schedule,
        &overrides,
        "manual",
        request.run_at.unwrap_or_else(Utc::now),
        idempotency,
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(serde_json::to_value(run)?)))
}

async fn webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(public_id): Path<String>,
    Json(parameters): Json<Value>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let schedule = state
        .store
        .get_schedule_by_public_id(&public_id)
        .await?
        .ok_or_else(ApiError::not_found)?;
    let candidate = bearer_token(&headers).ok_or_else(ApiError::unauthorized)?;
    if !schedule
        .webhook_secret_hash
        .as_deref()
        .is_some_and(|hash| verify_secret(hash, candidate))
    {
        return Err(ApiError::unauthorized());
    }
    let idempotency = idempotency_key(&headers);
    if let Some(key) = &idempotency {
        if let Some(run) = state
            .store
            .get_run_by_idempotency(schedule.view.id, key)
            .await?
        {
            return Ok((StatusCode::OK, Json(serde_json::to_value(run)?)));
        }
    }
    let run = create_run_from_schedule(
        &state,
        &schedule,
        &normalize_overrides(parameters)?,
        "webhook",
        Utc::now(),
        idempotency,
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(serde_json::to_value(run)?)))
}

async fn pause_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    authorize(&state, &headers)?;
    state.store.set_schedule_enabled(id, false).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn resume_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    authorize(&state, &headers)?;
    state.store.set_schedule_enabled(id, true).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn rotate_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let secret = new_secret();
    let public_id = Uuid::new_v4().to_string();
    state
        .store
        .rotate_webhook(id, public_id.clone(), hash_secret(&secret)?)
        .await?;
    Ok(Json(
        serde_json::json!({"public_id": public_id, "secret": secret}),
    ))
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<u32>,
}

async fn list_runs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    Ok(Json(serde_json::to_value(
        state.store.list_runs(query.limit.unwrap_or(100)).await?,
    )?))
}

async fn get_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    Ok(Json(serde_json::to_value(
        state
            .store
            .get_run(id)
            .await?
            .ok_or_else(ApiError::not_found)?,
    )?))
}

async fn run_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    Ok(Json(serde_json::to_value(
        state
            .store
            .audit_events("run", &id.to_string(), 500)
            .await?,
    )?))
}

async fn run_attempts(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if state.store.get_run(id).await?.is_none() {
        return Err(ApiError::not_found());
    }
    Ok(Json(serde_json::to_value(
        state.store.run_attempts(id).await?,
    )?))
}

async fn cancel_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    authorize(&state, &headers)?;
    let attempts = state.store.cancel_run(id).await?;
    for (agent_id, attempt_id) in attempts {
        state
            .send_to_agent(
                &agent_id,
                CoordinatorMessage {
                    payload: Some(coordinator_message::Payload::Cancel(
                        scheduler_protocol::control::CancelAttempt {
                            attempt_id: attempt_id.to_string(),
                        },
                    )),
                },
            )
            .await;
    }
    Ok(StatusCode::ACCEPTED)
}

async fn retry_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    authorize(&state, &headers)?;
    state.store.retry_run(id).await?;
    Ok(StatusCode::ACCEPTED)
}

async fn list_agents(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    Ok(Json(serde_json::to_value(
        state.store.list_agents().await?,
    )?))
}

async fn get_global_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Json<GlobalSettings>), ApiError> {
    authorize(&state, &headers)?;
    let settings = state.store.get_global_settings().await?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ETAG, format!("\"{}\"", settings.revision).parse()?);
    Ok((response_headers, Json(settings)))
}

async fn get_node_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<(HeaderMap, Json<NodeSettings>), ApiError> {
    authorize(&state, &headers)?;
    let settings = state
        .store
        .get_node_settings(&id)
        .await?
        .ok_or_else(ApiError::not_found)?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ETAG, format!("\"{}\"", settings.revision).parse()?);
    Ok((response_headers, Json(settings)))
}

#[derive(Debug, Deserialize)]
struct UpdateSettingsRequest {
    expected_revision: i64,
    lock_token: String,
    document: Value,
}

async fn update_global_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<UpdateSettingsRequest>,
) -> Result<(HeaderMap, Json<Value>), ApiError> {
    authorize(&state, &headers)?;
    require_if_match(&headers, request.expected_revision)?;
    let settings: GlobalSettings =
        serde_json::from_value(request.document.clone()).context("invalid global settings")?;
    settings.validate()?;
    let revision = state
        .store
        .update_settings(
            "global",
            request.expected_revision,
            &serde_json::to_string(&request.document)?,
            &request.lock_token,
        )
        .await?;
    state.push_all_node_settings().await?;
    settings_update_response(revision)
}

async fn update_node_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<UpdateSettingsRequest>,
) -> Result<(HeaderMap, Json<Value>), ApiError> {
    authorize(&state, &headers)?;
    require_if_match(&headers, request.expected_revision)?;
    let _: NodeSettings =
        serde_json::from_value(request.document.clone()).context("invalid node settings")?;
    let key = format!("node:{id}");
    let revision = state
        .store
        .update_settings(
            &key,
            request.expected_revision,
            &serde_json::to_string(&request.document)?,
            &request.lock_token,
        )
        .await?;
    state.push_node_settings(&id).await?;
    settings_update_response(revision)
}

#[derive(Debug, Deserialize)]
struct LockRequest {
    owner_session: String,
    lock_token: Option<String>,
    force: Option<bool>,
}

async fn acquire_lock(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(key): Path<String>,
    Json(request): Json<LockRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    Ok(Json(serde_json::to_value(
        state
            .store
            .acquire_lock(&key, &request.owner_session)
            .await?,
    )?))
}

async fn renew_lock(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(key): Path<String>,
    Json(request): Json<LockRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let expires = state
        .store
        .renew_lock(
            &key,
            request
                .lock_token
                .as_deref()
                .context("lock_token required")?,
        )
        .await?;
    Ok(Json(serde_json::json!({"expires_at": expires})))
}

async fn release_lock(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(key): Path<String>,
    Json(request): Json<LockRequest>,
) -> Result<StatusCode, ApiError> {
    authorize(&state, &headers)?;
    state
        .store
        .release_lock(
            &key,
            request.lock_token.as_deref().unwrap_or_default(),
            request.force.unwrap_or(false),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn resolve_and_encrypt(
    state: &AppState,
    spec: &ScheduleSpec,
) -> Result<(Vec<u8>, String)> {
    let blueprint_artifact = state
        .adapters
        .fetch(&spec.blueprint_ref.uri, ArtifactKind::Blueprint)
        .await?;
    let parameters_artifact = state
        .adapters
        .fetch(&spec.parameters_ref.uri, ArtifactKind::Parameters)
        .await?;
    let defaults = state.store.get_global_settings().await?;
    let blueprint = parse_blueprint_with_defaults(
        &blueprint_artifact.bytes,
        blueprint_artifact.media_type.as_deref(),
        defaults.default_max_attempts,
        defaults.default_timeout_seconds,
    )?;
    let parameters: Value =
        serde_json::from_slice(&parameters_artifact.bytes).context("invalid parameters JSON")?;
    scheduler_core::validate_parameters(&blueprint.parameters_schema, &parameters)?;
    let resolved = ResolvedScheduleSnapshot {
        blueprint,
        base_parameters: parameters,
        blueprint_source_version: blueprint_artifact.source_version,
        parameters_source_version: parameters_artifact.source_version,
    };
    let plaintext = serde_json::to_vec(&resolved)?;
    let digest = hex::encode(Sha256::digest(&plaintext));
    Ok((state.cipher.encrypt(&plaintext)?, digest))
}

pub async fn create_run_from_schedule(
    state: &AppState,
    schedule: &ScheduleRecord,
    overrides: &Value,
    trigger_kind: &str,
    scheduled_at: DateTime<Utc>,
    idempotency_key: Option<String>,
) -> Result<scheduler_core::RunView> {
    if !schedule.view.spec.enabled {
        bail!("schedule is paused");
    }
    let plaintext = state.cipher.decrypt(&schedule.encrypted_snapshot)?;
    let resolved: ResolvedScheduleSnapshot = serde_json::from_slice(&plaintext)?;
    let parameters = merge_parameters(&resolved.base_parameters, overrides)?;
    let snapshot = resolve_snapshot(&resolved, &parameters, &schedule.view.spec.required_labels)?;
    let encrypted = state.cipher.encrypt(&serde_json::to_vec(&snapshot)?)?;
    state
        .store
        .create_run(NewRun {
            id: Uuid::new_v4(),
            schedule_id: schedule.view.id,
            trigger_kind: trigger_kind.into(),
            scheduled_at,
            encrypted_snapshot: encrypted,
            key_id: state.cipher.key_id().into(),
            max_attempts: snapshot.policy.max_attempts,
            initial_backoff_seconds: snapshot.policy.initial_backoff_seconds,
            backoff_cap_seconds: snapshot.policy.backoff_cap_seconds,
            idempotency_key,
        })
        .await
}

fn validate_schedule_spec(spec: &ScheduleSpec) -> Result<()> {
    if spec.name.trim().is_empty() {
        bail!("schedule name is required");
    }
    if let Some(cron) = &spec.cron {
        scheduler_core::schedule::parse_cron(cron)?;
    }
    Ok(())
}

fn normalize_overrides(value: Value) -> Result<Value> {
    if value.is_null() {
        Ok(serde_json::json!({}))
    } else if value.is_object() {
        Ok(value)
    } else {
        bail!("parameter overrides must be a JSON object")
    }
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    if state.auth.verify_bearer(headers) {
        Ok(())
    } else {
        Err(ApiError::unauthorized())
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

fn idempotency_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn require_if_match(headers: &HeaderMap, expected_revision: i64) -> Result<(), ApiError> {
    let raw = headers
        .get(header::IF_MATCH)
        .ok_or_else(|| ApiError::precondition_required("If-Match is required"))?
        .to_str()
        .map_err(|_| ApiError::precondition_failed("If-Match is invalid"))?;
    let revision =
        raw.trim().trim_matches('"').parse::<i64>().map_err(|_| {
            ApiError::precondition_failed("If-Match must contain a settings revision")
        })?;
    if revision != expected_revision {
        return Err(ApiError::precondition_failed(
            "If-Match does not match expected_revision",
        ));
    }
    Ok(())
}

fn settings_update_response(revision: i64) -> Result<(HeaderMap, Json<Value>), ApiError> {
    let mut headers = HeaderMap::new();
    headers.insert(header::ETAG, format!("\"{revision}\"").parse()?);
    Ok((headers, Json(serde_json::json!({"revision": revision}))))
}

fn new_secret() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "authentication_required",
            message: "authentication required".into(),
        }
    }

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "resource_not_found",
            message: "resource not found".into(),
        }
    }

    fn precondition_required(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PRECONDITION_REQUIRED,
            code: "precondition_required",
            message: message.into(),
        }
    }

    fn precondition_failed(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PRECONDITION_FAILED,
            code: "precondition_failed",
            message: message.into(),
        }
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(error: E) -> Self {
        let error = error.into();
        let message = format!("{error:#}");
        let (status, code) =
            if message.contains("conflict") || message.contains("currently being edited") {
                (StatusCode::CONFLICT, "conflict")
            } else if message.contains("not found") {
                (StatusCode::NOT_FOUND, "resource_not_found")
            } else if message.contains("parameters failed validation") {
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "parameter_validation_failed",
                )
            } else if message.contains("artifact") || message.contains("blueprint") {
                (StatusCode::BAD_REQUEST, "artifact_resolution_failed")
            } else {
                (StatusCode::BAD_REQUEST, "invalid_request")
            };
        Self {
            status,
            code,
            message,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({
                "error": self.message,
                "code": self.code,
                "status": self.status.as_u16(),
            })),
        )
            .into_response()
    }
}

pub async fn materialize_cron(state: &AppState) -> Result<()> {
    let now = Utc::now();
    for schedule in state.store.cron_schedules().await? {
        let Some(cron) = schedule.view.spec.cron.as_ref() else {
            continue;
        };
        let mut cursor = schedule.last_cron_at.unwrap_or(schedule.view.created_at);
        for _ in 0..1_000 {
            let Some(next) = scheduler_core::schedule::next_occurrences(cron, cursor, 1)?
                .into_iter()
                .next()
            else {
                break;
            };
            if next > now {
                break;
            }
            create_run_from_schedule(state, &schedule, &serde_json::json!({}), "cron", next, None)
                .await?;
            state
                .store
                .advance_cron_cursor(schedule.view.id, next)
                .await?;
            cursor = next;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_updates_require_a_matching_if_match_revision() {
        let mut headers = HeaderMap::new();
        let missing = require_if_match(&headers, 7).expect_err("header required");
        assert_eq!(missing.status, StatusCode::PRECONDITION_REQUIRED);
        assert_eq!(missing.code, "precondition_required");

        headers.insert(header::IF_MATCH, "\"7\"".parse().expect("header"));
        require_if_match(&headers, 7).expect("matching revision");
        let stale = require_if_match(&headers, 8).expect_err("mismatch");
        assert_eq!(stale.status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(stale.code, "precondition_failed");
    }
}
