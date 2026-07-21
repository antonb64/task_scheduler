use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use scheduler_core::{
    ArtifactFetchError, ArtifactKind, GlobalSettings, NodeSettings, ResolvedScheduleSnapshot,
    ScheduleSpec,
    blueprint::{merge_parameters, parse_blueprint_with_defaults},
    resolve_snapshot,
};
use scheduler_protocol::control::{CoordinatorMessage, coordinator_message};
use scheduler_store::{
    CronBatchOccurrenceResult, CronOccurrenceResult, NewBlueprintRevision, NewRun, NewSchedule,
    ScheduleRecord,
};
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
        .route(
            "/api/v1/batches",
            get(crate::collection_runtime::list_batches),
        )
        .route(
            "/api/v1/batches/{id}",
            get(crate::collection_runtime::get_batch),
        )
        .route(
            "/api/v1/batches/{id}/items",
            get(crate::collection_runtime::list_batch_items),
        )
        .route(
            "/api/v1/batches/{id}/cancel",
            post(crate::collection_runtime::cancel_batch),
        )
        .route(
            "/api/v1/batches/{id}/retrigger",
            post(crate::collection_runtime::retrigger_batch),
        )
        .route("/api/v1/agents", get(list_agents))
        .route("/api/v1/blueprints", get(list_blueprints))
        .route("/api/v1/telemetry/status", get(telemetry_status))
        .route(
            "/api/v1/dashboard",
            get(get_dashboard).put(update_dashboard),
        )
        .route("/api/v1/agents/{id}/health", get(get_agent_health))
        .route(
            "/api/v1/agents/{id}/health/evidence",
            get(get_agent_health_evidence),
        )
        .route(
            "/api/v1/agents/{id}/health/quarantine",
            post(quarantine_agent),
        )
        .route("/api/v1/agents/{id}/health/reset", post(reset_agent_health))
        .route(
            "/api/v1/input-health/{blueprint_digest}/{input_fingerprint}/probe",
            post(grant_input_probe),
        )
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

async fn telemetry_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    Ok(Json(serde_json::to_value(scheduler_telemetry::status())?))
}

async fn get_agent_health(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    scheduler_core::validate_agent_id(&id)?;
    let view = state
        .store
        .node_health(&id)
        .await?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(serde_json::to_value(view)?))
}

async fn get_agent_health_evidence(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    scheduler_core::validate_agent_id(&id)?;
    let limit = page_limit(query.limit);
    let cursor =
        decode_time_uuid_cursor(query.cursor.as_deref(), ApiCursorKind::AgentHealthEvidence)?;
    let mut items = state
        .store
        .list_health_evidence_page(
            Some(&id),
            cursor.as_ref().map(|cursor| cursor.timestamp.as_str()),
            cursor.as_ref().map(|cursor| cursor.id.as_str()),
            limit + 1,
        )
        .await?;
    let has_more = items.len() > limit as usize;
    items.truncate(limit as usize);
    let next_cursor = has_more
        .then(|| {
            items.last().map(|item| {
                encode_api_cursor(
                    ApiCursorKind::AgentHealthEvidence,
                    &TimeCursor {
                        timestamp: item
                            .occurred_at
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        id: item.id.to_string(),
                    },
                )
            })
        })
        .flatten()
        .transpose()?;
    cursor_page(items, next_cursor)
}

async fn quarantine_agent(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    scheduler_core::validate_agent_id(&id)?;
    let view = state.store.set_node_manual_quarantine(&id, true).await?;
    state.apply_agent_health_view(&view).await;
    Ok(Json(serde_json::to_value(view)?))
}

async fn reset_agent_health(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    scheduler_core::validate_agent_id(&id)?;
    let view = state.store.set_node_manual_quarantine(&id, false).await?;
    state.apply_agent_health_view(&view).await;
    Ok(Json(serde_json::to_value(view)?))
}

async fn grant_input_probe(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((blueprint_digest, input_fingerprint)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if !is_hex_digest(&blueprint_digest) || !is_hex_digest(&input_fingerprint) {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_health_fingerprint",
            message: "health digests must contain exactly 64 hexadecimal characters".into(),
            request_id: None,
        });
    }
    Ok(Json(serde_json::to_value(
        state
            .store
            .grant_input_probe(&blueprint_digest, &input_fingerprint)
            .await?,
    )?))
}

fn is_hex_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn list_schedules(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let limit = page_limit(query.limit);
    let cursor = decode_time_uuid_cursor(query.cursor.as_deref(), ApiCursorKind::Schedules)?;
    let mut items = state
        .store
        .list_schedules_page(
            cursor.as_ref().map(|cursor| cursor.timestamp.as_str()),
            cursor.as_ref().map(|cursor| cursor.id.as_str()),
            limit + 1,
        )
        .await?;
    let has_more = items.len() > limit as usize;
    items.truncate(limit as usize);
    let next_cursor = has_more
        .then(|| {
            items.last().map(|item| {
                encode_api_cursor(
                    ApiCursorKind::Schedules,
                    &TimeCursor {
                        timestamp: item
                            .created_at
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        id: item.id.to_string(),
                    },
                )
            })
        })
        .flatten()
        .transpose()?;
    cursor_page(items, next_cursor)
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
        if let Some(batch) =
            crate::collection_runtime::batch_by_idempotency(&state, id, key).await?
        {
            return Ok((
                StatusCode::OK,
                Json(serde_json::json!({"kind": "batch", "batch": batch})),
            ));
        }
    }
    let schedule = state
        .store
        .get_schedule_record(id)
        .await?
        .ok_or_else(ApiError::not_found)?;
    let overrides = normalize_overrides(request.parameters)?;
    if schedule.view.spec.parameter_collection.is_some() {
        let batch = crate::collection_runtime::create_batch_from_schedule(
            &state,
            &schedule,
            &overrides,
            "manual",
            request.run_at.unwrap_or_else(Utc::now),
            idempotency,
        )
        .await?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({"kind": "batch", "batch": batch})),
        ));
    }
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
        if let Some(batch) =
            crate::collection_runtime::batch_by_idempotency(&state, schedule.view.id, key).await?
        {
            return Ok((
                StatusCode::OK,
                Json(serde_json::json!({"kind": "batch", "batch": batch})),
            ));
        }
    }
    let overrides = normalize_overrides(parameters)?;
    if schedule.view.spec.parameter_collection.is_some() {
        let batch = crate::collection_runtime::create_batch_from_schedule(
            &state,
            &schedule,
            &overrides,
            "webhook",
            Utc::now(),
            idempotency,
        )
        .await?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!({"kind": "batch", "batch": batch})),
        ));
    }
    let run = create_run_from_schedule(
        &state,
        &schedule,
        &overrides,
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
    cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct CursorPage<T> {
    items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

const API_CURSOR_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ApiCursorKind {
    AgentHealthEvidence,
    Schedules,
    Runs,
    RunEvents,
    RunAttempts,
    Agents,
    Blueprints,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiCursorEnvelope<T> {
    version: u8,
    kind: ApiCursorKind,
    position: T,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct TimeCursor {
    timestamp: String,
    id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct IdCursor {
    id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AttemptCursor {
    attempt_number: u32,
    id: String,
}

fn page_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(50).clamp(1, 200)
}

fn encode_api_cursor(kind: ApiCursorKind, position: &impl Serialize) -> Result<String> {
    Ok(
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&ApiCursorEnvelope {
            version: API_CURSOR_VERSION,
            kind,
            position,
        })?),
    )
}

fn decode_api_cursor<T: for<'de> Deserialize<'de>>(
    cursor: Option<&str>,
    expected_kind: ApiCursorKind,
) -> Result<Option<T>> {
    cursor
        .map(|cursor| {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .context("invalid pagination cursor")?;
            let envelope: ApiCursorEnvelope<T> =
                serde_json::from_slice(&bytes).context("invalid pagination cursor")?;
            if envelope.version != API_CURSOR_VERSION || envelope.kind != expected_kind {
                bail!("invalid pagination cursor");
            }
            Ok(envelope.position)
        })
        .transpose()
}

fn decode_time_uuid_cursor(
    cursor: Option<&str>,
    kind: ApiCursorKind,
) -> Result<Option<TimeCursor>> {
    let cursor: Option<TimeCursor> = decode_api_cursor(cursor, kind)?;
    if let Some(cursor) = cursor.as_ref() {
        validate_cursor_timestamp(&cursor.timestamp)?;
        validate_cursor_uuid(&cursor.id)?;
    }
    Ok(cursor)
}

fn decode_blueprint_cursor(cursor: Option<&str>) -> Result<Option<TimeCursor>> {
    let cursor: Option<TimeCursor> = decode_api_cursor(cursor, ApiCursorKind::Blueprints)?;
    if let Some(cursor) = cursor.as_ref() {
        validate_cursor_timestamp(&cursor.timestamp)?;
        if !is_canonical_digest(&cursor.id) {
            bail!("invalid pagination cursor");
        }
    }
    Ok(cursor)
}

fn decode_agent_cursor(cursor: Option<&str>) -> Result<Option<IdCursor>> {
    let cursor: Option<IdCursor> = decode_api_cursor(cursor, ApiCursorKind::Agents)?;
    if let Some(cursor) = cursor.as_ref()
        && scheduler_core::validate_agent_id(&cursor.id).is_err()
    {
        bail!("invalid pagination cursor");
    }
    Ok(cursor)
}

fn decode_event_cursor(cursor: Option<&str>) -> Result<Option<i64>> {
    decode_api_cursor::<IdCursor>(cursor, ApiCursorKind::RunEvents)?
        .map(|cursor| {
            let id = cursor
                .id
                .parse::<i64>()
                .context("invalid pagination cursor")?;
            if id <= 0 || id.to_string() != cursor.id {
                bail!("invalid pagination cursor");
            }
            Ok(id)
        })
        .transpose()
}

fn decode_attempt_cursor(cursor: Option<&str>) -> Result<Option<AttemptCursor>> {
    let cursor: Option<AttemptCursor> = decode_api_cursor(cursor, ApiCursorKind::RunAttempts)?;
    if let Some(cursor) = cursor.as_ref() {
        if cursor.attempt_number == 0 {
            bail!("invalid pagination cursor");
        }
        validate_cursor_uuid(&cursor.id)?;
    }
    Ok(cursor)
}

fn validate_cursor_timestamp(timestamp: &str) -> Result<()> {
    let parsed = DateTime::parse_from_rfc3339(timestamp).context("invalid pagination cursor")?;
    let canonical = parsed
        .with_timezone(&Utc)
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    if canonical != timestamp {
        bail!("invalid pagination cursor");
    }
    Ok(())
}

fn validate_cursor_uuid(id: &str) -> Result<()> {
    let parsed = Uuid::parse_str(id).context("invalid pagination cursor")?;
    if parsed.to_string() != id {
        bail!("invalid pagination cursor");
    }
    Ok(())
}

fn is_canonical_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn cursor_page<T: Serialize>(
    items: Vec<T>,
    next_cursor: Option<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(serde_json::to_value(CursorPage {
        items,
        next_cursor,
    })?))
}

async fn list_runs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let limit = page_limit(query.limit);
    let cursor = decode_time_uuid_cursor(query.cursor.as_deref(), ApiCursorKind::Runs)?;
    let mut items = state
        .store
        .list_runs_page(
            cursor.as_ref().map(|cursor| cursor.timestamp.as_str()),
            cursor.as_ref().map(|cursor| cursor.id.as_str()),
            limit + 1,
        )
        .await?;
    let has_more = items.len() > limit as usize;
    items.truncate(limit as usize);
    let next_cursor = has_more
        .then(|| {
            items.last().map(|item| {
                encode_api_cursor(
                    ApiCursorKind::Runs,
                    &TimeCursor {
                        timestamp: item
                            .created_at
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        id: item.id.to_string(),
                    },
                )
            })
        })
        .flatten()
        .transpose()?;
    cursor_page(items, next_cursor)
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
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if state.store.get_run(id).await?.is_none() {
        return Err(ApiError::not_found());
    }
    let limit = page_limit(query.limit);
    let cursor_id = decode_event_cursor(query.cursor.as_deref())?;
    let mut items = state
        .store
        .audit_events_page("run", &id.to_string(), cursor_id, limit + 1)
        .await?;
    let has_more = items.len() > limit as usize;
    items.truncate(limit as usize);
    let next_cursor = has_more
        .then(|| {
            items.last().map(|item| {
                let id = item["id"].as_i64().context("audit event ID is malformed")?;
                encode_api_cursor(ApiCursorKind::RunEvents, &IdCursor { id: id.to_string() })
            })
        })
        .flatten()
        .transpose()?;
    cursor_page(items, next_cursor)
}

async fn run_attempts(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if state.store.get_run(id).await?.is_none() {
        return Err(ApiError::not_found());
    }
    let limit = page_limit(query.limit);
    let cursor = decode_attempt_cursor(query.cursor.as_deref())?;
    let mut items = state
        .store
        .run_attempts_page(
            id,
            cursor.as_ref().map(|cursor| cursor.attempt_number),
            cursor.as_ref().map(|cursor| cursor.id.as_str()),
            limit + 1,
        )
        .await?;
    let has_more = items.len() > limit as usize;
    items.truncate(limit as usize);
    let next_cursor = has_more
        .then(|| {
            items.last().map(|item| {
                encode_api_cursor(
                    ApiCursorKind::RunAttempts,
                    &AttemptCursor {
                        attempt_number: item.attempt_number,
                        id: item.id.to_string(),
                    },
                )
            })
        })
        .flatten()
        .transpose()?;
    cursor_page(items, next_cursor)
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
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let limit = page_limit(query.limit);
    let cursor = decode_agent_cursor(query.cursor.as_deref())?;
    let mut items = state
        .store
        .list_agents_page(cursor.as_ref().map(|cursor| cursor.id.as_str()), limit + 1)
        .await?;
    let has_more = items.len() > limit as usize;
    items.truncate(limit as usize);
    let next_cursor = has_more
        .then(|| {
            items.last().map(|item| {
                encode_api_cursor(
                    ApiCursorKind::Agents,
                    &IdCursor {
                        id: item.id.clone(),
                    },
                )
            })
        })
        .flatten()
        .transpose()?;
    cursor_page(items, next_cursor)
}

async fn list_blueprints(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let limit = page_limit(query.limit);
    let cursor = decode_blueprint_cursor(query.cursor.as_deref())?;
    let mut items = state
        .store
        .list_blueprint_revisions_page(
            cursor.as_ref().map(|cursor| cursor.timestamp.as_str()),
            cursor.as_ref().map(|cursor| cursor.id.as_str()),
            limit + 1,
        )
        .await?;
    let has_more = items.len() > limit as usize;
    items.truncate(limit as usize);
    let next_cursor = has_more
        .then(|| {
            items.last().map(|item| {
                encode_api_cursor(
                    ApiCursorKind::Blueprints,
                    &TimeCursor {
                        timestamp: item
                            .loaded_at
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                        id: item.digest.clone(),
                    },
                )
            })
        })
        .flatten()
        .transpose()?;
    cursor_page(items, next_cursor)
}

async fn get_dashboard(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<(HeaderMap, Json<Value>), ApiError> {
    authorize(&state, &headers)?;
    let dashboard = state.store.get_dashboard().await?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ETAG, format!("\"{}\"", dashboard.revision).parse()?);
    Ok((response_headers, Json(serde_json::to_value(dashboard)?)))
}

async fn update_dashboard(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<UpdateSettingsRequest>,
) -> Result<(HeaderMap, Json<Value>), ApiError> {
    authorize(&state, &headers)?;
    require_if_match(&headers, request.expected_revision)?;
    let config: scheduler_core::DashboardConfig =
        serde_json::from_value(request.document).context("invalid dashboard configuration")?;
    config.validate()?;
    let revision = state
        .store
        .update_dashboard(&config, request.expected_revision, &request.lock_token)
        .await?;
    settings_update_response(revision)
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
    if !parameters.is_object() {
        bail!("parameters must be a JSON object");
    }
    if spec.parameter_collection.is_none() && blueprint.parameter_bindings.is_empty() {
        scheduler_core::validate_parameters(&blueprint.parameters_schema, &parameters)?;
    }
    let resolved = ResolvedScheduleSnapshot {
        blueprint,
        base_parameters: parameters,
        blueprint_source_version: blueprint_artifact.source_version,
        parameters_source_version: parameters_artifact.source_version,
    };
    let plaintext = serde_json::to_vec(&resolved)?;
    let digest = hex::encode(Sha256::digest(&plaintext));
    let blueprint_digest = hex::encode(Sha256::digest(serde_json::to_vec(&resolved.blueprint)?));
    let source_ref = reqwest::Url::parse(&spec.blueprint_ref.uri).map_or_else(
        |_| spec.blueprint_ref.uri.clone(),
        |mut uri| {
            uri.set_query(None);
            uri.set_fragment(None);
            uri.to_string()
        },
    );
    state
        .store
        .register_blueprint_revision(&NewBlueprintRevision {
            digest: blueprint_digest,
            resolved_snapshot_digest: digest.clone(),
            source_ref,
            source_version: resolved.blueprint_source_version.clone(),
            executor_kind: match resolved.blueprint.executor {
                scheduler_core::ExecutorSpec::Command(_) => "command".into(),
                scheduler_core::ExecutorSpec::ExcelMacro(_) => "excel_macro".into(),
            },
            required_labels: serde_json::to_value(&resolved.blueprint.required_labels)?,
            execution_policy: serde_json::to_value(&resolved.blueprint.policy)?,
            parameter_schema: blueprint_schema_metadata(&resolved.blueprint.parameters_schema),
            binding_declarations: serde_json::to_value(&resolved.blueprint.parameter_bindings)?,
        })
        .await?;
    Ok((state.cipher.encrypt(&plaintext)?, digest))
}

fn blueprint_schema_metadata(schema: &Value) -> Value {
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| {
            properties
                .iter()
                .map(|(name, value)| {
                    let mut metadata = serde_json::Map::new();
                    if let Some(value_type) = value.get("type") {
                        metadata.insert("type".into(), value_type.clone());
                    }
                    if let Some(format) = value.get("format") {
                        metadata.insert("format".into(), format.clone());
                    }
                    (name.clone(), Value::Object(metadata))
                })
                .collect::<serde_json::Map<_, _>>()
        })
        .unwrap_or_default();
    serde_json::json!({
        "type": schema.get("type").cloned().unwrap_or(Value::String("object".into())),
        "required": schema.get("required").cloned().unwrap_or_else(|| serde_json::json!([])),
        "properties": properties,
    })
}

pub async fn create_run_from_schedule(
    state: &AppState,
    schedule: &ScheduleRecord,
    overrides: &Value,
    trigger_kind: &str,
    scheduled_at: DateTime<Utc>,
    idempotency_key: Option<String>,
) -> Result<scheduler_core::RunView> {
    let new = build_run_from_schedule(
        state,
        schedule,
        overrides,
        trigger_kind,
        scheduled_at,
        idempotency_key,
    )
    .await?;
    state.store.create_run(new).await
}

async fn build_run_from_schedule(
    state: &AppState,
    schedule: &ScheduleRecord,
    overrides: &Value,
    trigger_kind: &str,
    scheduled_at: DateTime<Utc>,
    idempotency_key: Option<String>,
) -> Result<NewRun> {
    if !schedule.view.spec.enabled {
        bail!("schedule is paused");
    }
    let plaintext = state.cipher.decrypt(&schedule.encrypted_snapshot)?;
    let resolved: ResolvedScheduleSnapshot = serde_json::from_slice(&plaintext)?;
    let parameters = merge_parameters(&resolved.base_parameters, overrides)?;
    let snapshot = resolve_snapshot(&resolved, &parameters, &schedule.view.spec.required_labels)?;
    let encrypted = state.cipher.encrypt(&serde_json::to_vec(&snapshot)?)?;
    Ok(NewRun {
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
}

fn validate_schedule_spec(spec: &ScheduleSpec) -> Result<()> {
    if spec.name.trim().is_empty() {
        bail!("schedule name is required");
    }
    if let Some(collection) = &spec.parameter_collection {
        collection.validate()?;
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
    request_id: Option<String>,
}

impl ApiError {
    pub(crate) fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "authentication_required",
            message: "authentication required".into(),
            request_id: None,
        }
    }

    pub(crate) fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "resource_not_found",
            message: "resource not found".into(),
            request_id: None,
        }
    }

    fn precondition_required(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PRECONDITION_REQUIRED,
            code: "precondition_required",
            message: message.into(),
            request_id: None,
        }
    }

    fn precondition_failed(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PRECONDITION_FAILED,
            code: "precondition_failed",
            message: message.into(),
            request_id: None,
        }
    }
}

impl<E> From<E> for ApiError
where
    E: Into<anyhow::Error>,
{
    fn from(error: E) -> Self {
        let error = error.into();
        let artifact_error = error.downcast_ref::<ArtifactFetchError>();
        let collection_error =
            error.downcast_ref::<crate::collection_runtime::CollectionRuntimeError>();
        let public_message = error.to_string();
        let (status, code, message, request_id) = if let Some(error) = collection_error {
            let status = match error.code() {
                "collection_source_too_large" | "collection_item_limit_exceeded" => {
                    StatusCode::PAYLOAD_TOO_LARGE
                }
                "collection_source_timeout" | "collection_connector_timeout" => {
                    StatusCode::GATEWAY_TIMEOUT
                }
                "collection_source_transport"
                | "collection_source_http_status"
                | "collection_connector_transport"
                | "collection_connector_http_status" => StatusCode::BAD_GATEWAY,
                _ => StatusCode::BAD_REQUEST,
            };
            (
                status,
                error.code(),
                format!("collection operation failed ({})", error.code()),
                None,
            )
        } else if let Some(error) = artifact_error {
            let (status, code) = match error {
                ArtifactFetchError::Timeout { .. } => {
                    (StatusCode::GATEWAY_TIMEOUT, "connector_timeout")
                }
                ArtifactFetchError::Transport { .. } => {
                    (StatusCode::BAD_GATEWAY, "connector_transport_failed")
                }
                ArtifactFetchError::UpstreamStatus { .. } => {
                    (StatusCode::BAD_GATEWAY, "connector_upstream_failed")
                }
                ArtifactFetchError::InvalidResponse { .. } => {
                    (StatusCode::BAD_GATEWAY, "connector_invalid_response")
                }
                ArtifactFetchError::TooLarge { .. } => {
                    (StatusCode::PAYLOAD_TOO_LARGE, "artifact_too_large")
                }
                ArtifactFetchError::NotConfigured { .. } => {
                    (StatusCode::BAD_REQUEST, "connector_not_configured")
                }
                ArtifactFetchError::InvalidReference { .. } => {
                    (StatusCode::BAD_REQUEST, "invalid_artifact_reference")
                }
                ArtifactFetchError::UnsupportedKind { .. } => {
                    (StatusCode::BAD_REQUEST, "connector_kind_not_allowed")
                }
            };
            (status, code, "artifact resolution failed".into(), None)
        } else if public_message.contains("conflict")
            || public_message.contains("currently being edited")
            || public_message.contains("edit lock is invalid or expired")
        {
            (
                StatusCode::CONFLICT,
                "conflict",
                "the resource changed or is locked by another editor".into(),
                None,
            )
        } else if public_message.contains("not found") {
            (
                StatusCode::NOT_FOUND,
                "resource_not_found",
                "resource not found".into(),
                None,
            )
        } else if public_message.contains("parameters failed validation") {
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                "parameter_validation_failed",
                "parameters failed blueprint validation".into(),
                None,
            )
        } else if public_message.contains("artifact") || public_message.contains("blueprint") {
            (
                StatusCode::BAD_REQUEST,
                "artifact_resolution_failed",
                "artifact or blueprint validation failed".into(),
                None,
            )
        } else if is_safe_client_error(&public_message) {
            (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                public_message,
                None,
            )
        } else {
            let request_id = crate::management::current_request_id();
            tracing::error!(
                %request_id,
                error = %format!("{error:#}"),
                "management API request failed internally"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal server error; use request_id to locate the diagnostic log".into(),
                Some(request_id),
            )
        };
        Self {
            status,
            code,
            message,
            request_id,
        }
    }
}

fn is_safe_client_error(message: &str) -> bool {
    [
        "invalid pagination cursor",
        "invalid dashboard configuration",
        "invalid global settings",
        "invalid node settings",
        "invalid parameters JSON",
        "lock_token required",
        "schedule has no cron trigger",
        "schedule is paused",
        "schedule name is required",
        "parameter overrides must be a JSON object",
        "must be",
        "must contain",
        "must use",
        "must match",
        "must not",
        "at least",
        "between",
        "only a",
        "already terminal",
        "does not exist",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = serde_json::json!({
            "error": self.message,
            "code": self.code,
            "status": self.status.as_u16(),
        });
        if let Some(request_id) = self.request_id {
            body["request_id"] = Value::String(request_id);
        }
        (self.status, Json(body)).into_response()
    }
}

pub async fn materialize_cron(state: &AppState) -> Result<()> {
    let now = Utc::now();
    for schedule in state.store.cron_schedules().await? {
        let Some(cron) = schedule.view.spec.cron.as_ref() else {
            continue;
        };
        let cursor = schedule.last_cron_at.unwrap_or(schedule.view.created_at);
        for next in due_cron_batch(cron, cursor, now)? {
            opentelemetry::global::meter("scheduler-coordinator")
                .u64_histogram("scheduler.cron.lag_ms")
                .build()
                .record(
                    u64::try_from((now - next).num_milliseconds().max(0)).unwrap_or(u64::MAX),
                    &[],
                );
            if schedule.view.spec.parameter_collection.is_some() {
                let new =
                    crate::collection_runtime::build_cron_batch(state, &schedule, next).await?;
                match state
                    .store
                    .create_cron_batch(new, schedule.view.revision, cron)
                    .await?
                {
                    CronBatchOccurrenceResult::Applied(_) => {}
                    CronBatchOccurrenceResult::StaleSchedule => break,
                }
                continue;
            }
            let new = build_run_from_schedule(
                state,
                &schedule,
                &serde_json::json!({}),
                "cron",
                next,
                None,
            )
            .await?;
            match state
                .store
                .create_cron_occurrence(new, schedule.view.revision, cron)
                .await?
            {
                CronOccurrenceResult::Applied(_) => {}
                CronOccurrenceResult::StaleSchedule => break,
            }
        }
    }
    Ok(())
}

fn due_cron_batch(
    cron: &scheduler_core::CronSpec,
    cursor: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Vec<DateTime<Utc>>> {
    Ok(
        scheduler_core::schedule::next_occurrences(cron, cursor, 1_000)?
            .into_iter()
            .take_while(|occurrence| *occurrence <= now)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::Path, sync::Arc};

    use chrono::TimeZone;
    use scheduler_core::{
        AdapterRegistry, ArtifactRef, ConnectorConfig, ConnectorEndpointConfig, ExecutorSpec,
        SnapshotCipher,
    };
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        sync::RwLock,
    };

    use super::*;

    async fn read_mock_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let header_end = loop {
            if let Some(position) = request.windows(4).position(|value| value == b"\r\n\r\n") {
                break position + 4;
            }
            let mut buffer = [0_u8; 1024];
            let read = stream.read(&mut buffer).await.expect("request headers");
            assert_ne!(read, 0, "request ended before its headers");
            request.extend_from_slice(&buffer[..read]);
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().expect("content length"))
            })
            .expect("content length header");
        while request.len() < header_end + content_length {
            let mut buffer = [0_u8; 1024];
            let read = stream.read(&mut buffer).await.expect("request body");
            assert_ne!(read, 0, "request ended before its body");
            request.extend_from_slice(&buffer[..read]);
        }
        request
    }

    fn sqlite_file_url(path: &Path) -> String {
        let normalized = path.to_string_lossy().replace('\\', "/");
        #[cfg(windows)]
        {
            format!("sqlite:///{}", normalized.trim_start_matches('/'))
        }
        #[cfg(not(windows))]
        {
            format!("sqlite://{normalized}")
        }
    }

    fn raw_api_cursor(version: u8, kind: &str, position: Value) -> String {
        URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({
                "version": version,
                "kind": kind,
                "position": position,
            }))
            .expect("cursor fixture"),
        )
    }

    fn assert_invalid_cursor<T: std::fmt::Debug>(result: Result<T>) {
        let error = result.expect_err("cursor must be rejected");
        assert!(
            format!("{error:#}").contains("invalid pagination cursor"),
            "unexpected cursor error: {error:#}"
        );
    }

    #[test]
    fn api_cursors_are_versioned_and_bound_to_their_endpoint_kind() {
        let position = TimeCursor {
            timestamp: "2026-07-21T12:34:56.789Z".into(),
            id: Uuid::new_v4().to_string(),
        };
        let encoded =
            encode_api_cursor(ApiCursorKind::Schedules, &position).expect("encode cursor");
        let decoded = decode_time_uuid_cursor(Some(&encoded), ApiCursorKind::Schedules)
            .expect("decode matching cursor");
        assert_eq!(decoded, Some(position));

        let bytes = URL_SAFE_NO_PAD.decode(&encoded).expect("cursor base64");
        let envelope: Value = serde_json::from_slice(&bytes).expect("cursor JSON");
        assert_eq!(envelope["version"], API_CURSOR_VERSION);
        assert_eq!(envelope["kind"], "schedules");

        let cross_endpoint =
            decode_time_uuid_cursor(Some(&encoded), ApiCursorKind::Runs).expect_err("kind fence");
        let api_error = ApiError::from(cross_endpoint);
        assert_eq!(api_error.status, StatusCode::BAD_REQUEST);
        assert_eq!(api_error.code, "invalid_request");

        assert_invalid_cursor(decode_time_uuid_cursor(
            Some(&raw_api_cursor(
                API_CURSOR_VERSION + 1,
                "schedules",
                serde_json::json!({
                    "timestamp": "2026-07-21T12:34:56.789Z",
                    "id": Uuid::new_v4().to_string(),
                }),
            )),
            ApiCursorKind::Schedules,
        ));
        assert_invalid_cursor(decode_time_uuid_cursor(
            Some(&raw_api_cursor(
                API_CURSOR_VERSION,
                "unknown_list",
                serde_json::json!({
                    "timestamp": "2026-07-21T12:34:56.789Z",
                    "id": Uuid::new_v4().to_string(),
                }),
            )),
            ApiCursorKind::Schedules,
        ));
        assert_invalid_cursor(decode_time_uuid_cursor(
            Some(
                &URL_SAFE_NO_PAD.encode(
                    serde_json::to_vec(&serde_json::json!({
                        "timestamp": "2026-07-21T12:34:56.789Z",
                        "id": Uuid::new_v4().to_string(),
                    }))
                    .expect("legacy cursor fixture"),
                ),
            ),
            ApiCursorKind::Schedules,
        ));
    }

    #[test]
    fn api_cursors_reject_malformed_sort_fields_and_entity_ids() {
        for position in [
            serde_json::json!({
                "timestamp": "not-a-timestamp",
                "id": Uuid::new_v4().to_string(),
            }),
            serde_json::json!({
                "timestamp": "2026-07-21T12:34:56Z",
                "id": Uuid::new_v4().to_string(),
            }),
            serde_json::json!({
                "timestamp": "2026-07-21T12:34:56.789Z",
                "id": "not-a-uuid",
            }),
        ] {
            let encoded = raw_api_cursor(API_CURSOR_VERSION, "schedules", position);
            assert_invalid_cursor(decode_time_uuid_cursor(
                Some(&encoded),
                ApiCursorKind::Schedules,
            ));
        }

        for digest in ["a".repeat(63), "A".repeat(64), "g".repeat(64)] {
            let encoded = raw_api_cursor(
                API_CURSOR_VERSION,
                "blueprints",
                serde_json::json!({
                    "timestamp": "2026-07-21T12:34:56.789Z",
                    "id": digest,
                }),
            );
            assert_invalid_cursor(decode_blueprint_cursor(Some(&encoded)));
        }

        let invalid_agent = raw_api_cursor(
            API_CURSOR_VERSION,
            "agents",
            serde_json::json!({"id": "../not-an-agent"}),
        );
        assert_invalid_cursor(decode_agent_cursor(Some(&invalid_agent)));

        for id in ["0", "01", "-1", "not-an-event"] {
            let encoded = raw_api_cursor(
                API_CURSOR_VERSION,
                "run_events",
                serde_json::json!({"id": id}),
            );
            assert_invalid_cursor(decode_event_cursor(Some(&encoded)));
        }

        for position in [
            serde_json::json!({"attempt_number": 0, "id": Uuid::new_v4().to_string()}),
            serde_json::json!({"attempt_number": 1, "id": "not-a-uuid"}),
        ] {
            let encoded = raw_api_cursor(API_CURSOR_VERSION, "run_attempts", position);
            assert_invalid_cursor(decode_attempt_cursor(Some(&encoded)));
        }
    }

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

    #[test]
    fn connector_failures_have_stable_operator_safe_http_statuses() {
        let cases = [
            (
                ArtifactFetchError::NotConfigured {
                    connector: "records".into(),
                },
                StatusCode::BAD_REQUEST,
                "connector_not_configured",
            ),
            (
                ArtifactFetchError::UnsupportedKind {
                    connector: "records".into(),
                    kind: ArtifactKind::Blueprint,
                },
                StatusCode::BAD_REQUEST,
                "connector_kind_not_allowed",
            ),
            (
                ArtifactFetchError::TooLarge {
                    connector: Some("records".into()),
                    kind: ArtifactKind::Parameters,
                },
                StatusCode::PAYLOAD_TOO_LARGE,
                "artifact_too_large",
            ),
            (
                ArtifactFetchError::Transport {
                    connector: "records".into(),
                    kind: ArtifactKind::Parameters,
                },
                StatusCode::BAD_GATEWAY,
                "connector_transport_failed",
            ),
            (
                ArtifactFetchError::InvalidResponse {
                    connector: "records".into(),
                    kind: ArtifactKind::Parameters,
                    reason: "missing protocol version",
                },
                StatusCode::BAD_GATEWAY,
                "connector_invalid_response",
            ),
            (
                ArtifactFetchError::Timeout {
                    connector: "records".into(),
                    kind: ArtifactKind::Parameters,
                },
                StatusCode::GATEWAY_TIMEOUT,
                "connector_timeout",
            ),
        ];

        for (source, status, code) in cases {
            let error = ApiError::from(anyhow::Error::new(source).context("resolving schedule"));
            assert_eq!(error.status, status);
            assert_eq!(error.code, code);
            assert!(!error.message.contains("response body"));
        }

        let upstream = ApiError::from(ArtifactFetchError::UpstreamStatus {
            connector: "records".into(),
            kind: ArtifactKind::Parameters,
            status: 503,
        });
        assert_eq!(upstream.status, StatusCode::BAD_GATEWAY);
        assert_eq!(upstream.code, "connector_upstream_failed");
        assert_eq!(upstream.message, "artifact resolution failed");
        assert!(!upstream.message.contains("503"));
    }

    #[test]
    fn unexpected_api_failures_are_safe_correlated_500_errors() {
        let error = ApiError::from(anyhow::anyhow!(
            "database failure at /private/secret/scheduler.db: malformed row"
        ));
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error.code, "internal_error");
        assert!(error.request_id.is_some());
        assert!(!error.message.contains("scheduler.db"));
        assert!(!error.message.contains("malformed row"));
    }

    #[test]
    fn cron_materialization_batches_dense_fallback_scan_once() {
        let cron = scheduler_core::CronSpec {
            expression: "* * * * * *".into(),
            timezone: "Europe/Vienna".into(),
        };
        let cursor = Utc
            .with_ymd_and_hms(2026, 10, 25, 0, 59, 59)
            .single()
            .expect("valid cursor");
        let now = cursor + chrono::Duration::seconds(1_000);

        // Around a fallback, next_occurrences deliberately rewinds to the local day's start.
        // One 1,000-item scan is bounded; calling count=1 a thousand times would rescan that
        // prefix and make this regression pathologically slow.
        let started = std::time::Instant::now();
        let due = due_cron_batch(&cron, cursor, now).expect("batch");
        assert_eq!(due.len(), 1_000);
        assert_eq!(due.first(), Some(&(cursor + chrono::Duration::seconds(1))));
        assert_eq!(due.last(), Some(&now));
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "dense fallback materialization must remain a single bounded scan"
        );
    }

    #[tokio::test]
    async fn schedule_snapshot_resolves_file_blueprint_and_connector_parameters() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let blueprint_path = directory.path().join("connector-blueprint.yaml");
        std::fs::write(
            &blueprint_path,
            br#"api_version: scheduler/v1
executor:
  kind: command
  program: /usr/bin/example-task
  args:
    - "{{params.customer_id}}"
parameters_schema:
  type: object
  additionalProperties: false
  required: [customer_id, enabled]
  properties:
    customer_id:
      type: integer
      minimum: 1
      maximum: 100
    enabled:
      type: boolean
"#,
        )
        .expect("blueprint fixture");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("connector listener");
        let address = listener.local_addr().expect("connector address");
        let sidecar = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connector request");
            let request = read_mock_request(&mut stream).await;
            let header_end = request
                .windows(4)
                .position(|value| value == b"\r\n\r\n")
                .expect("request headers")
                + 4;
            let payload: Value =
                serde_json::from_slice(&request[header_end..]).expect("connector request JSON");
            assert_eq!(payload["api_version"], "scheduler.connector/v1");
            assert_eq!(payload["kind"], "parameters");
            assert_eq!(payload["resource"], "/accounts/current?revision=17");

            let body = br#"{"customer_id":42,"enabled":true}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v1\r\nETag: \"parameters-revision-17\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("connector response headers");
            stream
                .write_all(body)
                .await
                .expect("connector response body");
        });

        let database = directory.path().join("coordinator.db");
        let store = scheduler_store::Store::connect(&sqlite_file_url(&database), None)
            .await
            .expect("test store");
        let cipher = SnapshotCipher::from_base64("test", &SnapshotCipher::generate_base64())
            .expect("snapshot cipher");
        let mut adapters =
            AdapterRegistry::with_defaults(vec![directory.path().to_path_buf()], HashMap::new())
                .expect("default adapters");
        adapters
            .register_connectors(ConnectorConfig {
                api_version: "scheduler/connectors/v1".into(),
                connectors: HashMap::from([(
                    "records".into(),
                    ConnectorEndpointConfig {
                        base_url: format!("http://{address}"),
                        bearer_token_env: None,
                        allowed_kinds: vec![ArtifactKind::Parameters],
                        connect_timeout_seconds: 2,
                        timeout_seconds: 5,
                        allow_insecure_http: false,
                    },
                )]),
            })
            .expect("connector registration");
        let state = AppState {
            store,
            cipher,
            adapters,
            auth: crate::auth::AuthManager::new("test-admin-token", false).expect("auth"),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            internal_rest_url: "http://127.0.0.1:1".into(),
            internal_admin_token: "test-admin-token".into(),
            collection_sources: crate::collection_runtime::CollectionSourceRegistry::new(
                vec![directory.path().to_path_buf()],
                None,
            )
            .expect("collection sources"),
            collection_worker_id: "test-coordinator".into(),
        };
        let spec = ScheduleSpec {
            name: "connector snapshot".into(),
            blueprint_ref: ArtifactRef {
                uri: reqwest::Url::from_file_path(&blueprint_path)
                    .expect("blueprint file URL")
                    .to_string(),
            },
            parameters_ref: ArtifactRef {
                uri: "connector://records/accounts/current?revision=17".into(),
            },
            parameter_collection: None,
            required_labels: Default::default(),
            cron: None,
            webhook_enabled: false,
            enabled: true,
        };

        let (encrypted, digest) = resolve_and_encrypt(&state, &spec)
            .await
            .expect("resolved encrypted snapshot");
        sidecar.await.expect("connector sidecar");
        let plaintext = state.cipher.decrypt(&encrypted).expect("decrypt snapshot");
        assert_ne!(encrypted, plaintext, "snapshot must be encrypted at rest");
        assert_eq!(digest, hex::encode(Sha256::digest(&plaintext)));
        let resolved: ResolvedScheduleSnapshot =
            serde_json::from_slice(&plaintext).expect("resolved snapshot JSON");
        assert_eq!(
            resolved.base_parameters,
            serde_json::json!({"customer_id": 42, "enabled": true})
        );
        assert_eq!(
            resolved.parameters_source_version.as_deref(),
            Some("\"parameters-revision-17\"")
        );
        assert!(resolved.blueprint_source_version.is_some());
        let execution = resolve_snapshot(&resolved, &resolved.base_parameters, &Default::default())
            .expect("validated execution snapshot");
        let ExecutorSpec::Command(command) = execution.executor else {
            panic!("expected command executor");
        };
        assert_eq!(command.args, vec!["42"]);

        let schedule_id = Uuid::new_v4();
        state
            .store
            .create_schedule(NewSchedule {
                id: schedule_id,
                spec,
                encrypted_snapshot: encrypted,
                snapshot_digest: digest,
                key_id: state.cipher.key_id().into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("persist schedule");
        let persisted_record = state
            .store
            .get_schedule_record(schedule_id)
            .await
            .expect("load schedule")
            .expect("persisted schedule");
        let persisted_plaintext = state
            .cipher
            .decrypt(&persisted_record.encrypted_snapshot)
            .expect("decrypt persisted snapshot");
        let persisted: ResolvedScheduleSnapshot =
            serde_json::from_slice(&persisted_plaintext).expect("persisted snapshot JSON");
        assert_eq!(
            persisted.parameters_source_version.as_deref(),
            Some("\"parameters-revision-17\"")
        );

        // The one-request sidecar has already exited. Run creation must use
        // only the persisted encrypted schedule snapshot and never refetch.
        let run = create_run_from_schedule(
            &state,
            &persisted_record,
            &serde_json::json!({}),
            "manual",
            Utc::now(),
            None,
        )
        .await
        .expect("run from persisted snapshot");
        assert_eq!(run.state, scheduler_core::RunState::Queued);
    }

    #[test]
    fn blueprint_catalog_schema_metadata_never_retains_defaults_or_examples() {
        let metadata = blueprint_schema_metadata(&serde_json::json!({
            "type": "object",
            "required": ["password"],
            "properties": {
                "password": {
                    "type": "string",
                    "format": "password",
                    "default": "must-never-appear",
                    "examples": ["also-secret"]
                }
            }
        }));
        let encoded = serde_json::to_string(&metadata).expect("metadata");
        assert!(encoded.contains("password"));
        assert!(encoded.contains("format"));
        assert!(!encoded.contains("must-never-appear"));
        assert!(!encoded.contains("also-secret"));
    }
}
