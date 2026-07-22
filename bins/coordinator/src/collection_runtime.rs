use std::{
    collections::HashMap,
    net::IpAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use opentelemetry::KeyValue;
use reqwest::{Client, Url, header::HeaderValue};
use scheduler_core::{
    BatchItemState, ConnectorConfig, ParameterCollectionSpec, ResolvedScheduleSnapshot,
    SnapshotCipher, blueprint::merge_parameters, resolve_snapshot,
};
use scheduler_protocol::control::{CancelAttempt, CoordinatorMessage, coordinator_message};
use scheduler_store::{
    BatchItemView, BatchRecord, BatchView, CommitCollectionPage, CommitPageOutcome,
    FinalizeBatchOutcome, NewBatch, NewBatchItem, ScheduleRecord,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
use thiserror::Error;
use tracing::{Instrument, info, instrument, warn};
use uuid::Uuid;

use crate::{api::ApiError, state::AppState};

const COLLECTION_API_VERSION: &str = "scheduler.connector/v1";
const COLLECTION_BODY_LIMIT: usize = 16 * 1024 * 1024;
const MAX_CURSOR_BYTES: usize = 4 * 1024;
const MAX_SNAPSHOT_ID_BYTES: usize = 512;
const COLLECTION_LEASE_SECONDS: u64 = 60;

#[derive(Debug, Error)]
#[error("parameter collection failed ({code})")]
pub struct CollectionRuntimeError {
    code: &'static str,
}

impl CollectionRuntimeError {
    fn new(code: &'static str) -> Self {
        Self { code }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }
}

fn collection_error(code: &'static str) -> anyhow::Error {
    CollectionRuntimeError::new(code).into()
}

fn collection_error_code(error: &anyhow::Error) -> &'static str {
    error
        .downcast_ref::<CollectionRuntimeError>()
        .map(CollectionRuntimeError::code)
        .or_else(|| {
            error
                .to_string()
                .contains("collection_conflicting_duplicate_key")
                .then_some("collection_conflicting_duplicate_key")
        })
        .unwrap_or("collection_ingestion_failed")
}

fn source_kind(reference: &str) -> &'static str {
    match Url::parse(reference).ok().as_ref().map(Url::scheme) {
        Some("file") => "file",
        Some("http" | "https") => "http",
        Some("connector") => "connector",
        _ => "invalid",
    }
}

fn record_collection_error(stage: &'static str, code: &'static str) {
    opentelemetry::global::meter("scheduler-coordinator")
        .u64_counter("scheduler.collection.errors")
        .build()
        .add(
            1,
            &[KeyValue::new("stage", stage), KeyValue::new("code", code)],
        );
}

#[derive(Clone)]
pub struct CollectionSourceRegistry {
    file_roots: Arc<Vec<PathBuf>>,
    static_http: Client,
    connectors: Arc<HashMap<String, CollectionConnector>>,
}

#[derive(Clone)]
struct CollectionConnector {
    endpoint: Url,
    client: Client,
    authorization: Option<HeaderValue>,
}

impl CollectionSourceRegistry {
    pub fn new(file_roots: Vec<PathBuf>, config: Option<ConnectorConfig>) -> Result<Self> {
        let file_roots = file_roots
            .into_iter()
            .map(|root| {
                root.canonicalize()
                    .with_context(|| format!("invalid artifact root {}", root.display()))
            })
            .collect::<Result<Vec<_>>>()?;
        let static_http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        let mut connectors = HashMap::new();
        if let Some(config) = config {
            if config.api_version != "scheduler/connectors/v1" {
                bail!("unsupported connector config api_version; expected scheduler/connectors/v1");
            }
            for (name, connector) in config.connectors {
                if !connector_name_is_valid(&name) {
                    bail!("invalid parameter collection connector name");
                }
                if connector.connect_timeout_seconds == 0
                    || connector.timeout_seconds == 0
                    || connector.connect_timeout_seconds > connector.timeout_seconds
                {
                    bail!("invalid parameter collection connector timeout configuration");
                }
                let mut endpoint = Url::parse(&connector.base_url)
                    .context("parameter collection connector has an invalid base URL")?;
                validate_connector_url(&endpoint, connector.allow_insecure_http)?;
                let endpoint_path = format!(
                    "{}/v1/parameter-collections/page",
                    endpoint.path().trim_end_matches('/')
                );
                endpoint.set_path(&endpoint_path);
                let authorization = connector
                    .bearer_token_env
                    .as_deref()
                    .map(|name| {
                        if name.trim().is_empty() {
                            bail!("connector bearer token environment name cannot be empty");
                        }
                        let token = std::env::var(name)
                            .context("connector bearer token environment variable is not set")?;
                        if token.trim().is_empty() {
                            bail!("connector bearer token environment variable is empty");
                        }
                        let mut header = HeaderValue::from_str(&format!("Bearer {token}"))
                            .context("connector bearer token cannot be represented as a header")?;
                        header.set_sensitive(true);
                        Ok(header)
                    })
                    .transpose()?;
                let client = Client::builder()
                    .connect_timeout(Duration::from_secs(connector.connect_timeout_seconds))
                    .timeout(Duration::from_secs(connector.timeout_seconds))
                    .redirect(reqwest::redirect::Policy::none())
                    .build()?;
                connectors.insert(
                    name,
                    CollectionConnector {
                        endpoint,
                        client,
                        authorization,
                    },
                );
            }
        }
        Ok(Self {
            file_roots: Arc::new(file_roots),
            static_http,
            connectors: Arc::new(connectors),
        })
    }

    pub fn connector_count(&self) -> usize {
        self.connectors.len()
    }

    #[instrument(
        name = "coordinator.collection.source.fetch",
        skip_all,
        err,
        fields(source.kind = source_kind(reference), page.size = page_size)
    )]
    async fn fetch_page(
        &self,
        reference: &str,
        cursor: Option<&str>,
        expected_snapshot: Option<&str>,
        page_size: u32,
    ) -> Result<SourcePage> {
        let started = Instant::now();
        let kind = source_kind(reference);
        let result = async {
            let uri = Url::parse(reference)
                .map_err(|_| collection_error("collection_invalid_reference"))?;
            match uri.scheme() {
                "file" => {
                    let bytes = self.read_file(&uri).await?;
                    static_page(bytes, cursor, expected_snapshot, page_size)
                }
                "http" | "https" => {
                    let bytes = self.read_http(uri).await?;
                    static_page(bytes, cursor, expected_snapshot, page_size)
                }
                "connector" => {
                    self.read_connector(&uri, cursor, expected_snapshot, page_size)
                        .await
                }
                _ => Err(collection_error("collection_unsupported_source")),
            }
        }
        .await;
        let (outcome, error_code) = match &result {
            Ok(_) => ("success", None),
            Err(error) => ("error", Some(collection_error_code(error))),
        };
        let attributes = [
            KeyValue::new("source", kind),
            KeyValue::new("outcome", outcome),
        ];
        let meter = opentelemetry::global::meter("scheduler-coordinator");
        meter
            .u64_counter("scheduler.collection.source.fetches")
            .build()
            .add(1, &attributes);
        meter
            .f64_histogram("scheduler.collection.source.fetch.duration_ms")
            .build()
            .record(started.elapsed().as_secs_f64() * 1_000.0, &attributes);
        if let Some(code) = error_code {
            record_collection_error("fetch", code);
        }
        result
    }

    async fn read_file(&self, uri: &Url) -> Result<Vec<u8>> {
        let path = uri
            .to_file_path()
            .map_err(|_| collection_error("collection_invalid_reference"))?;
        let canonical = tokio::fs::canonicalize(path)
            .await
            .map_err(|_| collection_error("collection_source_unavailable"))?;
        if !self
            .file_roots
            .iter()
            .any(|root| canonical.starts_with(root))
        {
            return Err(collection_error("collection_file_not_allowed"));
        }
        let metadata = tokio::fs::metadata(&canonical)
            .await
            .map_err(|_| collection_error("collection_source_unavailable"))?;
        if metadata.len() as usize > COLLECTION_BODY_LIMIT {
            return Err(collection_error("collection_source_too_large"));
        }
        tokio::fs::read(canonical)
            .await
            .map_err(|_| collection_error("collection_source_unavailable"))
    }

    async fn read_http(&self, uri: Url) -> Result<Vec<u8>> {
        let response = self.static_http.get(uri).send().await.map_err(|error| {
            if error.is_timeout() {
                collection_error("collection_source_timeout")
            } else {
                collection_error("collection_source_transport")
            }
        })?;
        if !response.status().is_success() {
            return Err(collection_error("collection_source_http_status"));
        }
        read_bounded(response).await
    }

    async fn read_connector(
        &self,
        uri: &Url,
        cursor: Option<&str>,
        expected_snapshot: Option<&str>,
        page_size: u32,
    ) -> Result<SourcePage> {
        let name = connector_name(uri)?;
        let connector = self
            .connectors
            .get(name)
            .ok_or_else(|| collection_error("collection_connector_not_configured"))?;
        let authorization = connector
            .authorization
            .as_ref()
            .ok_or_else(|| collection_error("collection_connector_auth_required"))?;
        let mut resource = uri.path().to_owned();
        if let Some(query) = uri.query() {
            resource.push('?');
            resource.push_str(query);
        }
        let request = ConnectorPageRequest {
            api_version: COLLECTION_API_VERSION,
            resource: &resource,
            cursor,
            snapshot_id: expected_snapshot,
            page_size,
        };
        let mut builder = connector
            .client
            .post(connector.endpoint.clone())
            .json(&request);
        builder = builder.header(header::AUTHORIZATION, authorization.clone());
        let response = builder.send().await.map_err(|error| {
            if error.is_timeout() {
                collection_error("collection_connector_timeout")
            } else {
                collection_error("collection_connector_transport")
            }
        })?;
        let status = response.status();
        if status == StatusCode::CONFLICT || status == StatusCode::GONE {
            return Err(collection_error("collection_snapshot_expired"));
        }
        if !status.is_success() {
            return Err(collection_error("collection_connector_http_status"));
        }
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim();
        if content_type != "application/json" && !content_type.ends_with("+json") {
            return Err(collection_error(
                "collection_connector_invalid_content_type",
            ));
        }
        let bytes = read_bounded(response).await?;
        let response: ConnectorPageResponse = serde_json::from_slice(&bytes)
            .map_err(|_| collection_error("collection_connector_invalid_response"))?;
        if response.api_version != COLLECTION_API_VERSION {
            return Err(collection_error("collection_connector_version_mismatch"));
        }
        validate_source_page(
            response.snapshot_id,
            response.items,
            response.next_cursor,
            cursor,
            expected_snapshot,
            page_size,
        )
    }
}

#[derive(Serialize)]
struct ConnectorPageRequest<'a> {
    api_version: &'static str,
    resource: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<&'a str>,
    page_size: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectorPageResponse {
    api_version: String,
    snapshot_id: String,
    items: Vec<Value>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Debug)]
struct SourcePage {
    snapshot_id: String,
    items: Vec<Value>,
    next_cursor: Option<String>,
}

async fn read_bounded(mut response: reqwest::Response) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > COLLECTION_BODY_LIMIT as u64)
    {
        return Err(collection_error("collection_source_too_large"));
    }
    let mut bytes = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(COLLECTION_BODY_LIMIT as u64) as usize,
    );
    loop {
        let chunk = response.chunk().await.map_err(|error| {
            if error.is_timeout() {
                collection_error("collection_source_timeout")
            } else {
                collection_error("collection_source_body_failed")
            }
        })?;
        let Some(chunk) = chunk else { break };
        if bytes.len().saturating_add(chunk.len()) > COLLECTION_BODY_LIMIT {
            return Err(collection_error("collection_source_too_large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn static_page(
    bytes: Vec<u8>,
    cursor: Option<&str>,
    expected_snapshot: Option<&str>,
    page_size: u32,
) -> Result<SourcePage> {
    let snapshot_id = hex::encode(Sha256::digest(&bytes));
    if expected_snapshot.is_some_and(|expected| expected != snapshot_id) {
        return Err(collection_error("collection_snapshot_drift"));
    }
    let values = parse_static_collection(&bytes)?;
    let offset = match cursor {
        None => 0,
        Some(cursor) => cursor
            .strip_prefix("offset:")
            .and_then(|offset| offset.parse::<usize>().ok())
            .ok_or_else(|| collection_error("collection_cursor_invalid"))?,
    };
    if offset > values.len() {
        return Err(collection_error("collection_cursor_invalid"));
    }
    let end = offset.saturating_add(page_size as usize).min(values.len());
    let next_cursor = (end < values.len()).then(|| format!("offset:{end}"));
    validate_source_page(
        snapshot_id,
        values[offset..end].to_vec(),
        next_cursor,
        cursor,
        expected_snapshot,
        page_size,
    )
}

fn parse_static_collection(bytes: &[u8]) -> Result<Vec<Value>> {
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        return match value {
            Value::Array(items) => Ok(items),
            Value::Object(mut object) => object
                .remove("items")
                .and_then(|items| items.as_array().cloned())
                .ok_or_else(|| collection_error("collection_document_invalid")),
            _ => Err(collection_error("collection_document_invalid")),
        };
    }
    let document =
        std::str::from_utf8(bytes).map_err(|_| collection_error("collection_document_invalid"))?;
    let mut items = Vec::new();
    for line in document.lines().filter(|line| !line.trim().is_empty()) {
        items.push(
            serde_json::from_str(line)
                .map_err(|_| collection_error("collection_ndjson_line_invalid"))?,
        );
    }
    if items.is_empty() && !document.trim().is_empty() {
        return Err(collection_error("collection_document_invalid"));
    }
    Ok(items)
}

fn validate_source_page(
    snapshot_id: String,
    items: Vec<Value>,
    next_cursor: Option<String>,
    request_cursor: Option<&str>,
    expected_snapshot: Option<&str>,
    page_size: u32,
) -> Result<SourcePage> {
    if snapshot_id.is_empty() || snapshot_id.len() > MAX_SNAPSHOT_ID_BYTES {
        return Err(collection_error("collection_snapshot_invalid"));
    }
    if expected_snapshot.is_some_and(|expected| expected != snapshot_id) {
        return Err(collection_error("collection_snapshot_drift"));
    }
    if items.len() > page_size as usize {
        return Err(collection_error("collection_page_too_large"));
    }
    if let Some(next) = next_cursor.as_deref() {
        if next.is_empty() || next.len() > MAX_CURSOR_BYTES {
            return Err(collection_error("collection_cursor_invalid"));
        }
        if request_cursor == Some(next) || items.is_empty() {
            return Err(collection_error("collection_cursor_cycle"));
        }
    }
    Ok(SourcePage {
        snapshot_id,
        items,
        next_cursor,
    })
}

fn validate_connector_url(url: &Url, allow_insecure_http: bool) -> Result<()> {
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host().is_none()
    {
        bail!("invalid parameter collection connector base URL");
    }
    match url.scheme() {
        "https" => Ok(()),
        "http" if allow_insecure_http || url_is_loopback(url) => Ok(()),
        _ => bail!("parameter collection connector must use HTTPS"),
    }
}

fn url_is_loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    })
}

fn connector_name(uri: &Url) -> Result<&str> {
    if uri.scheme() != "connector"
        || !uri.username().is_empty()
        || uri.password().is_some()
        || uri.port().is_some()
        || uri.fragment().is_some()
    {
        return Err(collection_error("collection_invalid_reference"));
    }
    let name = uri
        .host_str()
        .ok_or_else(|| collection_error("collection_invalid_reference"))?;
    if !connector_name_is_valid(name) {
        return Err(collection_error("collection_invalid_reference"));
    }
    Ok(name)
}

fn connector_name_is_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && !name.starts_with('-')
        && !name.ends_with('-')
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

#[derive(Debug, Serialize, Deserialize)]
struct BatchExecutionSeed {
    resolved: ResolvedScheduleSnapshot,
    collection: ParameterCollectionSpec,
    required_labels: std::collections::BTreeMap<String, String>,
}

pub async fn create_batch_from_schedule(
    state: &AppState,
    schedule: &ScheduleRecord,
    overrides: &Value,
    trigger_kind: &str,
    scheduled_at: DateTime<Utc>,
    idempotency_key: Option<String>,
) -> Result<BatchView> {
    if !schedule.view.spec.enabled {
        bail!("schedule is paused");
    }
    let collection = schedule
        .view
        .spec
        .parameter_collection
        .clone()
        .context("schedule has no parameter collection")?;
    collection.validate()?;
    let plaintext = state.cipher.decrypt(&schedule.encrypted_snapshot)?;
    let resolved: ResolvedScheduleSnapshot = serde_json::from_slice(&plaintext)?;
    let seed = BatchExecutionSeed {
        resolved,
        collection: collection.clone(),
        required_labels: schedule.view.spec.required_labels.clone(),
    };
    let seed_plaintext = serde_json::to_vec(&seed)?;
    let encrypted_overrides = state.cipher.encrypt(&serde_json::to_vec(overrides)?)?;
    state
        .store
        .create_batch(NewBatch {
            id: Uuid::new_v4(),
            schedule_id: schedule.view.id,
            schedule_revision: schedule.view.revision,
            trigger_kind: trigger_kind.into(),
            scheduled_at,
            idempotency_key,
            encrypted_snapshot: state.cipher.encrypt(&seed_plaintext)?,
            encrypted_trigger_overrides: Some(encrypted_overrides),
            snapshot_digest: hex::encode(Sha256::digest(&seed_plaintext)),
            key_id: state.cipher.key_id().into(),
            page_size: collection.page_size,
            max_items: collection.max_items,
            max_active_runs: collection.max_active_runs,
            poison_distinct_nodes: collection.poison_distinct_nodes,
        })
        .await
}

pub async fn batch_by_idempotency(
    state: &AppState,
    schedule_id: Uuid,
    key: &str,
) -> Result<Option<BatchView>> {
    let id: Option<String> = sqlx::query_scalar(
        "SELECT target_id FROM trigger_identities WHERE schedule_id=? AND idempotency_key=? AND target_kind='batch'",
    )
    .bind(schedule_id.to_string())
    .bind(key)
    .fetch_optional(state.store.pool())
    .await?;
    let Some(id) = id else { return Ok(None) };
    let id = Uuid::parse_str(&id)?;
    Ok(state.store.get_batch(id).await?.map(|record| record.view))
}

pub async fn build_cron_batch(
    state: &AppState,
    schedule: &ScheduleRecord,
    scheduled_at: DateTime<Utc>,
) -> Result<NewBatch> {
    let collection = schedule
        .view
        .spec
        .parameter_collection
        .clone()
        .context("schedule has no parameter collection")?;
    let plaintext = state.cipher.decrypt(&schedule.encrypted_snapshot)?;
    let resolved: ResolvedScheduleSnapshot = serde_json::from_slice(&plaintext)?;
    let seed = BatchExecutionSeed {
        resolved,
        collection: collection.clone(),
        required_labels: schedule.view.spec.required_labels.clone(),
    };
    let seed_plaintext = serde_json::to_vec(&seed)?;
    let empty = serde_json::json!({});
    Ok(NewBatch {
        id: Uuid::new_v4(),
        schedule_id: schedule.view.id,
        schedule_revision: schedule.view.revision,
        trigger_kind: "cron".into(),
        scheduled_at,
        idempotency_key: None,
        encrypted_snapshot: state.cipher.encrypt(&seed_plaintext)?,
        encrypted_trigger_overrides: Some(state.cipher.encrypt(&serde_json::to_vec(&empty)?)?),
        snapshot_digest: hex::encode(Sha256::digest(&seed_plaintext)),
        key_id: state.cipher.key_id().into(),
        page_size: collection.page_size,
        max_items: collection.max_items,
        max_active_runs: collection.max_active_runs,
        poison_distinct_nodes: collection.poison_distinct_nodes,
    })
}

#[instrument(name = "coordinator.collection.worker_pass", skip_all)]
pub async fn collection_worker_pass(state: &AppState) -> Result<()> {
    let meter = opentelemetry::global::meter("scheduler-coordinator");
    let batches = state
        .store
        .claim_collection_batches(&state.collection_worker_id, COLLECTION_LEASE_SECONDS, 8)
        .await?;
    meter
        .u64_histogram("scheduler.collection.worker.claimed_batches")
        .build()
        .record(batches.len() as u64, &[]);
    let futures = batches
        .into_iter()
        .map(|batch| process_claimed_batch(state.clone(), batch));
    for result in futures::future::join_all(futures).await {
        if let Err(error) = result {
            warn!(error = %error, "collection ingestion task failed before safe classification");
        }
    }
    let state_gauge = meter.u64_gauge("scheduler.collection.batches").build();
    for (batch_state, count) in state.store.batch_state_counts().await? {
        state_gauge.record(count, &[KeyValue::new("state", batch_state.as_str())]);
    }
    Ok(())
}

#[instrument(
    name = "coordinator.collection.batch",
    skip_all,
    fields(
        batch_id = %batch.view.id,
        schedule_id = %batch.view.schedule_id,
        trigger = batch.view.trigger_kind.as_str()
    )
)]
async fn process_claimed_batch(state: AppState, mut batch: BatchRecord) -> Result<()> {
    let batch_id = batch.view.id;
    let result = process_claimed_batch_inner(&state, &mut batch).await;
    if let Err(error) = result {
        let code = collection_error_code(&error);
        state.store.fail_batch(batch_id, code).await?;
        record_collection_error("batch", code);
        opentelemetry::global::meter("scheduler-coordinator")
            .u64_counter("scheduler.collection.state_transitions")
            .build()
            .add(1, &[KeyValue::new("to", "failed")]);
        warn!(batch_id = %batch_id, failure_code = code, "parameter collection batch failed");
    }
    Ok(())
}

async fn process_claimed_batch_inner(state: &AppState, batch: &mut BatchRecord) -> Result<()> {
    let lease_token = batch
        .lease_token
        .clone()
        .context("claimed collection batch is missing its lease token")?;
    if batch.ingestion_complete {
        finalize_claimed_batch(state, batch, &lease_token).await?;
        return Ok(());
    }
    let seed: BatchExecutionSeed =
        serde_json::from_slice(&state.cipher.decrypt(&batch.encrypted_snapshot)?)?;
    let overrides: Value = match &batch.encrypted_trigger_overrides {
        Some(encrypted) => serde_json::from_slice(&state.cipher.decrypt(encrypted)?)?,
        None => serde_json::json!({}),
    };
    loop {
        let page_started = Instant::now();
        let source = source_kind(&seed.collection.source_ref.uri);
        if !state
            .store
            .renew_collection_lease(batch.view.id, &lease_token, COLLECTION_LEASE_SECONDS)
            .await?
        {
            return Ok(());
        }
        let cursor = batch
            .next_cursor_encrypted
            .as_deref()
            .map(|encrypted| decrypt_utf8(&state.cipher, encrypted))
            .transpose()?;
        let collection_snapshot = batch
            .collection_snapshot_encrypted
            .as_deref()
            .map(|encrypted| decrypt_utf8(&state.cipher, encrypted))
            .transpose()?;
        let page = state
            .collection_sources
            .fetch_page(
                &seed.collection.source_ref.uri,
                cursor.as_deref(),
                collection_snapshot.as_deref(),
                batch.page_size,
            )
            .await?;
        if batch
            .view
            .item_count
            .saturating_add(page.items.len() as u32)
            > batch.max_items
            || (page.next_cursor.is_some()
                && batch
                    .view
                    .item_count
                    .saturating_add(page.items.len() as u32)
                    >= batch.max_items)
        {
            return Err(collection_error("collection_item_limit_exceeded"));
        }
        let next_cursor_digest = match page.next_cursor.as_deref() {
            Some(next) => keyed_text_digest(&state.cipher, "collection-cursor", next)?,
            None => keyed_text_digest(&state.cipher, "collection-cursor", "complete")?,
        };
        if page.next_cursor.is_some()
            && cursor_digest_seen(state, batch.view.id, &next_cursor_digest).await?
        {
            return Err(collection_error("collection_cursor_cycle"));
        }
        let items = materialize_items(state, batch, &seed, &overrides, page.items).await?;
        let meter = opentelemetry::global::meter("scheduler-coordinator");
        for item_state in [
            BatchItemState::Ready,
            BatchItemState::Invalid,
            BatchItemState::Poisoned,
            BatchItemState::Held,
        ] {
            let count = items.iter().filter(|item| item.state == item_state).count() as u64;
            if count != 0 {
                meter
                    .u64_counter("scheduler.collection.items")
                    .build()
                    .add(count, &[KeyValue::new("state", item_state.as_str())]);
            }
        }
        let page_digest_value = serde_json::json!({
            "snapshot_id": page.snapshot_id,
            "next": page.next_cursor,
            "items": items.iter().map(|item| (&item.provider_key_hmac, &item.parameters_digest, item.state.as_str(), &item.failure_code)).collect::<Vec<_>>(),
        });
        let commit = CommitCollectionPage {
            batch_id: batch.view.id,
            lease_token: lease_token.clone(),
            expected_generation: batch.cursor_generation,
            request_cursor_digest: batch.next_cursor_digest.clone(),
            page_digest: state
                .cipher
                .input_fingerprint("collection-page", &page_digest_value)?,
            collection_snapshot_encrypted: state.cipher.encrypt(page.snapshot_id.as_bytes())?,
            collection_snapshot_digest: keyed_text_digest(
                &state.cipher,
                "collection-snapshot",
                &page.snapshot_id,
            )?,
            next_cursor_encrypted: page
                .next_cursor
                .as_deref()
                .map(|cursor| state.cipher.encrypt(cursor.as_bytes()))
                .transpose()?,
            next_cursor_digest,
            is_final: page.next_cursor.is_none(),
            items,
        };
        let is_final = commit.is_final;
        let committed_items = commit.items.len() as u64;
        let commit_span = tracing::info_span!(
            "coordinator.collection.page.commit",
            batch_id = %batch.view.id,
            generation = batch.cursor_generation,
            page.items = committed_items,
            page.final = is_final,
            commit.outcome = tracing::field::Empty,
        );
        let commit_result = state
            .store
            .commit_collection_page(commit)
            .instrument(commit_span.clone())
            .await;
        let commit_outcome = match commit_result {
            Ok(CommitPageOutcome::Applied) => "applied",
            Ok(CommitPageOutcome::Replayed) => "replayed",
            Ok(CommitPageOutcome::Stale) => "stale",
            Err(error) => {
                commit_span.record("commit.outcome", "error");
                record_collection_error("commit", "collection_page_commit_failed");
                meter
                    .u64_counter("scheduler.collection.page.commits")
                    .build()
                    .add(
                        1,
                        &[
                            KeyValue::new("source", source),
                            KeyValue::new("outcome", "error"),
                        ],
                    );
                return Err(error);
            }
        };
        commit_span.record("commit.outcome", commit_outcome);
        let page_attributes = [
            KeyValue::new("source", source),
            KeyValue::new("outcome", commit_outcome),
        ];
        meter
            .u64_counter("scheduler.collection.page.commits")
            .build()
            .add(1, &page_attributes);
        meter
            .u64_histogram("scheduler.collection.page.items")
            .build()
            .record(committed_items, &page_attributes);
        meter
            .f64_histogram("scheduler.collection.page.duration_ms")
            .build()
            .record(
                page_started.elapsed().as_secs_f64() * 1_000.0,
                &page_attributes,
            );
        if commit_outcome == "stale" {
            return Ok(());
        }
        *batch = state
            .store
            .get_batch(batch.view.id)
            .await?
            .context("collection batch disappeared after page commit")?;
        info!(
            batch_id = %batch.view.id,
            generation = batch.cursor_generation,
            item_count = batch.view.item_count,
            is_final,
            "parameter collection page committed"
        );
        if is_final {
            finalize_claimed_batch(state, batch, &lease_token).await?;
            return Ok(());
        }
    }
}

async fn finalize_claimed_batch(
    state: &AppState,
    batch: &BatchRecord,
    lease_token: &str,
) -> Result<()> {
    let finalization_started = Instant::now();
    let finalization_span = tracing::info_span!(
        "coordinator.collection.batch.finalize",
        batch_id = %batch.view.id,
        finalization.outcome = tracing::field::Empty,
    );
    let finalization_result = state
        .store
        .finalize_batch(batch.view.id, lease_token)
        .instrument(finalization_span.clone())
        .await;
    let meter = opentelemetry::global::meter("scheduler-coordinator");
    let outcome = match finalization_result {
        Ok(FinalizeBatchOutcome::Finalized) => {
            info!(
                batch_id = %batch.view.id,
                valid_items = batch.view.valid_item_count,
                invalid_items = batch.view.invalid_item_count,
                "parameter collection batch finalized"
            );
            "finalized"
        }
        Ok(FinalizeBatchOutcome::AlreadyFinalized) => "already_finalized",
        Err(error) => {
            finalization_span.record("finalization.outcome", "error");
            record_collection_error("finalize", "collection_finalization_failed");
            meter
                .u64_counter("scheduler.collection.finalizations")
                .build()
                .add(1, &[KeyValue::new("outcome", "error")]);
            meter
                .f64_histogram("scheduler.collection.finalization.duration_ms")
                .build()
                .record(
                    finalization_started.elapsed().as_secs_f64() * 1_000.0,
                    &[KeyValue::new("outcome", "error")],
                );
            return Err(error);
        }
    };
    finalization_span.record("finalization.outcome", outcome);
    meter
        .u64_counter("scheduler.collection.finalizations")
        .build()
        .add(1, &[KeyValue::new("outcome", outcome)]);
    meter
        .f64_histogram("scheduler.collection.finalization.duration_ms")
        .build()
        .record(
            finalization_started.elapsed().as_secs_f64() * 1_000.0,
            &[KeyValue::new("outcome", outcome)],
        );
    Ok(())
}

async fn materialize_items(
    state: &AppState,
    batch: &BatchRecord,
    seed: &BatchExecutionSeed,
    overrides: &Value,
    raw_items: Vec<Value>,
) -> Result<Vec<NewBatchItem>> {
    let mut items = Vec::with_capacity(raw_items.len());
    for (page_index, raw) in raw_items.into_iter().enumerate() {
        let item_index = batch.view.item_count + page_index as u32;
        items.push(materialize_item(state, batch, seed, overrides, item_index, raw).await?);
    }
    Ok(items)
}

async fn materialize_item(
    state: &AppState,
    batch: &BatchRecord,
    seed: &BatchExecutionSeed,
    overrides: &Value,
    item_index: u32,
    raw: Value,
) -> Result<NewBatchItem> {
    let mut failure_code = None;
    let mut held_as_poison = false;
    let (provider_key, item_parameters) = match raw.as_object() {
        Some(object) => {
            if object
                .keys()
                .any(|name| !matches!(name.as_str(), "key" | "parameters"))
            {
                failure_code = Some("collection_item_invalid_shape");
            }
            let provider_key = match object.get("key") {
                Some(Value::String(key)) if !key.is_empty() && key.len() <= 256 => key.clone(),
                Some(Value::String(_)) => {
                    failure_code = Some("collection_item_invalid_key");
                    String::new()
                }
                Some(_) => {
                    failure_code = Some("collection_item_invalid_key");
                    String::new()
                }
                None => {
                    failure_code = Some("collection_item_missing_key");
                    String::new()
                }
            };
            let parameters = match object.get("parameters") {
                Some(Value::Object(parameters)) => Value::Object(parameters.clone()),
                _ => {
                    failure_code.get_or_insert("collection_item_invalid_parameters");
                    Value::Object(Map::new())
                }
            };
            (provider_key, parameters)
        }
        None => {
            failure_code = Some("collection_item_invalid_shape");
            (String::new(), Value::Object(Map::new()))
        }
    };
    let internal_key = if provider_key.is_empty() {
        format!("invalid:{}:{item_index}", batch.view.id)
    } else {
        provider_key.clone()
    };
    let mut final_parameters = Value::Object(Map::new());
    let mut snapshot = None;
    let mut policy = None;
    if failure_code.is_none() {
        match merge_parameters(&seed.resolved.base_parameters, &item_parameters)
            .and_then(|parameters| merge_parameters(&parameters, overrides))
        {
            Ok(parameters) => {
                final_parameters = parameters;
                match resolve_snapshot(&seed.resolved, &final_parameters, &seed.required_labels) {
                    Ok(execution) => {
                        let execution_plaintext = serde_json::to_vec(&execution)?;
                        let blueprint_scope = &execution.blueprint_digest;
                        let input_fingerprint = state.cipher.input_fingerprint(
                            blueprint_scope,
                            &serde_json::json!({"parameters_digest": execution.parameters_digest}),
                        )?;
                        if state
                            .store
                            .consume_probe_or_is_held(blueprint_scope, &input_fingerprint)
                            .await?
                        {
                            held_as_poison = true;
                            failure_code = Some("collection_input_poisoned");
                        } else {
                            policy = Some(execution.policy.clone());
                            snapshot = Some(state.cipher.encrypt(&execution_plaintext)?);
                        }
                    }
                    Err(error) => {
                        failure_code = Some(if error.to_string().contains("failed validation") {
                            "collection_item_schema_invalid"
                        } else {
                            "collection_item_render_failed"
                        });
                    }
                }
            }
            Err(_) => failure_code = Some("collection_item_merge_failed"),
        }
    }
    let parameter_fingerprint = state
        .cipher
        .input_fingerprint(&batch.snapshot_digest, &final_parameters)?;
    let key_fingerprint =
        keyed_text_digest(&state.cipher, "collection-provider-key", &internal_key)?;
    Ok(NewBatchItem {
        id: Uuid::new_v4(),
        item_index,
        provider_key_encrypted: state.cipher.encrypt(provider_key.as_bytes())?,
        provider_key_hmac: key_fingerprint,
        encrypted_parameters: state
            .cipher
            .encrypt(&serde_json::to_vec(&final_parameters)?)?,
        encrypted_snapshot: snapshot,
        key_id: state.cipher.key_id().into(),
        parameters_digest: parameter_fingerprint,
        state: if held_as_poison {
            BatchItemState::Held
        } else if failure_code.is_some() {
            BatchItemState::Invalid
        } else {
            BatchItemState::Ready
        },
        failure_code: failure_code.map(str::to_owned),
        max_attempts: policy.as_ref().map(|policy| policy.max_attempts),
        initial_backoff_seconds: policy.as_ref().map(|policy| policy.initial_backoff_seconds),
        backoff_cap_seconds: policy.as_ref().map(|policy| policy.backoff_cap_seconds),
    })
}

fn keyed_text_digest(cipher: &SnapshotCipher, domain: &str, value: &str) -> Result<String> {
    cipher.input_fingerprint(domain, &Value::String(value.to_owned()))
}

fn decrypt_utf8(cipher: &SnapshotCipher, encrypted: &[u8]) -> Result<String> {
    String::from_utf8(cipher.decrypt(encrypted)?)
        .context("encrypted collection metadata is not UTF-8")
}

async fn cursor_digest_seen(state: &AppState, batch_id: Uuid, digest: &str) -> Result<bool> {
    let seen: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM batch_collection_pages WHERE batch_id=? AND request_cursor_digest=?)",
    )
    .bind(batch_id.to_string())
    .bind(digest)
    .fetch_one(state.store.pool())
    .await?;
    Ok(seen)
}

#[derive(Debug, Deserialize)]
pub struct BatchListQuery {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    pub provider_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct CursorPage<T> {
    items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BatchCursor {
    created_at: String,
    id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
struct BatchItemCursor {
    item_index: u32,
    id: Uuid,
}

fn encode_cursor<T: Serialize>(cursor: &T) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor)?))
}

fn decode_cursor<T: for<'de> Deserialize<'de>>(cursor: Option<&str>) -> Result<Option<T>> {
    cursor
        .map(|cursor| {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| collection_error("pagination_cursor_invalid"))?;
            serde_json::from_slice(&bytes)
                .map_err(|_| collection_error("pagination_cursor_invalid"))
        })
        .transpose()
}

pub async fn list_batches(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BatchListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let cursor: Option<BatchCursor> = decode_cursor(query.cursor.as_deref())?;
    let (created_at, cursor_id) = cursor
        .map(|cursor| (Some(cursor.created_at), Some(cursor.id.to_string())))
        .unwrap_or_default();
    let rows = sqlx::query(
        "SELECT * FROM batches WHERE (? IS NULL OR created_at<? OR (created_at=? AND id<?)) ORDER BY created_at DESC,id DESC LIMIT ?",
    )
    .bind(&created_at)
    .bind(&created_at)
    .bind(&created_at)
    .bind(&cursor_id)
    .bind(i64::from(limit + 1))
    .fetch_all(state.store.pool())
    .await?;
    let has_more = rows.len() > limit as usize;
    let mut batches = rows
        .into_iter()
        .take(limit as usize)
        .map(|row| {
            Ok(scheduler_store::BatchView {
                id: Uuid::parse_str(row.try_get::<String, _>("id")?.as_str())?,
                schedule_id: Uuid::parse_str(row.try_get::<String, _>("schedule_id")?.as_str())?,
                schedule_revision: row.try_get("schedule_revision")?,
                state: scheduler_core::BatchState::parse(
                    row.try_get::<String, _>("state")?.as_str(),
                )?,
                trigger_kind: row.try_get("trigger_kind")?,
                scheduled_at: parse_database_time(row.try_get("scheduled_at")?)?,
                item_count: row.try_get::<i64, _>("item_count")? as u32,
                valid_item_count: row.try_get::<i64, _>("valid_item_count")? as u32,
                invalid_item_count: row.try_get::<i64, _>("invalid_item_count")? as u32,
                poisoned_item_count: row.try_get::<i64, _>("poisoned_item_count")? as u32,
                held_item_count: row.try_get::<i64, _>("held_item_count")? as u32,
                failure_code: row.try_get("failure_code")?,
                created_at: parse_database_time(row.try_get("created_at")?)?,
                updated_at: parse_database_time(row.try_get("updated_at")?)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let next_cursor = if has_more {
        batches
            .last()
            .map(|batch| {
                encode_cursor(&BatchCursor {
                    created_at: batch
                        .created_at
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    id: batch.id,
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(Json(serde_json::to_value(CursorPage {
        items: std::mem::take(&mut batches),
        next_cursor,
    })?))
}

pub async fn get_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let batch = state
        .store
        .get_batch(id)
        .await?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(serde_json::to_value(batch.view)?))
}

#[derive(Debug, Serialize)]
struct BatchItemAdminView {
    #[serde(flatten)]
    item: BatchItemView,
    provider_key: String,
}

pub async fn list_batch_items(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(query): Query<BatchListQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if state.store.get_batch(id).await?.is_none() {
        return Err(ApiError::not_found());
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let cursor: Option<BatchItemCursor> = decode_cursor(query.cursor.as_deref())?;
    let (cursor_index, cursor_id) = cursor
        .map(|cursor| {
            (
                Some(i64::from(cursor.item_index)),
                Some(cursor.id.to_string()),
            )
        })
        .unwrap_or_default();
    if query
        .provider_key
        .as_ref()
        .is_some_and(|key| key.is_empty() || key.len() > 256)
    {
        return Err(collection_error("collection_provider_key_invalid").into());
    }
    let provider_digest = query
        .provider_key
        .as_deref()
        .map(|key| keyed_text_digest(&state.cipher, "collection-provider-key", key))
        .transpose()?;
    let rows = batch_item_page_rows(
        state.store.pool(),
        id,
        provider_digest.as_deref(),
        cursor_index,
        cursor_id.as_deref(),
        limit as usize,
    )
    .await?;
    let has_more = rows.len() > limit as usize;
    let items = rows
        .into_iter()
        .take(limit as usize)
        .map(|row| {
            let encrypted: Vec<u8> = row.try_get("provider_key_encrypted")?;
            Ok(BatchItemAdminView {
                item: BatchItemView {
                    id: Uuid::parse_str(row.try_get::<String, _>("id")?.as_str())?,
                    batch_id: Uuid::parse_str(row.try_get::<String, _>("batch_id")?.as_str())?,
                    item_index: row.try_get::<i64, _>("item_index")? as u32,
                    parameters_digest: row.try_get("parameters_digest")?,
                    state: BatchItemState::parse(row.try_get::<String, _>("state")?.as_str())?,
                    failure_code: row.try_get("failure_code")?,
                    run_id: row
                        .try_get::<Option<String>, _>("run_id")?
                        .map(|id| Uuid::parse_str(&id))
                        .transpose()?,
                    created_at: parse_database_time(row.try_get("created_at")?)?,
                    updated_at: parse_database_time(row.try_get("updated_at")?)?,
                },
                provider_key: decrypt_utf8(&state.cipher, &encrypted)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| {
                encode_cursor(&BatchItemCursor {
                    item_index: item.item.item_index,
                    id: item.item.id,
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(Json(serde_json::to_value(CursorPage {
        items,
        next_cursor,
    })?))
}

pub(crate) async fn batch_item_page_rows(
    pool: &sqlx::SqlitePool,
    batch_id: Uuid,
    provider_digest: Option<&str>,
    cursor_index: Option<i64>,
    cursor_id: Option<&str>,
    limit: usize,
) -> Result<Vec<sqlx::sqlite::SqliteRow>> {
    Ok(sqlx::query(
        "SELECT id,batch_id,item_index,parameters_digest,state,failure_code,run_id,created_at,updated_at,\
         provider_key_encrypted,encrypted_parameters FROM batch_items WHERE batch_id=? \
         AND (? IS NULL OR provider_key_hmac=?) \
         AND (? IS NULL OR item_index>? OR (item_index=? AND id>?)) \
         ORDER BY item_index,id LIMIT ?",
    )
    .bind(batch_id.to_string())
    .bind(provider_digest)
    .bind(provider_digest)
    .bind(cursor_index)
    .bind(cursor_index)
    .bind(cursor_index)
    .bind(cursor_id)
    .bind((limit + 1) as i64)
    .fetch_all(pool)
    .await?)
}

fn parse_database_time(value: String) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&value)?.with_timezone(&Utc))
}

pub async fn cancel_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    authorize(&state, &headers)?;
    for (agent_id, attempt_id) in state.store.cancel_batch(id).await? {
        state
            .send_to_agent(
                &agent_id,
                CoordinatorMessage {
                    payload: Some(coordinator_message::Payload::Cancel(CancelAttempt {
                        attempt_id: attempt_id.to_string(),
                    })),
                },
            )
            .await;
    }
    Ok(StatusCode::ACCEPTED)
}

pub async fn retrigger_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    authorize(&state, &headers)?;
    let batch = state
        .store
        .retrigger_batch_snapshot(id, Uuid::new_v4(), Utc::now())
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"kind": "batch", "batch": batch})),
    ))
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    if state.auth.verify_bearer(headers) {
        Ok(())
    } else {
        Err(ApiError::unauthorized())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use scheduler_core::{AdapterRegistry, ArtifactRef, ScheduleSpec};
    use scheduler_store::NewSchedule;
    use tokio::sync::RwLock;

    use super::*;

    #[test]
    fn static_json_and_ndjson_are_paged_deterministically() {
        let json = br#"[{"key":"a","parameters":{}},{"key":"b","parameters":{}},{"key":"c","parameters":{}}]"#;
        let first = static_page(json.to_vec(), None, None, 2).expect("first page");
        assert_eq!(first.items.len(), 2);
        assert_eq!(first.next_cursor.as_deref(), Some("offset:2"));
        let second = static_page(
            json.to_vec(),
            first.next_cursor.as_deref(),
            Some(&first.snapshot_id),
            2,
        )
        .expect("second page");
        assert_eq!(second.items.len(), 1);
        assert!(second.next_cursor.is_none());

        let ndjson = b"{\"key\":\"a\",\"parameters\":{}}\n{\"key\":\"b\",\"parameters\":{}}\n";
        let page = static_page(ndjson.to_vec(), None, None, 10).expect("ndjson");
        assert_eq!(page.items.len(), 2);
    }

    #[test]
    fn snapshot_drift_and_bad_cursors_fail_closed() {
        let json = br#"[{"key":"a","parameters":{}}]"#;
        let error =
            static_page(json.to_vec(), None, Some("different"), 10).expect_err("snapshot drift");
        assert_eq!(
            error
                .downcast_ref::<CollectionRuntimeError>()
                .expect("typed error")
                .code(),
            "collection_snapshot_drift"
        );
        assert!(static_page(json.to_vec(), Some("opaque"), None, 10).is_err());
    }

    #[test]
    fn connector_pages_reject_cycles_and_oversized_pages() {
        assert!(
            validate_source_page(
                "snapshot".into(),
                vec![Value::Null],
                Some("same".into()),
                Some("same"),
                Some("snapshot"),
                1,
            )
            .is_err()
        );
        assert!(
            validate_source_page(
                "snapshot".into(),
                vec![Value::Null, Value::Null],
                None,
                None,
                None,
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn connector_collection_protocol_carries_snapshot_and_opaque_cursor() {
        let request = ConnectorPageRequest {
            api_version: COLLECTION_API_VERSION,
            resource: "/daily?tenant=internal",
            cursor: Some("opaque-next"),
            snapshot_id: Some("snapshot-7"),
            page_size: 500,
        };
        let request = serde_json::to_value(request).expect("request JSON");
        assert_eq!(request["api_version"], COLLECTION_API_VERSION);
        assert_eq!(request["resource"], "/daily?tenant=internal");
        assert_eq!(request["cursor"], "opaque-next");
        assert_eq!(request["snapshot_id"], "snapshot-7");

        let response: ConnectorPageResponse = serde_json::from_value(serde_json::json!({
            "api_version": COLLECTION_API_VERSION,
            "snapshot_id": "snapshot-7",
            "items": [{"key": "a", "parameters": {}}],
            "next_cursor": "opaque-next-2"
        }))
        .expect("response JSON");
        let page = validate_source_page(
            response.snapshot_id,
            response.items,
            response.next_cursor,
            Some("opaque-next"),
            Some("snapshot-7"),
            500,
        )
        .expect("valid page");
        assert_eq!(page.next_cursor.as_deref(), Some("opaque-next-2"));
    }

    #[tokio::test]
    async fn completed_ingestion_is_finalized_after_worker_restart() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let blueprint_path = directory.path().join("blueprint.yaml");
        let parameters_path = directory.path().join("base.json");
        let collection_path = directory.path().join("empty-items.json");
        std::fs::write(
            &blueprint_path,
            br#"api_version: scheduler/v1
executor:
  kind: command
  program: /bin/true
  args: []
parameters_schema:
  type: object
  additionalProperties: false
"#,
        )
        .expect("blueprint");
        std::fs::write(&parameters_path, b"{}").expect("parameters");
        std::fs::write(&collection_path, b"[]").expect("collection");

        let roots = vec![directory.path().to_path_buf()];
        let store = scheduler_store::Store::connect(
            &format!(
                "sqlite://{}",
                directory.path().join("coordinator.db").display()
            ),
            None,
        )
        .await
        .expect("store");
        let cipher = SnapshotCipher::from_base64("test", &SnapshotCipher::generate_base64())
            .expect("cipher");
        let state = AppState {
            store,
            cipher,
            adapters: AdapterRegistry::with_defaults(roots.clone(), HashMap::new())
                .expect("adapters"),
            auth: crate::auth::AuthManager::new("test-admin-token", false).expect("auth"),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            internal_rest_url: "http://127.0.0.1:1".into(),
            internal_admin_token: "test-admin-token".into(),
            collection_sources: CollectionSourceRegistry::new(roots, None)
                .expect("collection sources"),
            collection_worker_id: "replacement-worker".into(),
        };
        let file_url =
            |path: &std::path::Path| Url::from_file_path(path).expect("file URL").to_string();
        let spec = ScheduleSpec {
            name: "restart boundary".into(),
            blueprint_ref: ArtifactRef {
                uri: file_url(&blueprint_path),
            },
            parameters_ref: ArtifactRef {
                uri: file_url(&parameters_path),
            },
            parameter_collection: Some(ParameterCollectionSpec {
                source_ref: ArtifactRef {
                    uri: file_url(&collection_path),
                },
                page_size: 1,
                max_items: 10,
                max_active_runs: 2,
                poison_distinct_nodes: 2,
            }),
            required_labels: BTreeMap::new(),
            cron: None,
            webhook_enabled: false,
            enabled: true,
        };
        let (encrypted_snapshot, snapshot_digest) = crate::api::resolve_and_encrypt(&state, &spec)
            .await
            .expect("resolved schedule");
        let schedule_id = Uuid::new_v4();
        state
            .store
            .create_schedule(NewSchedule {
                id: schedule_id,
                spec,
                encrypted_snapshot,
                snapshot_digest,
                key_id: state.cipher.key_id().into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("schedule");
        let schedule = state
            .store
            .get_schedule_record(schedule_id)
            .await
            .expect("schedule lookup")
            .expect("schedule exists");
        let created = create_batch_from_schedule(
            &state,
            &schedule,
            &serde_json::json!({}),
            "manual",
            Utc::now(),
            None,
        )
        .await
        .expect("batch");

        let mut claims = state
            .store
            .claim_collection_batches("original-worker", COLLECTION_LEASE_SECONDS, 1)
            .await
            .expect("claim batch");
        let claimed = claims.pop().expect("claimed batch");
        let page =
            static_page(b"[]".to_vec(), None, None, claimed.page_size).expect("empty final page");
        let page_digest_value = serde_json::json!({
            "snapshot_id": &page.snapshot_id,
            "next": &page.next_cursor,
            "items": Vec::<Value>::new(),
        });
        let committed = state
            .store
            .commit_collection_page(CommitCollectionPage {
                batch_id: claimed.view.id,
                lease_token: claimed.lease_token.clone().expect("lease token"),
                expected_generation: claimed.cursor_generation,
                request_cursor_digest: claimed.next_cursor_digest.clone(),
                page_digest: state
                    .cipher
                    .input_fingerprint("collection-page", &page_digest_value)
                    .expect("page digest"),
                collection_snapshot_encrypted: state
                    .cipher
                    .encrypt(page.snapshot_id.as_bytes())
                    .expect("encrypted snapshot"),
                collection_snapshot_digest: keyed_text_digest(
                    &state.cipher,
                    "collection-snapshot",
                    &page.snapshot_id,
                )
                .expect("snapshot digest"),
                next_cursor_encrypted: None,
                next_cursor_digest: keyed_text_digest(
                    &state.cipher,
                    "collection-cursor",
                    "complete",
                )
                .expect("terminal cursor digest"),
                is_final: true,
                items: Vec::new(),
            })
            .await
            .expect("commit final page");
        assert_eq!(committed, CommitPageOutcome::Applied);
        let persisted = state
            .store
            .get_batch(created.id)
            .await
            .expect("batch lookup")
            .expect("batch exists");
        assert!(persisted.ingestion_complete);
        assert_eq!(persisted.view.state, scheduler_core::BatchState::Collecting);

        sqlx::query("UPDATE batches SET lease_expires_at=? WHERE id=?")
            .bind(
                (Utc::now() - chrono::Duration::seconds(1))
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            )
            .bind(created.id.to_string())
            .execute(state.store.pool())
            .await
            .expect("expire original worker lease");
        let mut reclaimed = state
            .store
            .claim_collection_batches("replacement-worker", COLLECTION_LEASE_SECONDS, 1)
            .await
            .expect("reclaim batch");
        let mut reclaimed = reclaimed.pop().expect("reclaimed batch");
        assert!(reclaimed.ingestion_complete);

        process_claimed_batch_inner(&state, &mut reclaimed)
            .await
            .expect("restart finalization");
        let finalized = state
            .store
            .get_batch(created.id)
            .await
            .expect("batch lookup")
            .expect("batch exists");
        assert_eq!(finalized.view.state, scheduler_core::BatchState::Succeeded);
        assert!(finalized.lease_token.is_none());
        assert_eq!(finalized.cursor_generation, 1);
    }

    #[tokio::test]
    async fn static_collection_creates_valid_runs_and_quarantines_bad_items() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let blueprint_path = directory.path().join("blueprint.yaml");
        let parameters_path = directory.path().join("base.json");
        let collection_path = directory.path().join("items.ndjson");
        std::fs::write(
            &blueprint_path,
            br#"api_version: scheduler/v1
executor:
  kind: command
  program: /bin/true
  args: ["{{params.customer}}"]
parameters_schema:
  type: object
  additionalProperties: false
  required: [customer]
  properties:
    customer: {type: string}
"#,
        )
        .expect("blueprint");
        std::fs::write(&parameters_path, b"{}").expect("parameters");
        std::fs::write(
            &collection_path,
            b"{\"key\":\"provider-secret-42\",\"parameters\":{\"customer\":\"account-42\"}}\n{\"key\":\"provider-secret-43\",\"parameters\":{}}\n",
        )
        .expect("collection");
        let roots = vec![directory.path().to_path_buf()];
        let store = scheduler_store::Store::connect(
            &format!(
                "sqlite://{}",
                directory.path().join("coordinator.db").display()
            ),
            None,
        )
        .await
        .expect("store");
        let cipher = SnapshotCipher::from_base64("test", &SnapshotCipher::generate_base64())
            .expect("cipher");
        let state = AppState {
            store,
            cipher,
            adapters: AdapterRegistry::with_defaults(roots.clone(), HashMap::new())
                .expect("adapters"),
            auth: crate::auth::AuthManager::new("test-admin-token", false).expect("auth"),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            internal_rest_url: "http://127.0.0.1:1".into(),
            internal_admin_token: "test-admin-token".into(),
            collection_sources: CollectionSourceRegistry::new(roots, None)
                .expect("collection sources"),
            collection_worker_id: "test-collector".into(),
        };
        let file_url =
            |path: &std::path::Path| Url::from_file_path(path).expect("file URL").to_string();
        let spec = ScheduleSpec {
            name: "collection".into(),
            blueprint_ref: ArtifactRef {
                uri: file_url(&blueprint_path),
            },
            parameters_ref: ArtifactRef {
                uri: file_url(&parameters_path),
            },
            parameter_collection: Some(ParameterCollectionSpec {
                source_ref: ArtifactRef {
                    uri: file_url(&collection_path),
                },
                page_size: 1,
                max_items: 10,
                max_active_runs: 2,
                poison_distinct_nodes: 2,
            }),
            required_labels: BTreeMap::new(),
            cron: None,
            webhook_enabled: false,
            enabled: true,
        };
        let (encrypted_snapshot, snapshot_digest) = crate::api::resolve_and_encrypt(&state, &spec)
            .await
            .expect("resolved schedule");
        let schedule_id = Uuid::new_v4();
        state
            .store
            .create_schedule(NewSchedule {
                id: schedule_id,
                spec,
                encrypted_snapshot,
                snapshot_digest,
                key_id: state.cipher.key_id().into(),
                webhook_public_id: None,
                webhook_secret_hash: None,
            })
            .await
            .expect("schedule");
        let schedule = state
            .store
            .get_schedule_record(schedule_id)
            .await
            .expect("schedule lookup")
            .expect("schedule exists");
        let created = create_batch_from_schedule(
            &state,
            &schedule,
            &serde_json::json!({}),
            "manual",
            Utc::now(),
            Some("same-request".into()),
        )
        .await
        .expect("batch");
        let replay = create_batch_from_schedule(
            &state,
            &schedule,
            &serde_json::json!({}),
            "manual",
            Utc::now(),
            Some("same-request".into()),
        )
        .await
        .expect("idempotent batch");
        assert_eq!(replay.id, created.id);

        collection_worker_pass(&state)
            .await
            .expect("collection pass");
        let batch = state
            .store
            .get_batch(created.id)
            .await
            .expect("batch lookup")
            .expect("batch exists");
        assert_eq!(batch.view.state, scheduler_core::BatchState::Running);
        assert_eq!(batch.view.item_count, 2);
        assert_eq!(batch.view.valid_item_count, 1);
        assert_eq!(batch.view.invalid_item_count, 1);
        let items = state
            .store
            .list_batch_items(created.id, 10)
            .await
            .expect("items");
        assert_eq!(items[0].state, BatchItemState::Queued);
        assert_eq!(items[1].state, BatchItemState::Invalid);
        assert_eq!(
            items[1].failure_code.as_deref(),
            Some("collection_item_schema_invalid")
        );
        assert_eq!(state.store.queued_runs(10).await.expect("runs").len(), 1);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer test-admin-token".parse().expect("authorization"),
        );
        let first_page = list_batch_items(
            State(state.clone()),
            headers.clone(),
            Path(created.id),
            Query(BatchListQuery {
                limit: Some(1),
                cursor: None,
                provider_key: None,
            }),
        )
        .await
        .expect("first API item page")
        .0;
        assert_eq!(first_page["items"].as_array().expect("items").len(), 1);
        let cursor = first_page["next_cursor"]
            .as_str()
            .expect("next cursor")
            .to_owned();
        let second_page = list_batch_items(
            State(state.clone()),
            headers,
            Path(created.id),
            Query(BatchListQuery {
                limit: Some(1),
                cursor: Some(cursor),
                provider_key: None,
            }),
        )
        .await
        .expect("second API item page")
        .0;
        assert_eq!(second_page["items"].as_array().expect("items").len(), 1);
        assert!(second_page.get("next_cursor").is_none());
        let audit = state
            .store
            .audit_events("batch", &created.id.to_string(), 100)
            .await
            .expect("audit");
        let audit_json = serde_json::to_string(&audit).expect("audit JSON");
        assert!(!audit_json.contains("account-42"));
        assert!(!audit_json.contains("provider-secret"));
    }
}
