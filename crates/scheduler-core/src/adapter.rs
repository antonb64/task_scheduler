use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, ETAG, HeaderValue, LAST_MODIFIED};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::{Host, Url};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    Blueprint,
    Parameters,
}

impl ArtifactKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Blueprint => "blueprint",
            Self::Parameters => "parameters",
        }
    }
}

impl std::fmt::Display for ArtifactKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable, operator-safe artifact connector failures.
///
/// These variants deliberately retain no URI, bearer token, response body, or
/// transport error text. Callers can downcast `anyhow::Error` to this type for
/// status mapping without parsing display strings.
#[derive(Debug, Error)]
pub enum ArtifactFetchError {
    #[error("artifact connector {connector} is not configured")]
    NotConfigured { connector: String },
    #[error("invalid artifact reference: {reason}")]
    InvalidReference { reason: &'static str },
    #[error("artifact connector {connector} does not provide {kind} artifacts")]
    UnsupportedKind {
        connector: String,
        kind: ArtifactKind,
    },
    #[error("artifact connector {connector} timed out while fetching a {kind} artifact")]
    Timeout {
        connector: String,
        kind: ArtifactKind,
    },
    #[error("artifact connector {connector} transport failed while fetching a {kind} artifact")]
    Transport {
        connector: String,
        kind: ArtifactKind,
    },
    #[error("artifact connector {connector} returned HTTP status {status} for a {kind} artifact")]
    UpstreamStatus {
        connector: String,
        kind: ArtifactKind,
        status: u16,
    },
    #[error("artifact connector {connector} returned an invalid {kind} response: {reason}")]
    InvalidResponse {
        connector: String,
        kind: ArtifactKind,
        reason: &'static str,
    },
    #[error("{kind} artifact exceeds size limit")]
    TooLarge {
        connector: Option<String>,
        kind: ArtifactKind,
    },
}

impl ArtifactFetchError {
    pub const fn class(&self) -> &'static str {
        match self {
            Self::NotConfigured { .. } => "not_configured",
            Self::InvalidReference { .. } => "invalid_reference",
            Self::UnsupportedKind { .. } => "unsupported_kind",
            Self::Timeout { .. } => "timeout",
            Self::Transport { .. } => "transport",
            Self::UpstreamStatus { .. } => "upstream_status",
            Self::InvalidResponse { .. } => "invalid_response",
            Self::TooLarge { .. } => "too_large",
        }
    }

    pub fn connector(&self) -> Option<&str> {
        match self {
            Self::NotConfigured { connector }
            | Self::UnsupportedKind { connector, .. }
            | Self::Timeout { connector, .. }
            | Self::Transport { connector, .. }
            | Self::UpstreamStatus { connector, .. }
            | Self::InvalidResponse { connector, .. } => Some(connector),
            Self::TooLarge { connector, .. } => connector.as_deref(),
            Self::InvalidReference { .. } => None,
        }
    }

    pub const fn kind(&self) -> Option<ArtifactKind> {
        match self {
            Self::UnsupportedKind { kind, .. }
            | Self::Timeout { kind, .. }
            | Self::Transport { kind, .. }
            | Self::UpstreamStatus { kind, .. }
            | Self::InvalidResponse { kind, .. }
            | Self::TooLarge { kind, .. } => Some(*kind),
            Self::NotConfigured { .. } | Self::InvalidReference { .. } => None,
        }
    }

    pub const fn upstream_status(&self) -> Option<u16> {
        match self {
            Self::UpstreamStatus { status, .. } => Some(*status),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Artifact {
    pub bytes: Vec<u8>,
    pub media_type: Option<String>,
    pub source_version: Option<String>,
}

#[async_trait]
pub trait ArtifactAdapter: Send + Sync {
    async fn fetch(&self, uri: &Url, kind: ArtifactKind) -> Result<Artifact>;
}

/// Bootstrap configuration for named, out-of-process HTTP artifact providers.
///
/// This document is intentionally loaded only at coordinator startup. It may
/// refer to secret-bearing environment variables and must not be synchronized
/// through the management settings API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorConfig {
    pub api_version: String,
    #[serde(default)]
    pub connectors: HashMap<String, ConnectorEndpointConfig>,
}

impl ConnectorConfig {
    pub fn from_slice(document: &[u8]) -> Result<Self> {
        serde_json::from_slice(document)
            .or_else(|_| serde_yaml::from_slice(document))
            .context("connector config must be valid JSON or YAML")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorEndpointConfig {
    pub base_url: String,
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    #[serde(default = "default_connector_kinds")]
    pub allowed_kinds: Vec<ArtifactKind>,
    #[serde(default = "default_connect_timeout_seconds")]
    pub connect_timeout_seconds: u64,
    #[serde(default = "default_connector_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub allow_insecure_http: bool,
}

fn default_connector_kinds() -> Vec<ArtifactKind> {
    vec![ArtifactKind::Parameters]
}

const fn default_connect_timeout_seconds() -> u64 {
    5
}

const fn default_connector_timeout_seconds() -> u64 {
    20
}

#[derive(Clone, Default)]
pub struct AdapterRegistry {
    adapters: HashMap<String, Arc<dyn ArtifactAdapter>>,
}

impl AdapterRegistry {
    pub fn with_defaults(
        allowed_file_roots: Vec<PathBuf>,
        http_headers: HashMap<String, String>,
    ) -> Result<Self> {
        let mut registry = Self::default();
        registry.register("file", Arc::new(FileAdapter::new(allowed_file_roots)?));
        let http = Arc::new(HttpAdapter::new(http_headers)?);
        registry.register("http", http.clone());
        registry.register("https", http);
        Ok(registry)
    }

    pub fn register(&mut self, scheme: impl Into<String>, adapter: Arc<dyn ArtifactAdapter>) {
        self.adapters.insert(scheme.into(), adapter);
    }

    pub fn register_connectors(&mut self, config: ConnectorConfig) -> Result<()> {
        self.register("connector", Arc::new(ConnectorAdapter::new(config)?));
        Ok(())
    }

    pub async fn fetch(&self, reference: &str, kind: ArtifactKind) -> Result<Artifact> {
        let uri = Url::parse(reference).map_err(|_| ArtifactFetchError::InvalidReference {
            reason: "artifact URI is invalid",
        })?;
        let adapter = if let Some(adapter) = self.adapters.get(uri.scheme()) {
            adapter
        } else if uri.scheme() == "connector" {
            let started = std::time::Instant::now();
            let error = match connector_name(&uri) {
                Ok(connector) => ArtifactFetchError::NotConfigured {
                    connector: connector.to_owned(),
                },
                Err(error) => error,
            };
            tracing::warn!(
                target: "scheduler.connector",
                connector_name = error.connector().unwrap_or("<invalid>"),
                artifact_kind = kind.as_str(),
                error_class = error.class(),
                upstream_status = error.upstream_status(),
                duration_ms = started.elapsed().as_millis() as u64,
                "artifact connector fetch failed"
            );
            return Err(error.into());
        } else {
            return Err(ArtifactFetchError::InvalidReference {
                reason: "artifact URI scheme has no registered adapter",
            }
            .into());
        };
        adapter.fetch(&uri, kind).await
    }
}

struct ConnectorAdapter {
    connectors: HashMap<String, NamedConnector>,
}

struct NamedConnector {
    endpoint: Url,
    client: reqwest::Client,
    authorization: Option<HeaderValue>,
    allowed_kinds: HashSet<ArtifactKind>,
}

#[derive(Serialize)]
struct ConnectorFetchRequest<'a> {
    api_version: &'static str,
    kind: &'static str,
    resource: &'a str,
}

impl ConnectorAdapter {
    fn new(config: ConnectorConfig) -> Result<Self> {
        Self::new_with_env(config, |name| {
            std::env::var(name).with_context(|| {
                format!("connector bearer token environment variable {name} is not set")
            })
        })
    }

    fn new_with_env<F>(config: ConnectorConfig, environment: F) -> Result<Self>
    where
        F: Fn(&str) -> Result<String>,
    {
        if config.api_version != "scheduler/connectors/v1" {
            bail!("unsupported connector config api_version; expected scheduler/connectors/v1");
        }

        let mut connectors = HashMap::with_capacity(config.connectors.len());
        for (name, config) in config.connectors {
            validate_connector_name(&name)?;
            if config.allowed_kinds.is_empty() {
                bail!("artifact connector {name} must allow at least one artifact kind");
            }
            if config.connect_timeout_seconds == 0 || config.timeout_seconds == 0 {
                bail!("artifact connector {name} timeouts must be at least one second");
            }
            if config.connect_timeout_seconds > config.timeout_seconds {
                bail!("artifact connector {name} connect timeout cannot exceed its total timeout");
            }

            let mut base_url = Url::parse(&config.base_url)
                .with_context(|| format!("artifact connector {name} has an invalid base_url"))?;
            validate_connector_base_url(&name, &base_url, config.allow_insecure_http)?;
            let base_path = base_url.path().trim_end_matches('/');
            let endpoint_path = format!("{base_path}/v1/artifacts/fetch");
            base_url.set_path(&endpoint_path);

            let authorization = config
                .bearer_token_env
                .as_deref()
                .map(|environment_name| {
                    if environment_name.trim().is_empty() {
                        bail!("artifact connector {name} bearer_token_env cannot be empty");
                    }
                    let token = environment(environment_name)?;
                    if token.trim().is_empty() {
                        bail!(
                            "connector bearer token environment variable {environment_name} is empty"
                        );
                    }
                    let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                        .with_context(|| {
                            format!(
                                "artifact connector {name} bearer token cannot be represented as an HTTP header"
                            )
                        })?;
                    value.set_sensitive(true);
                    Ok(value)
                })
                .transpose()?;
            let client = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(config.connect_timeout_seconds))
                .timeout(Duration::from_secs(config.timeout_seconds))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .with_context(|| format!("cannot initialize artifact connector {name}"))?;

            connectors.insert(
                name,
                NamedConnector {
                    endpoint: base_url,
                    client,
                    authorization,
                    allowed_kinds: config.allowed_kinds.into_iter().collect(),
                },
            );
        }
        Ok(Self { connectors })
    }
}

#[async_trait]
impl ArtifactAdapter for ConnectorAdapter {
    async fn fetch(&self, uri: &Url, kind: ArtifactKind) -> Result<Artifact> {
        let started = std::time::Instant::now();
        let result = self.fetch_typed(uri, kind).await;
        match &result {
            Ok(artifact) => tracing::info!(
                target: "scheduler.connector",
                connector_name = connector_name(uri).unwrap_or("<invalid>"),
                artifact_kind = kind.as_str(),
                artifact_bytes = artifact.bytes.len() as u64,
                duration_ms = started.elapsed().as_millis() as u64,
                "artifact connector fetch succeeded"
            ),
            Err(error) => tracing::warn!(
                target: "scheduler.connector",
                connector_name = error.connector().unwrap_or("<invalid>"),
                artifact_kind = error.kind().unwrap_or(kind).as_str(),
                error_class = error.class(),
                upstream_status = error.upstream_status(),
                duration_ms = started.elapsed().as_millis() as u64,
                "artifact connector fetch failed"
            ),
        }
        result.map_err(Into::into)
    }
}

impl ConnectorAdapter {
    async fn fetch_typed(
        &self,
        uri: &Url,
        kind: ArtifactKind,
    ) -> std::result::Result<Artifact, ArtifactFetchError> {
        if uri.scheme() != "connector" {
            return Err(ArtifactFetchError::InvalidReference {
                reason: "connector URI must use the connector scheme",
            });
        }
        let name = connector_name(uri)?;
        let connector =
            self.connectors
                .get(name)
                .ok_or_else(|| ArtifactFetchError::NotConfigured {
                    connector: name.to_owned(),
                })?;
        if !connector.allowed_kinds.contains(&kind) {
            return Err(ArtifactFetchError::UnsupportedKind {
                connector: name.to_owned(),
                kind,
            });
        }

        let mut resource = uri.path().to_owned();
        if let Some(query) = uri.query() {
            resource.push('?');
            resource.push_str(query);
        }
        let payload = ConnectorFetchRequest {
            api_version: CONNECTOR_API_VERSION,
            kind: kind.as_str(),
            resource: &resource,
        };
        let mut request = connector
            .client
            .post(connector.endpoint.clone())
            .json(&payload);
        if let Some(authorization) = &connector.authorization {
            request = request.header(AUTHORIZATION, authorization.clone());
        }
        let mut response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                ArtifactFetchError::Timeout {
                    connector: name.to_owned(),
                    kind,
                }
            } else {
                ArtifactFetchError::Transport {
                    connector: name.to_owned(),
                    kind,
                }
            }
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(ArtifactFetchError::UpstreamStatus {
                connector: name.to_owned(),
                kind,
                status: status.as_u16(),
            });
        }
        validate_connector_response_headers(name, kind, &response)?;
        let media_type = response.headers()[CONTENT_TYPE]
            .to_str()
            .expect("validated connector content type")
            .to_owned();
        let source_version = source_version(&response);
        let bytes = match read_bounded(&mut response, kind).await {
            Ok(bytes) => bytes,
            Err(BoundedReadError::TooLarge) => {
                return Err(ArtifactFetchError::TooLarge {
                    connector: Some(name.to_owned()),
                    kind,
                });
            }
            Err(BoundedReadError::Timeout) => {
                return Err(ArtifactFetchError::Timeout {
                    connector: name.to_owned(),
                    kind,
                });
            }
            Err(BoundedReadError::BodyRead) => {
                return Err(ArtifactFetchError::InvalidResponse {
                    connector: name.to_owned(),
                    kind,
                    reason: "response body could not be read",
                });
            }
        };
        Ok(Artifact {
            bytes,
            media_type: Some(media_type),
            source_version,
        })
    }
}

const CONNECTOR_API_VERSION: &str = "scheduler.connector/v1";
const CONNECTOR_API_VERSION_HEADER: &str = "x-scheduler-connector-api-version";

fn connector_name(uri: &Url) -> std::result::Result<&str, ArtifactFetchError> {
    if !uri.username().is_empty() || uri.password().is_some() {
        return Err(ArtifactFetchError::InvalidReference {
            reason: "connector URI must not contain user information",
        });
    }
    if uri.fragment().is_some() {
        return Err(ArtifactFetchError::InvalidReference {
            reason: "connector URI must not contain a fragment",
        });
    }
    if uri.port().is_some() {
        return Err(ArtifactFetchError::InvalidReference {
            reason: "connector URI name must not include a port",
        });
    }
    let name = uri.host_str().ok_or(ArtifactFetchError::InvalidReference {
        reason: "connector URI must contain a connector name",
    })?;
    if !connector_name_is_valid(name) {
        return Err(ArtifactFetchError::InvalidReference {
            reason: "connector URI contains an invalid connector name",
        });
    }
    Ok(name)
}

fn validate_connector_response_headers(
    name: &str,
    kind: ArtifactKind,
    response: &reqwest::Response,
) -> std::result::Result<(), ArtifactFetchError> {
    let version = response
        .headers()
        .get(CONNECTOR_API_VERSION_HEADER)
        .and_then(|value| value.to_str().ok());
    if version != Some(CONNECTOR_API_VERSION) {
        return Err(ArtifactFetchError::InvalidResponse {
            connector: name.to_owned(),
            kind,
            reason: "missing or unsupported connector API version",
        });
    }
    let Some(media_type) = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(ArtifactFetchError::InvalidResponse {
            connector: name.to_owned(),
            kind,
            reason: "missing or invalid Content-Type",
        });
    };
    if !connector_media_type_is_valid(kind, media_type) {
        return Err(ArtifactFetchError::InvalidResponse {
            connector: name.to_owned(),
            kind,
            reason: "unsupported Content-Type",
        });
    }
    Ok(())
}

fn connector_media_type_is_valid(kind: ArtifactKind, media_type: &str) -> bool {
    let essence = media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let json = essence == "application/json"
        || essence
            .strip_prefix("application/")
            .is_some_and(|subtype| subtype.ends_with("+json"));
    match kind {
        ArtifactKind::Parameters => json,
        ArtifactKind::Blueprint => {
            json || matches!(
                essence.as_str(),
                "application/yaml" | "application/x-yaml" | "text/yaml" | "text/x-yaml"
            ) || essence.ends_with("+yaml")
        }
    }
}

fn validate_connector_name(name: &str) -> Result<()> {
    if !connector_name_is_valid(name) {
        bail!(
            "artifact connector names must contain only lowercase ASCII letters, digits, and internal hyphens"
        );
    }
    Ok(())
}

fn connector_name_is_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
}

fn validate_connector_base_url(name: &str, url: &Url, allow_insecure_http: bool) -> Result<()> {
    if !url.username().is_empty() || url.password().is_some() {
        bail!("artifact connector {name} base_url must not contain user information");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("artifact connector {name} base_url must not contain a query or fragment");
    }
    match url.scheme() {
        "https" => {}
        "http" if allow_insecure_http || is_loopback(url) => {}
        "http" => {
            bail!(
                "artifact connector {name} must use HTTPS unless insecure HTTP is explicitly enabled"
            )
        }
        _ => bail!("artifact connector {name} base_url must use HTTP or HTTPS"),
    }
    if url.host().is_none() {
        bail!("artifact connector {name} base_url must contain a host");
    }
    Ok(())
}

fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

struct FileAdapter {
    roots: Vec<PathBuf>,
}

impl FileAdapter {
    fn new(roots: Vec<PathBuf>) -> Result<Self> {
        let roots = roots
            .into_iter()
            .map(|root| {
                root.canonicalize()
                    .with_context(|| format!("invalid artifact root {}", root.display()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { roots })
    }
}

#[async_trait]
impl ArtifactAdapter for FileAdapter {
    async fn fetch(&self, uri: &Url, kind: ArtifactKind) -> Result<Artifact> {
        let path = uri
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("file artifact URI is not a local path"))?;
        let canonical = tokio::fs::canonicalize(&path)
            .await
            .with_context(|| format!("cannot resolve artifact {}", path.display()))?;
        if !self.roots.iter().any(|root| canonical.starts_with(root)) {
            bail!("artifact path is outside configured file roots");
        }
        let metadata = tokio::fs::metadata(&canonical).await?;
        if metadata.len() > max_size(kind) {
            return Err(ArtifactFetchError::TooLarge {
                connector: None,
                kind,
            }
            .into());
        }
        let bytes = tokio::fs::read(&canonical).await?;
        let modified = metadata.modified().ok().and_then(|time| {
            time.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|duration| format!("{}:{}", duration.as_nanos(), metadata.len()))
        });
        Ok(Artifact {
            bytes,
            media_type: canonical
                .extension()
                .and_then(|value| value.to_str())
                .map(extension_media_type)
                .map(str::to_owned),
            source_version: modified,
        })
    }
}

struct HttpAdapter {
    client: reqwest::Client,
}

impl HttpAdapter {
    fn new(headers: HashMap<String, String>) -> Result<Self> {
        let mut default_headers = reqwest::header::HeaderMap::new();
        for (name, value) in headers {
            default_headers.insert(
                reqwest::header::HeaderName::try_from(name)?,
                reqwest::header::HeaderValue::try_from(value)?,
            );
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .default_headers(default_headers)
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::limited(3))
                .build()?,
        })
    }
}

#[async_trait]
impl ArtifactAdapter for HttpAdapter {
    async fn fetch(&self, uri: &Url, kind: ArtifactKind) -> Result<Artifact> {
        let mut response = self
            .client
            .get(uri.clone())
            .send()
            .await
            .context("artifact HTTP transport failed")?;
        let status = response.status();
        if !status.is_success() {
            bail!(
                "artifact HTTP request failed with status {} ({})",
                status.as_u16(),
                status.canonical_reason().unwrap_or("unknown status")
            );
        }
        let media_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let source_version = source_version(&response);
        let bytes = match read_bounded(&mut response, kind).await {
            Ok(bytes) => bytes,
            Err(BoundedReadError::TooLarge) => {
                return Err(ArtifactFetchError::TooLarge {
                    connector: None,
                    kind,
                }
                .into());
            }
            Err(BoundedReadError::Timeout) => bail!("artifact response timed out"),
            Err(BoundedReadError::BodyRead) => bail!("artifact response body could not be read"),
        };
        Ok(Artifact {
            bytes,
            media_type,
            source_version,
        })
    }
}

fn source_version(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get(ETAG)
        .or_else(|| response.headers().get(LAST_MODIFIED))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedReadError {
    TooLarge,
    Timeout,
    BodyRead,
}

async fn read_bounded(
    response: &mut reqwest::Response,
    kind: ArtifactKind,
) -> std::result::Result<Vec<u8>, BoundedReadError> {
    let limit = max_size(kind);
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(BoundedReadError::TooLarge);
    }
    let mut bytes =
        Vec::with_capacity(response.content_length().unwrap_or_default().min(limit) as usize);
    loop {
        let chunk = response.chunk().await.map_err(|error| {
            if error.is_timeout() {
                BoundedReadError::Timeout
            } else {
                BoundedReadError::BodyRead
            }
        })?;
        let Some(chunk) = chunk else { break };
        if bytes.len().saturating_add(chunk.len()) as u64 > limit {
            return Err(BoundedReadError::TooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn max_size(kind: ArtifactKind) -> u64 {
    match kind {
        ArtifactKind::Blueprint => 1_048_576,
        ArtifactKind::Parameters => 4_194_304,
    }
}

fn extension_media_type(extension: &str) -> &'static str {
    match extension.to_ascii_lowercase().as_str() {
        "yaml" | "yml" => "application/yaml",
        _ => "application/json",
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    fn connector_config(
        base_url: impl Into<String>,
        allowed_kinds: Vec<ArtifactKind>,
    ) -> ConnectorConfig {
        ConnectorConfig {
            api_version: "scheduler/connectors/v1".into(),
            connectors: HashMap::from([(
                "records".into(),
                ConnectorEndpointConfig {
                    base_url: base_url.into(),
                    bearer_token_env: Some("RECORDS_CONNECTOR_TOKEN".into()),
                    allowed_kinds,
                    connect_timeout_seconds: 2,
                    timeout_seconds: 5,
                    allow_insecure_http: false,
                },
            )]),
        }
    }

    fn registry_with_connector(config: ConnectorConfig, token: &str) -> AdapterRegistry {
        let adapter = ConnectorAdapter::new_with_env(config, |name| {
            assert_eq!(name, "RECORDS_CONNECTOR_TOKEN");
            Ok(token.to_owned())
        })
        .expect("connector adapter");
        let mut registry =
            AdapterRegistry::with_defaults(Vec::new(), HashMap::new()).expect("registry");
        registry.register("connector", Arc::new(adapter));
        registry
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let header_end = loop {
            if let Some(position) = request.windows(4).position(|value| value == b"\r\n\r\n") {
                break position + 4;
            }
            let mut buffer = [0_u8; 1024];
            let read = stream.read(&mut buffer).await.expect("request");
            assert_ne!(read, 0, "connection closed before request headers");
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
            assert_ne!(read, 0, "connection closed before request body");
            request.extend_from_slice(&buffer[..read]);
        }
        request
    }

    async fn fetch_fixture_response(
        kind: ArtifactKind,
        response: impl Into<Vec<u8>>,
    ) -> Result<Artifact> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let response = response.into();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let _request = read_http_request(&mut stream).await;
            stream.write_all(&response).await.expect("response");
        });
        let config = connector_config(format!("http://{address}"), vec![kind]);
        let registry = registry_with_connector(config, "fixture-token");
        let result = registry.fetch("connector://records/fixture", kind).await;
        server.await.expect("server");
        result
    }

    #[tokio::test]
    async fn file_adapter_allows_files_under_an_explicit_root() {
        let root = tempfile::tempdir().expect("root");
        let path = root.path().join("parameters.json");
        std::fs::write(&path, b"{}").expect("fixture");
        let registry = AdapterRegistry::with_defaults(vec![root.path().into()], HashMap::new())
            .expect("registry");

        let artifact = registry
            .fetch(
                url::Url::from_file_path(&path).expect("URL").as_str(),
                ArtifactKind::Parameters,
            )
            .await
            .expect("artifact");
        assert_eq!(artifact.bytes, b"{}");
        assert_eq!(artifact.media_type.as_deref(), Some("application/json"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_adapter_rejects_a_symlink_that_escapes_the_root() {
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::NamedTempFile::new().expect("outside");
        let link = root.path().join("escape.json");
        std::os::unix::fs::symlink(outside.path(), &link).expect("symlink");
        let registry = AdapterRegistry::with_defaults(vec![root.path().into()], HashMap::new())
            .expect("registry");

        let error = registry
            .fetch(
                url::Url::from_file_path(&link).expect("URL").as_str(),
                ArtifactKind::Parameters,
            )
            .await
            .expect_err("escape must fail");
        assert!(error.to_string().contains("outside configured file roots"));
    }

    #[tokio::test]
    async fn http_adapter_sends_configured_authentication_headers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let mut request = vec![0; 4096];
            let read = stream.read(&mut request).await.expect("request");
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.contains("authorization: Bearer artifact-secret\r\n"));
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nETag: fixture-v1\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                )
                .await
                .expect("response");
        });
        let mut headers = HashMap::new();
        headers.insert("authorization".into(), "Bearer artifact-secret".into());
        let registry = AdapterRegistry::with_defaults(Vec::new(), headers).expect("registry");

        let artifact = registry
            .fetch(
                &format!("http://{address}/parameters.json"),
                ArtifactKind::Parameters,
            )
            .await
            .expect("artifact");
        assert_eq!(artifact.bytes, b"{}");
        assert_eq!(artifact.source_version.as_deref(), Some("fixture-v1"));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn http_adapter_reports_upstream_status_code() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let mut request = vec![0; 4096];
            let _read = stream.read(&mut request).await.expect("request");
            stream
                .write_all(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("response");
        });
        let registry =
            AdapterRegistry::with_defaults(Vec::new(), HashMap::new()).expect("registry");

        let error = registry
            .fetch(
                &format!("http://{address}/blueprint.yaml"),
                ArtifactKind::Blueprint,
            )
            .await
            .expect_err("upstream failure");
        assert!(error.to_string().contains("status 503"));
        server.await.expect("server");
    }

    #[test]
    fn connector_config_accepts_yaml_and_json_with_parameter_defaults() {
        let yaml = br#"
api_version: scheduler/connectors/v1
connectors:
  records:
    base_url: https://records.example.test/api
"#;
        let parsed = ConnectorConfig::from_slice(yaml).expect("YAML config");
        assert_eq!(
            parsed.connectors["records"].allowed_kinds,
            vec![ArtifactKind::Parameters]
        );

        let json = br#"{
            "api_version":"scheduler/connectors/v1",
            "connectors":{}
        }"#;
        assert!(ConnectorConfig::from_slice(json).is_ok());
    }

    #[test]
    fn connector_config_requires_secure_remote_endpoints() {
        let insecure = connector_config(
            "http://records.example.test",
            vec![ArtifactKind::Parameters],
        );
        let error = ConnectorAdapter::new_with_env(insecure, |_| Ok("token".into()))
            .err()
            .expect("remote plaintext HTTP must fail");
        assert!(error.to_string().contains("must use HTTPS"));

        let loopback = connector_config(
            "http://127.0.0.1:12345/prefix",
            vec![ArtifactKind::Parameters],
        );
        ConnectorAdapter::new_with_env(loopback, |_| Ok("token".into()))
            .expect("loopback HTTP is suitable for a local sidecar");
    }

    #[test]
    fn connector_config_rejects_an_empty_bearer_token_value() {
        let config = connector_config(
            "https://records.example.test",
            vec![ArtifactKind::Parameters],
        );
        let error = ConnectorAdapter::new_with_env(config, |_| Ok(" \t ".into()))
            .err()
            .expect("an empty bearer token must fail startup");
        assert!(error.to_string().contains("environment variable"));
        assert!(error.to_string().contains("is empty"));
    }

    #[tokio::test]
    async fn connector_registration_validates_the_whole_config_atomically() {
        let mut registry =
            AdapterRegistry::with_defaults(Vec::new(), HashMap::new()).expect("registry");
        let mut config = connector_config("http://127.0.0.1:12345", vec![ArtifactKind::Parameters]);
        config
            .connectors
            .get_mut("records")
            .expect("records config")
            .bearer_token_env = None;
        config.connectors.insert(
            "invalid".into(),
            ConnectorEndpointConfig {
                base_url: "http://remote.example.test".into(),
                bearer_token_env: None,
                allowed_kinds: vec![ArtifactKind::Parameters],
                connect_timeout_seconds: 5,
                timeout_seconds: 20,
                allow_insecure_http: false,
            },
        );

        registry
            .register_connectors(config)
            .expect_err("one invalid connector must reject the complete document");
        let error = registry
            .fetch("connector://records/item", ArtifactKind::Parameters)
            .await
            .expect_err("a failed registration must not install partial state");
        assert!(
            error
                .to_string()
                .contains("artifact connector records is not configured")
        );
    }

    #[tokio::test]
    async fn named_connector_posts_authenticated_versioned_artifacts() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let request = read_http_request(&mut stream).await;
            let header_end = request
                .windows(4)
                .position(|value| value == b"\r\n\r\n")
                .expect("headers")
                + 4;
            let headers = String::from_utf8_lossy(&request[..header_end]);
            assert!(headers.starts_with("POST /prefix/v1/artifacts/fetch HTTP/1.1\r\n"));
            assert!(headers.contains("authorization: Bearer connector-secret\r\n"));
            let payload: serde_json::Value =
                serde_json::from_slice(&request[header_end..]).expect("JSON request");
            assert_eq!(payload["api_version"], "scheduler.connector/v1");
            assert_eq!(payload["kind"], "parameters");
            assert_eq!(payload["resource"], "/customer%2F42?revision=7");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v1\r\nETag: source-v7\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("response");
        });
        let config = connector_config(
            format!("http://{address}/prefix/"),
            vec![ArtifactKind::Parameters],
        );
        let registry = registry_with_connector(config, "connector-secret");

        let artifact = registry
            .fetch(
                "connector://records/customer%2F42?revision=7",
                ArtifactKind::Parameters,
            )
            .await
            .expect("artifact");
        assert_eq!(artifact.bytes, br#"{"ok":true}"#);
        assert_eq!(artifact.media_type.as_deref(), Some("application/json"));
        assert_eq!(artifact.source_version.as_deref(), Some("source-v7"));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn connector_requires_response_protocol_version_and_content_type() {
        let cases = [
            (
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                    .as_slice(),
                "missing or unsupported connector API version",
            ),
            (
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v2\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                    .as_slice(),
                "missing or unsupported connector API version",
            ),
            (
                b"HTTP/1.1 200 OK\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v1\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                    .as_slice(),
                "missing or invalid Content-Type",
            ),
            (
                b"HTTP/1.1 200 OK\r\nContent-Type: application/yaml\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v1\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                    .as_slice(),
                "unsupported Content-Type",
            ),
        ];

        for (response, expected_reason) in cases {
            let error = fetch_fixture_response(ArtifactKind::Parameters, response)
                .await
                .expect_err("invalid connector headers must fail");
            let typed = error
                .downcast_ref::<ArtifactFetchError>()
                .expect("typed connector failure");
            assert_eq!(typed.class(), "invalid_response");
            assert!(error.to_string().contains(expected_reason));
        }
    }

    #[tokio::test]
    async fn connector_accepts_kind_appropriate_vendor_and_yaml_media_types() {
        let parameters = fetch_fixture_response(
            ArtifactKind::Parameters,
            b"HTTP/1.1 200 OK\r\nContent-Type: application/vnd.scheduler+json; charset=utf-8\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v1\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                .as_slice(),
        )
        .await
        .expect("vendor JSON parameters");
        assert_eq!(parameters.bytes, b"{}");

        let blueprint = fetch_fixture_response(
            ArtifactKind::Blueprint,
            b"HTTP/1.1 200 OK\r\nContent-Type: application/yaml; charset=utf-8\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v1\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"
                .as_slice(),
        )
        .await
        .expect("YAML blueprint");
        assert_eq!(blueprint.bytes, b"{}");
    }

    #[tokio::test]
    async fn connector_rejects_unknown_kinds_and_unsafe_references_without_fallback() {
        let config = connector_config(
            "https://records.example.test",
            vec![ArtifactKind::Parameters],
        );
        let registry = registry_with_connector(config, "token");

        let error = registry
            .fetch("connector://records/item", ArtifactKind::Blueprint)
            .await
            .expect_err("kind must be allowed explicitly");
        assert!(
            error
                .to_string()
                .contains("does not provide blueprint artifacts")
        );

        let error = registry
            .fetch("connector://missing/item", ArtifactKind::Parameters)
            .await
            .expect_err("unknown connector must not fall back");
        assert!(error.to_string().contains("missing is not configured"));
        assert!(matches!(
            error.downcast_ref::<ArtifactFetchError>(),
            Some(ArtifactFetchError::NotConfigured { connector }) if connector == "missing"
        ));

        let error = registry
            .fetch(
                "connector://user:password@records/item",
                ArtifactKind::Parameters,
            )
            .await
            .expect_err("userinfo must be rejected");
        assert!(
            error
                .to_string()
                .contains("must not contain user information")
        );

        let error = registry
            .fetch("connector://records/item#secret", ArtifactKind::Parameters)
            .await
            .expect_err("fragments must be rejected");
        assert!(error.to_string().contains("must not contain a fragment"));
    }

    #[tokio::test]
    async fn connector_errors_do_not_expose_response_bodies_or_bearer_tokens() {
        const SECRET: &str = "secret-that-must-not-leak";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let _request = read_http_request(&mut stream).await;
            let body = format!("provider exception containing {SECRET}");
            let response = format!(
                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("response");
        });
        let config = connector_config(format!("http://{address}"), vec![ArtifactKind::Parameters]);
        let registry = registry_with_connector(config, SECRET);

        let error = registry
            .fetch("connector://records/item", ArtifactKind::Parameters)
            .await
            .expect_err("provider failure");
        let diagnostic = format!("{error:#}");
        assert!(diagnostic.contains("status 500"));
        assert!(!diagnostic.contains(SECRET));
        assert!(!diagnostic.contains("provider exception"));
        assert!(matches!(
            error.downcast_ref::<ArtifactFetchError>(),
            Some(ArtifactFetchError::UpstreamStatus {
                connector,
                kind: ArtifactKind::Parameters,
                status: 500,
            }) if connector == "records"
        ));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn connector_total_timeout_is_enforced_and_reported_safely() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let _request = read_http_request(&mut stream).await;
            std::future::pending::<()>().await;
        });
        let mut config =
            connector_config(format!("http://{address}"), vec![ArtifactKind::Parameters]);
        let endpoint = config
            .connectors
            .get_mut("records")
            .expect("records config");
        endpoint.connect_timeout_seconds = 1;
        endpoint.timeout_seconds = 1;
        let registry = registry_with_connector(config, "timeout-secret");

        let error = registry
            .fetch("connector://records/slow", ArtifactKind::Parameters)
            .await
            .expect_err("connector must time out");
        let diagnostic = format!("{error:#}");
        assert!(diagnostic.contains("timed out while fetching"));
        assert!(!diagnostic.contains("timeout-secret"));
        assert_eq!(
            error
                .downcast_ref::<ArtifactFetchError>()
                .expect("typed timeout")
                .class(),
            "timeout"
        );
        server.abort();
    }

    #[tokio::test]
    async fn connector_timeout_while_reading_the_body_remains_a_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let _request = read_http_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v1\r\nContent-Length: 2\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("response headers");
            std::future::pending::<()>().await;
        });
        let mut config =
            connector_config(format!("http://{address}"), vec![ArtifactKind::Parameters]);
        let endpoint = config
            .connectors
            .get_mut("records")
            .expect("records config");
        endpoint.connect_timeout_seconds = 1;
        endpoint.timeout_seconds = 1;
        let registry = registry_with_connector(config, "timeout-secret");

        let error = registry
            .fetch("connector://records/stalled-body", ArtifactKind::Parameters)
            .await
            .expect_err("stalled response body must time out");
        assert!(matches!(
            error.downcast_ref::<ArtifactFetchError>(),
            Some(ArtifactFetchError::Timeout {
                connector,
                kind: ArtifactKind::Parameters,
            }) if connector == "records"
        ));
        server.abort();
    }

    #[tokio::test]
    async fn connector_does_not_follow_redirects() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let _request = read_http_request(&mut stream).await;
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{address}/redirect-target\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("redirect response");
            drop(stream);
            assert!(
                tokio::time::timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "connector followed an upstream redirect"
            );
        });
        let config = connector_config(format!("http://{address}"), vec![ArtifactKind::Parameters]);
        let registry = registry_with_connector(config, "token");

        let error = registry
            .fetch("connector://records/item", ArtifactKind::Parameters)
            .await
            .expect_err("redirect is not an artifact response");
        assert!(error.to_string().contains("status 302"));
        server.await.expect("server");
    }

    #[tokio::test]
    async fn connector_rejects_oversized_content_length_before_reading_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let _request = read_http_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v1\r\nContent-Length: 4194305\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("response headers");
        });
        let config = connector_config(format!("http://{address}"), vec![ArtifactKind::Parameters]);
        let registry = registry_with_connector(config, "token");

        let error = registry
            .fetch("connector://records/large", ArtifactKind::Parameters)
            .await
            .expect_err("oversized declared artifact");
        assert!(format!("{error:#}").contains("artifact exceeds size limit"));
        assert_eq!(
            error
                .downcast_ref::<ArtifactFetchError>()
                .expect("typed size failure")
                .class(),
            "too_large"
        );
        server.await.expect("server");
    }

    #[tokio::test]
    async fn connector_enforces_size_limit_for_chunked_responses() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("connection");
            let _request = read_http_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Scheduler-Connector-Api-Version: scheduler.connector/v1\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("response headers");
            let chunk = vec![b'x'; 65_536];
            for _ in 0..65 {
                if stream.write_all(b"10000\r\n").await.is_err()
                    || stream.write_all(&chunk).await.is_err()
                    || stream.write_all(b"\r\n").await.is_err()
                {
                    return;
                }
            }
            let _ = stream.write_all(b"0\r\n\r\n").await;
        });
        let config = connector_config(format!("http://{address}"), vec![ArtifactKind::Parameters]);
        let registry = registry_with_connector(config, "token");

        let error = registry
            .fetch("connector://records/large", ArtifactKind::Parameters)
            .await
            .expect_err("oversized chunked artifact");
        assert!(format!("{error:#}").contains("artifact exceeds size limit"));
        server.await.expect("server");
    }
}
