use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    logs::{
        BatchConfigBuilder as LogBatchConfigBuilder, BatchLogProcessor, LogBatch, LogExporter,
        SdkLoggerProvider,
    },
    metrics::{
        PeriodicReader, SdkMeterProvider, Temporality, data::ResourceMetrics,
        exporter::PushMetricExporter,
    },
    trace::{
        BatchConfigBuilder as SpanBatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider,
        SpanData, SpanExporter,
    },
};
use reqwest_otel::{Certificate as HttpCertificate, Identity as HttpIdentity};
use serde::Serialize;
use tonic::{
    metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue},
    transport::{Certificate, ClientTlsConfig, Identity},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);
const SPAN_QUEUE_CAPACITY: usize = 2_048;
const LOG_QUEUE_CAPACITY: usize = 2_048;
const MAX_EXPORT_BATCH: usize = 512;
const MAX_SECRET_FILE_BYTES: u64 = 64 * 1024;

static GLOBAL_STATUS: OnceLock<Arc<TelemetryStatus>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    Grpc,
    HttpProtobuf,
}

impl OtlpProtocol {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "grpc" => Ok(Self::Grpc),
            "http/protobuf" => Ok(Self::HttpProtobuf),
            _ => bail!("OTLP protocol must be grpc or http/protobuf"),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::HttpProtobuf => "http/protobuf",
        }
    }
}

#[derive(Clone)]
struct SignalConfig {
    protocol: OtlpProtocol,
    endpoint: String,
    headers: HashMap<String, String>,
}

#[derive(Clone, Default)]
struct TlsMaterial {
    ca: Option<Vec<u8>>,
    client_certificate: Option<Vec<u8>>,
    client_key: Option<Vec<u8>>,
}

/// Fully resolved bootstrap-only telemetry configuration.
///
/// Its custom `Debug` implementation deliberately exposes neither endpoints,
/// header values, credentials, certificates, keys, nor source file paths.
#[derive(Clone)]
pub struct TelemetryConfig {
    service_name: &'static str,
    traces: Option<SignalConfig>,
    metrics: Option<SignalConfig>,
    logs: Option<SignalConfig>,
    tls: TlsMaterial,
}

impl fmt::Debug for TelemetryConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let protocols = self
            .signals()
            .map(|signal| signal.protocol.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let custom_headers = self.signals().any(|signal| !signal.headers.is_empty());
        formatter
            .debug_struct("TelemetryConfig")
            .field("service_name", &self.service_name)
            .field("configured", &self.is_configured())
            .field("protocols", &protocols)
            .field("custom_headers", &custom_headers)
            .field("custom_ca", &self.tls.ca.is_some())
            .field(
                "client_identity",
                &(self.tls.client_certificate.is_some() && self.tls.client_key.is_some()),
            )
            .finish()
    }
}

impl TelemetryConfig {
    pub fn from_environment(
        service_name: &'static str,
        endpoint_override: Option<&str>,
    ) -> Result<Self> {
        Self::from_lookup(service_name, endpoint_override, |name| {
            std::env::var(name).ok()
        })
    }

    fn from_lookup<F>(
        service_name: &'static str,
        endpoint_override: Option<&str>,
        lookup: F,
    ) -> Result<Self>
    where
        F: Fn(&str) -> Option<String>,
    {
        let global_endpoint = endpoint_override
            .map(str::to_owned)
            .or_else(|| lookup("OTEL_EXPORTER_OTLP_ENDPOINT"));
        let trace_endpoint =
            lookup("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").or_else(|| global_endpoint.clone());
        let metric_endpoint =
            lookup("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").or_else(|| global_endpoint.clone());
        let log_endpoint =
            lookup("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT").or_else(|| global_endpoint.clone());
        if trace_endpoint.is_none() && metric_endpoint.is_none() && log_endpoint.is_none() {
            return Ok(Self {
                service_name,
                traces: None,
                metrics: None,
                logs: None,
                tls: TlsMaterial::default(),
            });
        }

        let default_protocol = lookup("OTEL_EXPORTER_OTLP_PROTOCOL")
            .as_deref()
            .map(OtlpProtocol::parse)
            .transpose()?
            .unwrap_or(OtlpProtocol::Grpc);
        let file_headers = lookup("SCHEDULER_OTLP_HEADERS_FILE")
            .map(PathBuf::from)
            .map(|path| read_secret_file(&path))
            .transpose()?
            .map(|bytes| {
                let document =
                    String::from_utf8(bytes).context("OTLP header file must contain UTF-8 text")?;
                parse_headers(&document)
            })
            .transpose()?
            .unwrap_or_default();
        let mut global_headers = file_headers;
        if let Some(headers) = lookup("OTEL_EXPORTER_OTLP_HEADERS") {
            global_headers.extend(parse_headers(&headers)?);
        }
        if let Some(path) = lookup("SCHEDULER_OTLP_CREDENTIAL_FILE")
            .or_else(|| lookup("SCHEDULER_OTLP_BEARER_TOKEN_FILE"))
        {
            let token = String::from_utf8(read_secret_file(Path::new(&path))?)
                .context("OTLP credential file must contain UTF-8 text")?;
            let token = token.trim();
            if token.is_empty() || token.contains(['\r', '\n', '\0']) {
                bail!("OTLP credential file is empty or malformed");
            }
            global_headers.insert("authorization".into(), format!("Bearer {token}"));
        }

        let signal = |name: &str,
                      endpoint: Option<String>,
                      protocol_env: &str,
                      headers_env: &str,
                      path: &str|
         -> Result<Option<SignalConfig>> {
            let Some(endpoint) = endpoint else {
                return Ok(None);
            };
            let protocol = lookup(protocol_env)
                .as_deref()
                .map(OtlpProtocol::parse)
                .transpose()?
                .unwrap_or(default_protocol);
            let endpoint_is_signal_specific = lookup(&format!(
                "OTEL_EXPORTER_OTLP_{}_ENDPOINT",
                name.to_ascii_uppercase()
            ))
            .is_some();
            let endpoint = normalize_endpoint(
                &endpoint,
                protocol,
                (!endpoint_is_signal_specific).then_some(path),
            )?;
            let mut headers = global_headers.clone();
            if let Some(signal_headers) = lookup(headers_env) {
                headers.extend(parse_headers(&signal_headers)?);
            }
            validate_headers(&headers)?;
            Ok(Some(SignalConfig {
                protocol,
                endpoint,
                headers,
            }))
        };

        let tls = load_tls_material(&lookup)?;
        Ok(Self {
            service_name,
            traces: signal(
                "traces",
                trace_endpoint,
                "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
                "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
                "/v1/traces",
            )?,
            metrics: signal(
                "metrics",
                metric_endpoint,
                "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
                "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
                "/v1/metrics",
            )?,
            logs: signal(
                "logs",
                log_endpoint,
                "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL",
                "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
                "/v1/logs",
            )?,
            tls,
        })
    }

    pub fn is_configured(&self) -> bool {
        self.traces.is_some() || self.metrics.is_some() || self.logs.is_some()
    }

    fn signals(&self) -> impl Iterator<Item = &SignalConfig> {
        self.traces
            .iter()
            .chain(self.metrics.iter())
            .chain(self.logs.iter())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetryStatusSnapshot {
    pub configured: bool,
    pub protocol: String,
    pub last_success_unix_ms: Option<u64>,
    pub last_error_class: Option<String>,
    pub failed_signals: Vec<String>,
    pub dropped_telemetry: u64,
    pub export_batches_in_flight: u64,
    pub span_queue_capacity: usize,
    pub log_queue_capacity: usize,
}

struct TelemetryStatus {
    configured: bool,
    protocol: String,
    last_success_unix_ms: AtomicU64,
    failed_signals: [AtomicBool; 3],
    dropped_telemetry: AtomicU64,
    export_batches_in_flight: AtomicU64,
}

#[derive(Clone, Copy)]
enum TelemetrySignal {
    Traces = 0,
    Metrics = 1,
    Logs = 2,
}

impl TelemetryStatus {
    fn new(config: &TelemetryConfig) -> Self {
        let protocols = config
            .signals()
            .map(|signal| signal.protocol.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        Self {
            configured: config.is_configured(),
            protocol: if protocols.len() == 1 {
                protocols.into_iter().next().unwrap_or("disabled").into()
            } else if protocols.is_empty() {
                "disabled".into()
            } else {
                "mixed".into()
            },
            last_success_unix_ms: AtomicU64::new(0),
            failed_signals: std::array::from_fn(|_| AtomicBool::new(false)),
            dropped_telemetry: AtomicU64::new(0),
            export_batches_in_flight: AtomicU64::new(0),
        }
    }

    fn begin_export(&self) {
        self.export_batches_in_flight
            .fetch_add(1, Ordering::Relaxed);
    }

    fn finish_export(&self, signal: TelemetrySignal, result: &OTelSdkResult, items: u64) {
        self.export_batches_in_flight
            .fetch_sub(1, Ordering::Relaxed);
        if result.is_ok() {
            let unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            self.last_success_unix_ms
                .store(unix_ms.max(1), Ordering::Relaxed);
            self.failed_signals[signal as usize].store(false, Ordering::Relaxed);
        } else {
            self.dropped_telemetry
                .fetch_add(items.max(1), Ordering::Relaxed);
            self.failed_signals[signal as usize].store(true, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> TelemetryStatusSnapshot {
        let last_success = self.last_success_unix_ms.load(Ordering::Relaxed);
        let failed_signals = ["traces", "metrics", "logs"]
            .into_iter()
            .zip(&self.failed_signals)
            .filter(|(_, failed)| failed.load(Ordering::Relaxed))
            .map(|(signal, _)| signal.to_owned())
            .collect::<Vec<_>>();
        TelemetryStatusSnapshot {
            configured: self.configured,
            protocol: self.protocol.clone(),
            last_success_unix_ms: (last_success != 0).then_some(last_success),
            last_error_class: (!failed_signals.is_empty()).then(|| "export_failed".to_owned()),
            failed_signals,
            dropped_telemetry: self.dropped_telemetry.load(Ordering::Relaxed),
            export_batches_in_flight: self.export_batches_in_flight.load(Ordering::Relaxed),
            span_queue_capacity: SPAN_QUEUE_CAPACITY,
            log_queue_capacity: LOG_QUEUE_CAPACITY,
        }
    }
}

pub fn status() -> TelemetryStatusSnapshot {
    GLOBAL_STATUS
        .get()
        .map_or_else(disabled_status, |status| status.snapshot())
}

fn disabled_status() -> TelemetryStatusSnapshot {
    TelemetryStatusSnapshot {
        configured: false,
        protocol: "disabled".into(),
        last_success_unix_ms: None,
        last_error_class: None,
        failed_signals: Vec::new(),
        dropped_telemetry: 0,
        export_batches_in_flight: 0,
        span_queue_capacity: SPAN_QUEUE_CAPACITY,
        log_queue_capacity: LOG_QUEUE_CAPACITY,
    }
}

pub struct TelemetryGuard {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
    logger: Option<SdkLoggerProvider>,
    status: Arc<TelemetryStatus>,
}

impl TelemetryGuard {
    pub fn status(&self) -> TelemetryStatusSnapshot {
        self.status.snapshot()
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.logger.take() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = self.meter.take() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = self.tracer.take() {
            let _ = provider.shutdown();
        }
    }
}

pub fn init(service_name: &'static str, endpoint: Option<&str>) -> Result<TelemetryGuard> {
    let config = TelemetryConfig::from_environment(service_name, endpoint)?;
    init_with_config(config)
}

pub fn init_with_config(config: TelemetryConfig) -> Result<TelemetryGuard> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let format = tracing_subscriber::fmt::layer().json();
    let status = Arc::new(TelemetryStatus::new(&config));
    let (tracer_provider, meter_provider, logger_provider) = build_providers(&config, &status)?;
    let tracer = tracer_provider
        .as_ref()
        .map(|provider| provider.tracer(config.service_name));
    let trace_layer = tracer.map(|tracer| tracing_opentelemetry::layer().with_tracer(tracer));
    let log_layer = logger_provider
        .as_ref()
        .map(OpenTelemetryTracingBridge::new);

    tracing_subscriber::registry()
        .with(filter)
        .with(format)
        .with(trace_layer)
        .with(log_layer)
        .try_init()
        .context("cannot initialize structured telemetry subscriber")?;

    if let Some(provider) = &tracer_provider {
        global::set_tracer_provider(provider.clone());
    }
    if let Some(provider) = &meter_provider {
        global::set_meter_provider(provider.clone());
    }
    let _ = GLOBAL_STATUS.set(status.clone());
    Ok(TelemetryGuard {
        tracer: tracer_provider,
        meter: meter_provider,
        logger: logger_provider,
        status,
    })
}

fn build_providers(
    config: &TelemetryConfig,
    status: &Arc<TelemetryStatus>,
) -> Result<(
    Option<SdkTracerProvider>,
    Option<SdkMeterProvider>,
    Option<SdkLoggerProvider>,
)> {
    let resource = Resource::builder()
        .with_service_name(config.service_name)
        .build();
    let tracer = config
        .traces
        .as_ref()
        .map(|signal| -> Result<_> {
            let exporter = ObservedSpanExporter {
                inner: build_span_exporter(signal, &config.tls)?,
                status: status.clone(),
            };
            let processor = BatchSpanProcessor::builder(exporter)
                .with_batch_config(
                    SpanBatchConfigBuilder::default()
                        .with_max_queue_size(SPAN_QUEUE_CAPACITY)
                        .with_max_export_batch_size(MAX_EXPORT_BATCH)
                        .build(),
                )
                .build();
            Ok(SdkTracerProvider::builder()
                .with_resource(resource.clone())
                .with_span_processor(processor)
                .build())
        })
        .transpose()?;
    let meter = config
        .metrics
        .as_ref()
        .map(|signal| -> Result<_> {
            let exporter = ObservedMetricExporter {
                inner: build_metric_exporter(signal, &config.tls)?,
                status: status.clone(),
            };
            let reader = PeriodicReader::builder(exporter)
                .with_interval(Duration::from_secs(30))
                .build();
            Ok(SdkMeterProvider::builder()
                .with_resource(resource.clone())
                .with_reader(reader)
                .build())
        })
        .transpose()?;
    let logger = config
        .logs
        .as_ref()
        .map(|signal| -> Result<_> {
            let exporter = ObservedLogExporter {
                inner: build_log_exporter(signal, &config.tls)?,
                status: status.clone(),
            };
            let processor = BatchLogProcessor::builder(exporter)
                .with_batch_config(
                    LogBatchConfigBuilder::default()
                        .with_max_queue_size(LOG_QUEUE_CAPACITY)
                        .with_max_export_batch_size(MAX_EXPORT_BATCH)
                        .build(),
                )
                .build();
            Ok(SdkLoggerProvider::builder()
                .with_resource(resource.clone())
                .with_log_processor(processor)
                .build())
        })
        .transpose()?;
    Ok((tracer, meter, logger))
}

fn build_span_exporter(
    signal: &SignalConfig,
    tls: &TlsMaterial,
) -> Result<opentelemetry_otlp::SpanExporter> {
    match signal.protocol {
        OtlpProtocol::Grpc => {
            let builder = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(&signal.endpoint)
                .with_timeout(EXPORT_TIMEOUT)
                .with_metadata(grpc_metadata(&signal.headers)?);
            Ok(with_grpc_tls(builder, tls)?.build()?)
        }
        OtlpProtocol::HttpProtobuf => Ok(opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(&signal.endpoint)
            .with_protocol(Protocol::HttpBinary)
            .with_timeout(EXPORT_TIMEOUT)
            .with_headers(signal.headers.clone())
            .with_http_client(http_client(tls)?)
            .build()?),
    }
}

fn build_metric_exporter(
    signal: &SignalConfig,
    tls: &TlsMaterial,
) -> Result<opentelemetry_otlp::MetricExporter> {
    match signal.protocol {
        OtlpProtocol::Grpc => {
            let builder = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(&signal.endpoint)
                .with_timeout(EXPORT_TIMEOUT)
                .with_metadata(grpc_metadata(&signal.headers)?);
            Ok(with_grpc_tls(builder, tls)?.build()?)
        }
        OtlpProtocol::HttpProtobuf => Ok(opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_endpoint(&signal.endpoint)
            .with_protocol(Protocol::HttpBinary)
            .with_timeout(EXPORT_TIMEOUT)
            .with_headers(signal.headers.clone())
            .with_http_client(http_client(tls)?)
            .build()?),
    }
}

fn build_log_exporter(
    signal: &SignalConfig,
    tls: &TlsMaterial,
) -> Result<opentelemetry_otlp::LogExporter> {
    match signal.protocol {
        OtlpProtocol::Grpc => {
            let builder = opentelemetry_otlp::LogExporter::builder()
                .with_tonic()
                .with_endpoint(&signal.endpoint)
                .with_timeout(EXPORT_TIMEOUT)
                .with_metadata(grpc_metadata(&signal.headers)?);
            Ok(with_grpc_tls(builder, tls)?.build()?)
        }
        OtlpProtocol::HttpProtobuf => Ok(opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(&signal.endpoint)
            .with_protocol(Protocol::HttpBinary)
            .with_timeout(EXPORT_TIMEOUT)
            .with_headers(signal.headers.clone())
            .with_http_client(http_client(tls)?)
            .build()?),
    }
}

fn with_grpc_tls<B>(builder: B, tls: &TlsMaterial) -> Result<B>
where
    B: WithTonicConfig,
{
    if tls.ca.is_none() && tls.client_certificate.is_none() {
        return Ok(builder);
    }
    let mut config = ClientTlsConfig::new();
    if let Some(ca) = &tls.ca {
        config = config.ca_certificate(Certificate::from_pem(ca));
    }
    if let (Some(certificate), Some(key)) = (&tls.client_certificate, &tls.client_key) {
        config = config.identity(Identity::from_pem(certificate, key));
    }
    Ok(builder.with_tls_config(config))
}

fn http_client(tls: &TlsMaterial) -> Result<reqwest_otel::Client> {
    let mut builder = reqwest_otel::Client::builder()
        .connect_timeout(EXPORT_TIMEOUT)
        .timeout(EXPORT_TIMEOUT)
        .redirect(reqwest_otel::redirect::Policy::none());
    if let Some(ca) = &tls.ca {
        builder = builder.add_root_certificate(
            HttpCertificate::from_pem(ca).context("OTLP CA file is not a valid PEM certificate")?,
        );
    }
    if let (Some(certificate), Some(key)) = (&tls.client_certificate, &tls.client_key) {
        let mut identity = certificate.clone();
        identity.extend_from_slice(key);
        builder = builder.identity(
            HttpIdentity::from_pem(&identity).context("OTLP client identity is not valid PEM")?,
        );
    }
    builder
        .build()
        .context("cannot initialize OTLP HTTP client")
}

fn grpc_metadata(headers: &HashMap<String, String>) -> Result<MetadataMap> {
    let mut metadata = MetadataMap::with_capacity(headers.len());
    for (name, value) in headers {
        let key = MetadataKey::<Ascii>::from_str(name)
            .map_err(|_| anyhow::anyhow!("OTLP header name is invalid for gRPC"))?;
        let mut value = MetadataValue::<Ascii>::from_str(value)
            .map_err(|_| anyhow::anyhow!("OTLP header value is invalid for gRPC"))?;
        value.set_sensitive(true);
        metadata.insert(key, value);
    }
    Ok(metadata)
}

fn normalize_endpoint(
    endpoint: &str,
    protocol: OtlpProtocol,
    default_signal_path: Option<&str>,
) -> Result<String> {
    let mut endpoint = reqwest::Url::parse(endpoint).context("OTLP endpoint is not a valid URL")?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
        || endpoint.query().is_some()
    {
        bail!("OTLP endpoint must be an absolute http(s) URL without credentials or a query");
    }
    if protocol == OtlpProtocol::HttpProtobuf
        && let Some(signal_path) = default_signal_path
    {
        let path = format!(
            "{}/{}",
            endpoint.path().trim_end_matches('/'),
            signal_path.trim_start_matches('/')
        );
        endpoint.set_path(&path);
    }
    Ok(endpoint.to_string())
}

fn parse_headers(document: &str) -> Result<HashMap<String, String>> {
    let mut headers = HashMap::new();
    for entry in document
        .replace(['\r', '\n'], ",")
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (name, value) = entry
            .split_once('=')
            .context("OTLP headers must use comma-separated name=value entries")?;
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty() {
            bail!("OTLP header name cannot be empty");
        }
        let value = percent_decode(value.trim())?;
        headers.insert(name, value);
    }
    validate_headers(&headers)?;
    Ok(headers)
}

fn percent_decode(value: &str) -> Result<String> {
    let mut bytes = Vec::with_capacity(value.len());
    let input = value.as_bytes();
    let mut index = 0;
    while index < input.len() {
        if input[index] == b'%' {
            if index + 2 >= input.len() {
                bail!("OTLP header contains invalid percent encoding");
            }
            let high = hex_nibble(input[index + 1])?;
            let low = hex_nibble(input[index + 2])?;
            bytes.push((high << 4) | low);
            index += 3;
        } else {
            bytes.push(input[index]);
            index += 1;
        }
    }
    String::from_utf8(bytes).context("OTLP header is not valid UTF-8")
}

fn hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("OTLP header contains invalid percent encoding"),
    }
}

fn validate_headers(headers: &HashMap<String, String>) -> Result<()> {
    for (name, value) in headers {
        http::HeaderName::from_str(name)
            .map_err(|_| anyhow::anyhow!("OTLP header name is invalid"))?;
        http::HeaderValue::from_str(value)
            .map_err(|_| anyhow::anyhow!("OTLP header value is invalid"))?;
    }
    Ok(())
}

fn read_secret_file(path: &Path) -> Result<Vec<u8>> {
    let metadata = std::fs::metadata(path).context("cannot inspect OTLP secret file")?;
    if metadata.len() > MAX_SECRET_FILE_BYTES {
        bail!("OTLP secret file exceeds 64 KiB");
    }
    std::fs::read(path).context("cannot read OTLP secret file")
}

fn load_tls_material<F>(lookup: &F) -> Result<TlsMaterial>
where
    F: Fn(&str) -> Option<String>,
{
    let ca =
        lookup("SCHEDULER_OTLP_TLS_CA_FILE").or_else(|| lookup("OTEL_EXPORTER_OTLP_CERTIFICATE"));
    let certificate = lookup("SCHEDULER_OTLP_TLS_CLIENT_CERT_FILE")
        .or_else(|| lookup("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE"));
    let key = lookup("SCHEDULER_OTLP_TLS_CLIENT_KEY_FILE")
        .or_else(|| lookup("OTEL_EXPORTER_OTLP_CLIENT_KEY"));
    if certificate.is_some() != key.is_some() {
        bail!("OTLP client certificate and key files must be configured together");
    }
    Ok(TlsMaterial {
        ca: ca
            .map(|path| read_secret_file(Path::new(&path)))
            .transpose()?,
        client_certificate: certificate
            .map(|path| read_secret_file(Path::new(&path)))
            .transpose()?,
        client_key: key
            .map(|path| read_secret_file(Path::new(&path)))
            .transpose()?,
    })
}

struct ObservedSpanExporter {
    inner: opentelemetry_otlp::SpanExporter,
    status: Arc<TelemetryStatus>,
}

impl fmt::Debug for ObservedSpanExporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ObservedSpanExporter(<redacted>)")
    }
}

impl SpanExporter for ObservedSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let items = batch.len() as u64;
        self.status.begin_export();
        let result = self.inner.export(batch).await;
        self.status
            .finish_export(TelemetrySignal::Traces, &result, items);
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

struct ObservedLogExporter {
    inner: opentelemetry_otlp::LogExporter,
    status: Arc<TelemetryStatus>,
}

impl fmt::Debug for ObservedLogExporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ObservedLogExporter(<redacted>)")
    }
}

impl LogExporter for ObservedLogExporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let items = batch.iter().count() as u64;
        self.status.begin_export();
        let result = self.inner.export(batch).await;
        self.status
            .finish_export(TelemetrySignal::Logs, &result, items);
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn event_enabled(
        &self,
        level: opentelemetry::logs::Severity,
        target: &str,
        name: Option<&str>,
    ) -> bool {
        self.inner.event_enabled(level, target, name)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

struct ObservedMetricExporter {
    inner: opentelemetry_otlp::MetricExporter,
    status: Arc<TelemetryStatus>,
}

impl PushMetricExporter for ObservedMetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        self.status.begin_export();
        let result = self.inner.export(metrics).await;
        self.status
            .finish_export(TelemetrySignal::Metrics, &result, 1);
        result
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn temporality(&self) -> Temporality {
        self.inner.temporality()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use opentelemetry_sdk::error::OTelSdkError;

    use super::*;

    fn config(values: &[(&str, &str)]) -> Result<TelemetryConfig> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>();
        TelemetryConfig::from_lookup("test-service", None, |name| values.get(name).cloned())
    }

    #[test]
    fn no_endpoint_keeps_telemetry_local_only() {
        let config = config(&[]).expect("config");
        assert!(!config.is_configured());
        assert_eq!(
            TelemetryStatus::new(&config).snapshot().protocol,
            "disabled"
        );
    }

    #[test]
    fn global_http_endpoint_gets_standard_per_signal_paths() {
        let config = config(&[
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "https://collector.example/otel",
            ),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
        ])
        .expect("config");
        assert_eq!(
            config.traces.as_ref().expect("traces").endpoint,
            "https://collector.example/otel/v1/traces"
        );
        assert_eq!(
            config.metrics.as_ref().expect("metrics").endpoint,
            "https://collector.example/otel/v1/metrics"
        );
        assert_eq!(
            config.logs.as_ref().expect("logs").endpoint,
            "https://collector.example/otel/v1/logs"
        );
    }

    #[test]
    fn signal_endpoint_and_protocol_override_global_values() {
        let config = config(&[
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "http://collector.example:4317",
            ),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
            (
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                "https://traces.example/custom",
            ),
            ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf"),
        ])
        .expect("config");
        let traces = config.traces.expect("traces");
        assert_eq!(traces.protocol, OtlpProtocol::HttpProtobuf);
        assert_eq!(traces.endpoint, "https://traces.example/custom");
        assert_eq!(
            config.metrics.expect("metrics").protocol,
            OtlpProtocol::Grpc
        );
    }

    #[test]
    fn headers_are_decoded_but_debug_output_is_redacted() {
        let secret = "very-secret-token";
        let endpoint = "https://collector.example/private";
        let config = config(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint),
            (
                "OTEL_EXPORTER_OTLP_HEADERS",
                "authorization=Bearer%20very-secret-token,x-tenant=reports",
            ),
        ])
        .expect("config");
        assert_eq!(
            config.traces.as_ref().expect("traces").headers["authorization"],
            "Bearer very-secret-token"
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains(secret));
        assert!(!debug.contains(endpoint));
        assert!(!debug.contains("authorization"));
        assert!(debug.contains("custom_headers: true"));
    }

    #[test]
    fn credential_and_header_files_are_bounded_and_redacted() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let header_path = directory.path().join("headers.secret");
        let token_path = directory.path().join("credential.secret");
        std::fs::write(&header_path, "x-api-key=file-secret").expect("header file");
        std::fs::write(&token_path, "bearer-secret\n").expect("token file");
        let config = config(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317"),
            (
                "SCHEDULER_OTLP_HEADERS_FILE",
                header_path.to_str().expect("path"),
            ),
            (
                "SCHEDULER_OTLP_CREDENTIAL_FILE",
                token_path.to_str().expect("path"),
            ),
            ("OTEL_EXPORTER_OTLP_HEADERS", "x-api-key=environment-secret"),
            (
                "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
                "x-api-key=trace-secret",
            ),
        ])
        .expect("config");
        let trace_headers = &config.traces.as_ref().expect("traces").headers;
        assert_eq!(trace_headers["authorization"], "Bearer bearer-secret");
        assert_eq!(trace_headers["x-api-key"], "trace-secret");
        assert_eq!(
            config.metrics.as_ref().expect("metrics").headers["x-api-key"],
            "environment-secret",
            "environment headers must override the shared header file"
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains("file-secret"));
        assert!(!debug.contains("bearer-secret"));
        assert!(!debug.contains("environment-secret"));
        assert!(!debug.contains("trace-secret"));
        assert!(!debug.contains(header_path.to_str().expect("path")));
    }

    #[test]
    fn oversized_secret_file_is_rejected_without_exposing_its_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("oversized-private-header-file");
        std::fs::write(&path, vec![b'x'; MAX_SECRET_FILE_BYTES as usize + 1])
            .expect("oversized file");
        let error = config(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317"),
            (
                "SCHEDULER_OTLP_HEADERS_FILE",
                path.to_str().expect("secret path"),
            ),
        ])
        .expect_err("oversized files must fail");
        assert!(error.to_string().contains("exceeds 64 KiB"));
        assert!(!error.to_string().contains(path.to_str().expect("path")));
    }

    #[test]
    fn malformed_tls_pair_error_does_not_expose_paths() {
        let secret_path = "/private/keys/client-secret.pem";
        let error = config(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector.example"),
            ("SCHEDULER_OTLP_TLS_CLIENT_KEY_FILE", secret_path),
        ])
        .expect_err("certificate required");
        assert!(!error.to_string().contains(secret_path));
    }

    #[test]
    fn tls_material_is_loaded_without_appearing_in_debug_output() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let ca_path = directory.path().join("private-ca.pem");
        let certificate_path = directory.path().join("private-client.pem");
        let key_path = directory.path().join("private-client-key.pem");
        std::fs::write(&ca_path, "ca-secret-material").expect("CA file");
        std::fs::write(&certificate_path, "certificate-secret-material").expect("certificate file");
        std::fs::write(&key_path, "key-secret-material").expect("key file");
        let config = config(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector.example"),
            (
                "SCHEDULER_OTLP_TLS_CA_FILE",
                ca_path.to_str().expect("CA path"),
            ),
            (
                "SCHEDULER_OTLP_TLS_CLIENT_CERT_FILE",
                certificate_path.to_str().expect("certificate path"),
            ),
            (
                "SCHEDULER_OTLP_TLS_CLIENT_KEY_FILE",
                key_path.to_str().expect("key path"),
            ),
        ])
        .expect("configuration");
        assert_eq!(
            config.tls.ca.as_deref(),
            Some(b"ca-secret-material".as_slice())
        );
        assert_eq!(
            config.tls.client_certificate.as_deref(),
            Some(b"certificate-secret-material".as_slice())
        );
        assert_eq!(
            config.tls.client_key.as_deref(),
            Some(b"key-secret-material".as_slice())
        );
        let debug = format!("{config:?}");
        for secret in [
            "ca-secret-material",
            "certificate-secret-material",
            "key-secret-material",
            ca_path.to_str().expect("CA path"),
            certificate_path.to_str().expect("certificate path"),
            key_path.to_str().expect("key path"),
        ] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn one_successful_signal_cannot_hide_another_signals_export_failure() {
        let config =
            config(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317")]).expect("config");
        let status = TelemetryStatus::new(&config);

        status.begin_export();
        status.finish_export(
            TelemetrySignal::Traces,
            &Err(OTelSdkError::InternalFailure(
                "collector unavailable".into(),
            )),
            4,
        );
        assert_eq!(
            status.snapshot().last_error_class.as_deref(),
            Some("export_failed")
        );
        assert_eq!(status.snapshot().failed_signals, ["traces"]);
        assert_eq!(status.snapshot().dropped_telemetry, 4);

        status.begin_export();
        status.finish_export(TelemetrySignal::Metrics, &Ok(()), 1);
        assert_eq!(
            status.snapshot().last_error_class.as_deref(),
            Some("export_failed"),
            "metrics recovery must not hide a trace exporter failure"
        );

        status.begin_export();
        status.finish_export(TelemetrySignal::Traces, &Ok(()), 1);
        assert_eq!(status.snapshot().last_error_class, None);
        assert!(status.snapshot().failed_signals.is_empty());
        assert_eq!(status.snapshot().export_batches_in_flight, 0);
    }

    #[tokio::test]
    async fn unavailable_collectors_do_not_block_provider_construction() {
        for protocol in ["grpc", "http/protobuf"] {
            let config = config(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9"),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", protocol),
            ])
            .expect("config");
            let status = Arc::new(TelemetryStatus::new(&config));
            let started = Instant::now();
            let providers = build_providers(&config, &status).expect("providers");
            assert!(started.elapsed() < Duration::from_secs(2));
            drop(providers);
            assert!(status.snapshot().configured);
        }
    }
}
