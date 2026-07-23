mod api;
mod auth;
mod background;
mod collection_runtime;
mod config;
mod control;
mod health;
mod management;
mod state;
mod ui;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use config::Config;
use scheduler_core::{AdapterRegistry, ConnectorConfig, SnapshotCipher};
use scheduler_protocol::control::scheduler_control_server::SchedulerControlServer;
use scheduler_store::Store;
use state::AppState;
use tokio::sync::RwLock;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tower_http::{limit::RequestBodyLimitLayer, trace::TraceLayer};
use tracing::{info, info_span};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    let telemetry = scheduler_telemetry::init_with_instance(
        "scheduler-coordinator",
        config.otlp_endpoint.as_deref(),
        &service_instance_id,
        None,
    )?;
    let telemetry_status = telemetry.status();
    info!(
        configured = telemetry_status.configured,
        protocol = %telemetry_status.protocol,
        "telemetry initialized"
    );
    if config.artifact_roots.is_empty() {
        bail!("configure at least one --artifact-root");
    }
    let store = Store::connect(&config.database_url, Some(&config.lock_path)).await?;
    let cipher = SnapshotCipher::from_base64(&config.master_key_id, &config.master_key)
        .context("invalid SCHEDULER_MASTER_KEY")?;
    let mut adapters =
        AdapterRegistry::with_defaults(config.artifact_roots.clone(), HashMap::new())?;
    let mut collection_connector_config = None;
    if let Some(path) = &config.connector_config {
        let document = tokio::fs::read(path)
            .await
            .with_context(|| format!("cannot read connector config {}", path.display()))?;
        let connector_config = ConnectorConfig::from_slice(&document)
            .with_context(|| format!("invalid connector config {}", path.display()))?;
        let connector_count = connector_config.connectors.len();
        collection_connector_config = Some(connector_config.clone());
        adapters.register_connectors(connector_config)?;
        info!(connector_count, "configured artifact sidecar connectors");
    }
    let collection_sources = collection_runtime::CollectionSourceRegistry::new(
        config.artifact_roots.clone(),
        collection_connector_config,
    )?;
    let auth = auth::AuthManager::new(&config.admin_token, config.secure_cookies)?;
    let state = AppState {
        store,
        cipher,
        adapters,
        auth,
        sessions: Arc::new(RwLock::new(HashMap::new())),
        internal_rest_url: config.internal_rest_url.clone(),
        internal_admin_token: config.admin_token.clone(),
        collection_sources,
        collection_worker_id: format!("coordinator-{}", uuid::Uuid::new_v4()),
    };

    background::spawn(state.clone());

    let app = api::router(state.clone())
        .merge(ui::router(state.clone()))
        .layer(RequestBodyLimitLayer::new(5 * 1024 * 1024))
        .layer(axum::middleware::from_fn(management::request_context))
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &axum::http::Request<_>| {
                // Query strings can contain exact provider keys. Route traces
                // therefore record only the path and method.
                info_span!(
                    "management.http.request",
                    http.request.method = %request.method(),
                    http.route = request.uri().path()
                )
            }),
        );
    let rest_addr = config.rest_addr;
    let http_cert = config.http_tls_cert.clone();
    let http_key = config.http_tls_key.clone();
    let rest = async move {
        match (http_cert, http_key) {
            (Some(cert), Some(key)) => {
                let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key).await?;
                info!(%rest_addr, "HTTPS management API listening");
                axum_server::bind_rustls(rest_addr, tls)
                    .serve(app.into_make_service())
                    .await?;
            }
            (None, None) => {
                let listener = tokio::net::TcpListener::bind(rest_addr).await?;
                info!(%rest_addr, "HTTP management API listening; configure TLS for non-local deployments");
                axum::serve(listener, app).await?;
            }
            _ => bail!("both HTTP TLS certificate and key are required"),
        }
        Ok::<_, anyhow::Error>(())
    };

    let grpc_addr = config.grpc_addr;
    let mut control = ControlServiceBuilder::new(state, &config).await?;
    let grpc = async move {
        info!(%grpc_addr, "agent control plane listening");
        control
            .builder
            .add_service(SchedulerControlServer::new(control.service))
            .serve(grpc_addr)
            .await?;
        Ok::<_, anyhow::Error>(())
    };

    tokio::try_join!(rest, grpc)?;
    Ok(())
}

struct ControlServiceBuilder {
    builder: Server,
    service: control::ControlService,
}

impl ControlServiceBuilder {
    async fn new(state: AppState, config: &Config) -> Result<Self> {
        let mut builder = Server::builder();
        let certificate_fingerprints =
            parse_agent_certificate_fingerprints(&config.agent_certificate_fingerprints)?;
        match (
            &config.grpc_tls_cert,
            &config.grpc_tls_key,
            &config.grpc_client_ca,
        ) {
            (Some(cert), Some(key), Some(ca)) => {
                if certificate_fingerprints.is_empty() {
                    bail!(
                        "gRPC mTLS requires at least one --agent-certificate-fingerprint binding"
                    );
                }
                let identity =
                    Identity::from_pem(tokio::fs::read(cert).await?, tokio::fs::read(key).await?);
                let client_ca = Certificate::from_pem(tokio::fs::read(ca).await?);
                builder = builder.tls_config(
                    ServerTlsConfig::new()
                        .identity(identity)
                        .client_ca_root(client_ca),
                )?;
            }
            (None, None, None) => {
                if !certificate_fingerprints.is_empty() {
                    bail!("agent certificate fingerprints require gRPC mTLS");
                }
                tracing::warn!("gRPC mTLS is disabled; use only for local development")
            }
            _ => bail!("gRPC TLS requires certificate, key, and client CA together"),
        }
        Ok(Self {
            builder,
            service: control::ControlService::new(
                state,
                (!certificate_fingerprints.is_empty()).then_some(certificate_fingerprints),
            ),
        })
    }
}

fn parse_agent_certificate_fingerprints(values: &[String]) -> Result<HashMap<String, [u8; 32]>> {
    let mut parsed = HashMap::new();
    let mut unique_fingerprints = HashSet::new();
    for value in values {
        let (agent_id, fingerprint) = value.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("agent certificate fingerprints must use agent-id=sha256-hex entries")
        })?;
        scheduler_core::validate_agent_id(agent_id)
            .context("invalid agent ID in certificate fingerprint binding")?;
        let normalized = fingerprint.replace(':', "").to_ascii_lowercase();
        let bytes = hex::decode(&normalized)
            .context("agent certificate fingerprint must be hexadecimal SHA-256")?;
        let fingerprint: [u8; 32] = bytes.try_into().map_err(|_| {
            anyhow::anyhow!("agent certificate fingerprint must contain exactly 32 bytes")
        })?;
        if parsed.insert(agent_id.to_owned(), fingerprint).is_some() {
            bail!("agent certificate fingerprint contains a duplicate agent ID");
        }
        if !unique_fingerprints.insert(fingerprint) {
            bail!("agent certificate fingerprint is assigned to more than one agent ID");
        }
    }
    Ok(parsed)
}

#[cfg(test)]
mod certificate_identity_tests {
    use super::parse_agent_certificate_fingerprints;

    #[test]
    fn parses_colon_formatted_sha256_and_rejects_duplicates() {
        let fingerprint = (0_u8..32)
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join(":");
        let parsed = parse_agent_certificate_fingerprints(&[format!("node-a={fingerprint}")])
            .expect("fingerprint binding");
        assert_eq!(parsed["node-a"], std::array::from_fn(|index| index as u8));
        assert!(
            parse_agent_certificate_fingerprints(&[
                format!("node-a={fingerprint}"),
                format!("node-a={fingerprint}"),
            ])
            .is_err()
        );
        assert!(
            parse_agent_certificate_fingerprints(&[
                format!("node-a={fingerprint}"),
                format!("node-b={fingerprint}"),
            ])
            .is_err()
        );
        assert!(parse_agent_certificate_fingerprints(&["node-a=abcd".into()]).is_err());
    }
}
