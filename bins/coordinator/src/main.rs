mod api;
mod auth;
mod background;
mod config;
mod control;
mod state;
mod ui;

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, bail};
use clap::Parser;
use config::Config;
use scheduler_core::{AdapterRegistry, SnapshotCipher};
use scheduler_protocol::control::scheduler_control_server::SchedulerControlServer;
use scheduler_store::Store;
use state::AppState;
use tokio::sync::RwLock;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tower_http::{limit::RequestBodyLimitLayer, trace::TraceLayer};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    let _telemetry =
        scheduler_telemetry::init("scheduler-coordinator", config.otlp_endpoint.as_deref())?;
    if config.artifact_roots.is_empty() {
        bail!("configure at least one --artifact-root");
    }
    let store = Store::connect(&config.database_url, Some(&config.lock_path)).await?;
    let cipher = SnapshotCipher::from_base64(&config.master_key_id, &config.master_key)
        .context("invalid SCHEDULER_MASTER_KEY")?;
    let adapters = AdapterRegistry::with_defaults(config.artifact_roots.clone(), HashMap::new())?;
    let auth = auth::AuthManager::new(&config.admin_token, config.secure_cookies)?;
    let state = AppState {
        store,
        cipher,
        adapters,
        auth,
        sessions: Arc::new(RwLock::new(HashMap::new())),
        internal_rest_url: config.internal_rest_url.clone(),
        internal_admin_token: config.admin_token.clone(),
    };

    background::spawn(state.clone());

    let app = api::router(state.clone())
        .merge(ui::router(state.clone()))
        .layer(RequestBodyLimitLayer::new(5 * 1024 * 1024))
        .layer(TraceLayer::new_for_http());
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
        match (
            &config.grpc_tls_cert,
            &config.grpc_tls_key,
            &config.grpc_client_ca,
        ) {
            (Some(cert), Some(key), Some(ca)) => {
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
                tracing::warn!("gRPC mTLS is disabled; use only for local development")
            }
            _ => bail!("gRPC TLS requires certificate, key, and client CA together"),
        }
        Ok(Self {
            builder,
            service: control::ControlService::new(state),
        })
    }
}
