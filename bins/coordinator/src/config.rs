use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(version, about = "Single authoritative task scheduler coordinator")]
pub struct Config {
    #[arg(
        long,
        env = "SCHEDULER_DATABASE_URL",
        default_value = "sqlite://scheduler.db"
    )]
    pub database_url: String,
    #[arg(long, env = "SCHEDULER_LOCK_PATH", default_value = "scheduler.lock")]
    pub lock_path: PathBuf,
    #[arg(long, env = "SCHEDULER_REST_ADDR", default_value = "127.0.0.1:8080")]
    pub rest_addr: SocketAddr,
    #[arg(long, env = "SCHEDULER_GRPC_ADDR", default_value = "127.0.0.1:50051")]
    pub grpc_addr: SocketAddr,
    #[arg(
        long,
        env = "SCHEDULER_INTERNAL_REST_URL",
        default_value = "http://127.0.0.1:8080"
    )]
    pub internal_rest_url: String,
    #[arg(long, env = "SCHEDULER_MASTER_KEY")]
    pub master_key: String,
    #[arg(long, env = "SCHEDULER_MASTER_KEY_ID", default_value = "v1")]
    pub master_key_id: String,
    #[arg(long, env = "SCHEDULER_ADMIN_TOKEN")]
    pub admin_token: String,
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,
    #[arg(
        long = "artifact-root",
        env = "SCHEDULER_ARTIFACT_ROOTS",
        value_delimiter = ','
    )]
    pub artifact_roots: Vec<PathBuf>,
    #[arg(long, env = "SCHEDULER_GRPC_TLS_CERT")]
    pub grpc_tls_cert: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_GRPC_TLS_KEY")]
    pub grpc_tls_key: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_GRPC_CLIENT_CA")]
    pub grpc_client_ca: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_HTTP_TLS_CERT")]
    pub http_tls_cert: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_HTTP_TLS_KEY")]
    pub http_tls_key: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_SECURE_COOKIES", default_value_t = false)]
    pub secure_cookies: bool,
}
