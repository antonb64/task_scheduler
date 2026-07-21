use std::{collections::HashMap, net::SocketAddr, path::PathBuf};

use anyhow::{Result, bail};
use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(version, about = "Task scheduler machine agent")]
pub struct Config {
    #[arg(long, env = "SCHEDULER_AGENT_ID")]
    pub agent_id: String,
    #[arg(
        long,
        env = "SCHEDULER_COORDINATOR_URL",
        default_value = "http://127.0.0.1:50051"
    )]
    pub coordinator_url: String,
    #[arg(
        long,
        env = "SCHEDULER_AGENT_DATABASE_URL",
        default_value = "sqlite://agent.db"
    )]
    pub database_url: String,
    #[arg(
        long,
        env = "SCHEDULER_AGENT_UI_ADDR",
        default_value = "127.0.0.1:8081"
    )]
    pub ui_addr: SocketAddr,
    #[arg(long, env = "SCHEDULER_AGENT_UI_TLS_CERT")]
    pub ui_tls_cert: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_AGENT_UI_TLS_KEY")]
    pub ui_tls_key: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_EXECUTOR_PATH", default_value = "task-executor")]
    pub executor_path: PathBuf,
    #[arg(long, env = "SCHEDULER_AGENT_CAPACITY", default_value_t = 2)]
    pub capacity: u32,
    #[arg(long = "label", env = "SCHEDULER_AGENT_LABELS", value_delimiter = ',')]
    pub label_values: Vec<String>,
    #[arg(long, env = "SCHEDULER_AGENT_TLS_CA")]
    pub tls_ca: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_AGENT_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_AGENT_TLS_KEY")]
    pub tls_key: Option<PathBuf>,
    #[arg(long, env = "SCHEDULER_AGENT_TLS_DOMAIN")]
    pub tls_domain: Option<String>,
    #[arg(
        long = "allow-environment-binding",
        env = "SCHEDULER_AGENT_ALLOWED_ENV_BINDINGS",
        value_delimiter = ','
    )]
    pub allowed_environment_bindings: Vec<String>,
    #[arg(
        long = "secret-root",
        env = "SCHEDULER_AGENT_SECRET_ROOTS",
        value_delimiter = ','
    )]
    pub secret_roots: Vec<PathBuf>,
    #[arg(
        long,
        env = "SCHEDULER_AGENT_BINDING_MAX_BYTES",
        default_value_t = 65_536
    )]
    pub binding_max_bytes: usize,
    #[arg(
        long,
        env = "SCHEDULER_TASK_OUTPUT_LOGGING",
        default_value = "metadata",
        value_parser = ["off", "metadata", "content"]
    )]
    pub task_output_logging: String,
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub otlp_endpoint: Option<String>,
}

impl Config {
    pub fn labels(&self) -> Result<HashMap<String, String>> {
        let mut labels = HashMap::new();
        for label in &self.label_values {
            let Some((key, value)) = label.split_once('=') else {
                bail!("label must use key=value syntax");
            };
            labels.insert(key.to_owned(), value.to_owned());
        }
        labels
            .entry("os".into())
            .or_insert_with(|| std::env::consts::OS.into());
        labels
            .entry("arch".into())
            .or_insert_with(|| std::env::consts::ARCH.into());
        #[cfg(windows)]
        labels
            .entry("capability".into())
            .or_insert_with(|| "excel".into());
        Ok(labels)
    }
}
