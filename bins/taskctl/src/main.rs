use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use reqwest::{Method, StatusCode};
use scheduler_core::ScheduleSpec;
use serde_json::Value;
use uuid::Uuid;

#[derive(Parser)]
#[command(version, about = "Administer the distributed task scheduler")]
struct Cli {
    #[arg(long, env = "SCHEDULER_URL", default_value = "http://127.0.0.1:8080")]
    url: String,
    #[arg(long, env = "SCHEDULER_ADMIN_TOKEN")]
    token: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    GenerateMasterKey,
    Schedules {
        #[command(subcommand)]
        command: ScheduleCommand,
    },
    Runs {
        #[command(subcommand)]
        command: RunCommand,
    },
    Nodes,
}

#[derive(Subcommand)]
enum ScheduleCommand {
    List,
    Show {
        id: Uuid,
    },
    Create {
        #[arg(long)]
        spec: PathBuf,
    },
    Run {
        id: Uuid,
        #[arg(long)]
        parameters: Option<PathBuf>,
        #[arg(long)]
        run_at: Option<DateTime<Utc>>,
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    Pause {
        id: Uuid,
    },
    Resume {
        id: Uuid,
    },
    RotateWebhook {
        id: Uuid,
    },
}

#[derive(Subcommand)]
enum RunCommand {
    List {
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    Show {
        id: Uuid,
    },
    Cancel {
        id: Uuid,
    },
    Retry {
        id: Uuid,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if matches!(cli.command, Command::GenerateMasterKey) {
        println!("{}", scheduler_core::SnapshotCipher::generate_base64());
        return Ok(());
    }
    let token = cli
        .token
        .context("--token or SCHEDULER_ADMIN_TOKEN is required")?;
    let client = reqwest::Client::new();
    let response = match cli.command {
        Command::GenerateMasterKey => unreachable!(),
        Command::Nodes => {
            request(
                &client,
                &cli.url,
                &token,
                Method::GET,
                "/api/v1/agents",
                None,
                None,
            )
            .await?
        }
        Command::Schedules { command } => match command {
            ScheduleCommand::List => {
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::GET,
                    "/api/v1/schedules",
                    None,
                    None,
                )
                .await?
            }
            ScheduleCommand::Show { id } => {
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::GET,
                    &format!("/api/v1/schedules/{id}"),
                    None,
                    None,
                )
                .await?
            }
            ScheduleCommand::Create { spec } => {
                let bytes = tokio::fs::read(&spec).await?;
                let parsed: ScheduleSpec = serde_json::from_slice(&bytes)
                    .or_else(|_| serde_yaml::from_slice(&bytes))
                    .with_context(|| format!("invalid schedule spec {}", spec.display()))?;
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::POST,
                    "/api/v1/schedules",
                    Some(serde_json::to_value(parsed)?),
                    None,
                )
                .await?
            }
            ScheduleCommand::Run {
                id,
                parameters,
                run_at,
                idempotency_key,
            } => {
                let parameters = match parameters {
                    Some(path) => serde_json::from_slice(&tokio::fs::read(path).await?)?,
                    None => serde_json::json!({}),
                };
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::POST,
                    &format!("/api/v1/schedules/{id}/runs"),
                    Some(serde_json::json!({"parameters": parameters, "run_at": run_at})),
                    idempotency_key.as_deref(),
                )
                .await?
            }
            ScheduleCommand::Pause { id } => {
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::POST,
                    &format!("/api/v1/schedules/{id}/pause"),
                    None,
                    None,
                )
                .await?
            }
            ScheduleCommand::Resume { id } => {
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::POST,
                    &format!("/api/v1/schedules/{id}/resume"),
                    None,
                    None,
                )
                .await?
            }
            ScheduleCommand::RotateWebhook { id } => {
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::POST,
                    &format!("/api/v1/schedules/{id}/webhook/rotate"),
                    None,
                    None,
                )
                .await?
            }
        },
        Command::Runs { command } => match command {
            RunCommand::List { limit } => {
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::GET,
                    &format!("/api/v1/runs?limit={limit}"),
                    None,
                    None,
                )
                .await?
            }
            RunCommand::Show { id } => {
                let run_path = format!("/api/v1/runs/{id}");
                let attempts_path = format!("/api/v1/runs/{id}/attempts");
                let (run, attempts) = tokio::try_join!(
                    request(
                        &client,
                        &cli.url,
                        &token,
                        Method::GET,
                        &run_path,
                        None,
                        None,
                    ),
                    request(
                        &client,
                        &cli.url,
                        &token,
                        Method::GET,
                        &attempts_path,
                        None,
                        None,
                    )
                )?;
                serde_json::json!({"run": run, "attempts": attempts})
            }
            RunCommand::Cancel { id } => {
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::POST,
                    &format!("/api/v1/runs/{id}/cancel"),
                    None,
                    None,
                )
                .await?
            }
            RunCommand::Retry { id } => {
                request(
                    &client,
                    &cli.url,
                    &token,
                    Method::POST,
                    &format!("/api/v1/runs/{id}/retry"),
                    None,
                    None,
                )
                .await?
            }
        },
    };
    if response.is_null() {
        println!("ok");
    } else {
        println!("{}", serde_json::to_string_pretty(&response)?);
    }
    Ok(())
}

async fn request(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    method: Method,
    path: &str,
    body: Option<Value>,
    idempotency_key: Option<&str>,
) -> Result<Value> {
    let mut request = client
        .request(method, format!("{}{}", base.trim_end_matches('/'), path))
        .bearer_auth(token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    if let Some(key) = idempotency_key {
        request = request.header("Idempotency-Key", key);
    }
    let response = request.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    if !status.is_success() {
        bail!(
            "request failed with {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    if status == StatusCode::NO_CONTENT || bytes.is_empty() {
        return Ok(Value::Null);
    }
    Ok(serde_json::from_slice(&bytes)?)
}
