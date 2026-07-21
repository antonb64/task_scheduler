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
    /// Generate a new coordinator snapshot-encryption key.
    GenerateMasterKey,
    /// Create, inspect, and trigger schedules.
    Schedules {
        #[command(subcommand)]
        command: ScheduleCommand,
    },
    /// Inspect and operate individual task runs.
    Runs {
        #[command(subcommand)]
        command: RunCommand,
    },
    /// Inspect and operate parameter-collection batches.
    Batches {
        #[command(subcommand)]
        command: BatchCommand,
    },
    /// List nodes or inspect and control node health.
    Nodes {
        #[command(subcommand)]
        command: Option<NodeCommand>,
    },
    /// Operate health state for a blueprint/input fingerprint.
    InputHealth {
        #[command(subcommand)]
        command: InputHealthCommand,
    },
}

#[derive(Subcommand)]
enum ScheduleCommand {
    /// List configured schedules.
    List,
    /// Show one schedule by ID.
    Show {
        id: Uuid,
    },
    /// Create a schedule from a JSON or YAML specification.
    Create {
        #[arg(long)]
        spec: PathBuf,
    },
    /// Trigger a schedule. Collection schedules return a batch receipt.
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

#[derive(Subcommand)]
enum BatchCommand {
    /// List recent parameter-collection batches.
    List {
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=1000))]
        limit: u32,
    },
    /// Show one batch by ID.
    #[command(visible_alias = "detail")]
    Show { id: Uuid },
    /// List batch items, including provider keys for the administrator.
    Items {
        id: Uuid,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=200))]
        limit: u32,
    },
    /// Cancel collection and all queued or running child runs.
    Cancel { id: Uuid },
    /// Create a new batch from the original immutable collection snapshot.
    Retrigger { id: Uuid },
}

#[derive(Subcommand)]
enum NodeCommand {
    /// List registered nodes (the default when no node command is supplied).
    List,
    /// Show the current health evaluation for a node.
    Health { id: String },
    /// Show safe, redacted health evidence for a node.
    Evidence { id: String },
    /// Manually quarantine a node from new placement.
    Quarantine { id: String },
    /// Clear manual quarantine and place the node into probation.
    Reset { id: String },
}

#[derive(Subcommand)]
enum InputHealthCommand {
    /// Release one controlled probe for a quarantined input fingerprint.
    Probe {
        blueprint_digest: String,
        input_fingerprint: String,
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
        Command::Nodes { command } => {
            let operation = node_operation(command.unwrap_or(NodeCommand::List))?;
            request(
                &client,
                &cli.url,
                &token,
                operation.method,
                &operation.path,
                None,
                None,
            )
            .await?
        }
        Command::InputHealth { command } => {
            let operation = input_health_operation(command)?;
            request(
                &client,
                &cli.url,
                &token,
                operation.method,
                &operation.path,
                None,
                None,
            )
            .await?
        }
        Command::Batches { command } => {
            let operation = batch_operation(command);
            request(
                &client,
                &cli.url,
                &token,
                operation.method,
                &operation.path,
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
    let response = request.send().await.map_err(|error| {
        let class = if error.is_timeout() {
            "timed out"
        } else if error.is_connect() {
            "could not connect"
        } else if error.is_builder() {
            "could not be built"
        } else {
            "failed"
        };
        // Do not include reqwest's Display text: it can contain the full URL,
        // including userinfo accidentally placed in the configured base URL.
        anyhow::anyhow!("request {class}")
    })?;
    let status = response.status();
    let bytes = response.bytes().await?;
    decode_response(status, &bytes)
}

fn decode_response(status: StatusCode, bytes: &[u8]) -> Result<Value> {
    if !status.is_success() {
        let code = serde_json::from_slice::<Value>(bytes)
            .ok()
            .and_then(|body| body.get("code").and_then(Value::as_str).map(str::to_owned))
            .filter(|code| is_known_safe_error_code(code));
        if let Some(code) = code {
            bail!("request failed with {status} ({code})");
        }
        bail!("request failed with {status}");
    }
    if status == StatusCode::NO_CONTENT || bytes.is_empty() {
        return Ok(Value::Null);
    }
    Ok(serde_json::from_slice(bytes)?)
}

fn is_known_safe_error_code(code: &str) -> bool {
    matches!(
        code,
        "authentication_required"
            | "resource_not_found"
            | "precondition_required"
            | "precondition_failed"
            | "connector_timeout"
            | "connector_transport_failed"
            | "connector_upstream_failed"
            | "connector_invalid_response"
            | "connector_not_configured"
            | "connector_kind_not_allowed"
            | "artifact_too_large"
            | "artifact_resolution_failed"
            | "invalid_artifact_reference"
            | "invalid_health_fingerprint"
            | "invalid_request"
            | "conflict"
    )
}

fn validate_node_id(id: &str) -> Result<()> {
    scheduler_core::validate_agent_id(id).context("invalid node ID")
}

fn validate_health_digest(label: &str, digest: &str) -> Result<()> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{label} must contain exactly 64 hexadecimal characters");
    }
    Ok(())
}

struct ApiOperation {
    method: Method,
    path: String,
}

fn batch_operation(command: BatchCommand) -> ApiOperation {
    match command {
        BatchCommand::List { limit } => ApiOperation {
            method: Method::GET,
            path: format!("/api/v1/batches?limit={limit}"),
        },
        BatchCommand::Show { id } => ApiOperation {
            method: Method::GET,
            path: format!("/api/v1/batches/{id}"),
        },
        BatchCommand::Items { id, limit } => ApiOperation {
            method: Method::GET,
            path: format!("/api/v1/batches/{id}/items?limit={limit}"),
        },
        BatchCommand::Cancel { id } => ApiOperation {
            method: Method::POST,
            path: format!("/api/v1/batches/{id}/cancel"),
        },
        BatchCommand::Retrigger { id } => ApiOperation {
            method: Method::POST,
            path: format!("/api/v1/batches/{id}/retrigger"),
        },
    }
}

fn node_operation(command: NodeCommand) -> Result<ApiOperation> {
    let (method, path) = match command {
        NodeCommand::List => (Method::GET, "/api/v1/agents".into()),
        NodeCommand::Health { id } => {
            validate_node_id(&id)?;
            (Method::GET, format!("/api/v1/agents/{id}/health"))
        }
        NodeCommand::Evidence { id } => {
            validate_node_id(&id)?;
            (Method::GET, format!("/api/v1/agents/{id}/health/evidence"))
        }
        NodeCommand::Quarantine { id } => {
            validate_node_id(&id)?;
            (
                Method::POST,
                format!("/api/v1/agents/{id}/health/quarantine"),
            )
        }
        NodeCommand::Reset { id } => {
            validate_node_id(&id)?;
            (Method::POST, format!("/api/v1/agents/{id}/health/reset"))
        }
    };
    Ok(ApiOperation { method, path })
}

fn input_health_operation(command: InputHealthCommand) -> Result<ApiOperation> {
    match command {
        InputHealthCommand::Probe {
            blueprint_digest,
            input_fingerprint,
        } => {
            validate_health_digest("blueprint digest", &blueprint_digest)?;
            validate_health_digest("input fingerprint", &input_fingerprint)?;
            Ok(ApiOperation {
                method: Method::POST,
                path: format!("/api/v1/input-health/{blueprint_digest}/{input_fingerprint}/probe"),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "018f6f8c-6302-7fc5-a0cb-b4e421fd61af";

    #[test]
    fn nodes_without_a_subcommand_remain_a_list_operation() {
        let cli = Cli::try_parse_from(["taskctl", "--token", "test", "nodes"])
            .expect("legacy nodes invocation");
        let Command::Nodes { command } = cli.command else {
            panic!("expected nodes command");
        };
        let operation = node_operation(command.unwrap_or(NodeCommand::List)).expect("operation");
        assert_eq!(operation.method, Method::GET);
        assert_eq!(operation.path, "/api/v1/agents");
    }

    #[test]
    fn batch_arguments_build_the_documented_routes() {
        let cli = Cli::try_parse_from([
            "taskctl", "--token", "test", "batches", "items", ID, "--limit", "125",
        ])
        .expect("batch items arguments");
        let Command::Batches { command } = cli.command else {
            panic!("expected batches command");
        };
        let operation = batch_operation(command);
        assert_eq!(operation.method, Method::GET);
        assert_eq!(
            operation.path,
            format!("/api/v1/batches/{ID}/items?limit=125")
        );

        let invalid = Cli::try_parse_from([
            "taskctl", "--token", "test", "batches", "items", ID, "--limit", "201",
        ]);
        assert!(
            invalid.is_err(),
            "item limits above the API maximum fail locally"
        );

        let id = Uuid::parse_str(ID).expect("UUID");
        for (command, method, suffix) in [
            (BatchCommand::Show { id }, Method::GET, ""),
            (BatchCommand::Cancel { id }, Method::POST, "/cancel"),
            (BatchCommand::Retrigger { id }, Method::POST, "/retrigger"),
        ] {
            let operation = batch_operation(command);
            assert_eq!(operation.method, method);
            assert_eq!(operation.path, format!("/api/v1/batches/{ID}{suffix}"));
        }
    }

    #[test]
    fn node_health_and_probe_arguments_are_validated_before_building_paths() {
        let health = node_operation(NodeCommand::Health {
            id: "excel-node-01".into(),
        })
        .expect("health operation");
        assert_eq!(health.method, Method::GET);
        assert_eq!(health.path, "/api/v1/agents/excel-node-01/health");
        assert!(
            node_operation(NodeCommand::Quarantine {
                id: "../unsafe".into()
            })
            .is_err()
        );

        let blueprint = "a".repeat(64);
        let input = "B".repeat(64);
        let probe = input_health_operation(InputHealthCommand::Probe {
            blueprint_digest: blueprint.clone(),
            input_fingerprint: input.clone(),
        })
        .expect("probe operation");
        assert_eq!(probe.method, Method::POST);
        assert_eq!(
            probe.path,
            format!("/api/v1/input-health/{blueprint}/{input}/probe")
        );
        assert!(
            input_health_operation(InputHealthCommand::Probe {
                blueprint_digest: "not-a-digest".into(),
                input_fingerprint: input,
            })
            .is_err()
        );
    }

    #[test]
    fn accepted_collection_trigger_receipt_is_preserved() {
        let receipt = serde_json::json!({
            "kind": "batch",
            "batch": {"id": ID, "state": "scheduled"}
        });
        let decoded = decode_response(
            StatusCode::ACCEPTED,
            &serde_json::to_vec(&receipt).expect("receipt JSON"),
        )
        .expect("accepted response");
        assert_eq!(decoded, receipt);
    }

    #[test]
    fn failed_responses_never_echo_secrets_or_provider_keys() {
        let body = br#"{
            "code":"connector_transport_failed",
            "error":"provider customer-42-secret failed with bearer top-secret"
        }"#;
        let error = decode_response(StatusCode::BAD_GATEWAY, body)
            .expect_err("failed response")
            .to_string();
        assert!(error.contains("502 Bad Gateway"));
        assert!(error.contains("connector_transport_failed"));
        assert!(!error.contains("customer-42-secret"));
        assert!(!error.contains("top-secret"));

        let malicious = br#"{"code":"secret=leaked provider=customer-42"}"#;
        let error = decode_response(StatusCode::BAD_REQUEST, malicious)
            .expect_err("malformed safe code")
            .to_string();
        assert_eq!(error, "request failed with 400 Bad Request");

        let provider_key_as_code = br#"{"code":"customer_42"}"#;
        let error = decode_response(StatusCode::BAD_REQUEST, provider_key_as_code)
            .expect_err("unrecognized code")
            .to_string();
        assert_eq!(error, "request failed with 400 Bad Request");
    }

    #[test]
    fn help_mentions_batch_and_health_operations() {
        let error = match Cli::try_parse_from(["taskctl", "batches", "--help"]) {
            Err(error) => error,
            Ok(_) => panic!("help must exit through clap"),
        };
        let help = error.to_string();
        for command in ["list", "show", "items", "cancel", "retrigger"] {
            assert!(help.contains(command), "batch help omitted {command}");
        }

        let error = match Cli::try_parse_from(["taskctl", "nodes", "--help"]) {
            Err(error) => error,
            Ok(_) => panic!("help must exit through clap"),
        };
        let help = error.to_string();
        for command in ["health", "evidence", "quarantine", "reset"] {
            assert!(help.contains(command), "node help omitted {command}");
        }
    }
}
