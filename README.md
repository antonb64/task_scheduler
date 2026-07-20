# Rust Distributed Task Scheduler

A single-coordinator, at-least-once task scheduler for commands and interactive Windows Excel macros. It includes a durable SQLite control plane, machine agents, isolated task processes, cron and HTTP triggers, synchronized settings, a functional UI on every node, and OTLP telemetry.

## Components

- `coordinator` owns schedules, runs, attempts, leases, settings, audit events, and dispatch decisions.
- `agent` connects outbound over gRPC, advertises labels/capacity, persists acceptances and results locally, and hosts an HTTP UI proxy.
- `task-executor` supervises one command or Excel macro in a separate process tree.
- `taskctl` provides the administrative CLI.

At-least-once delivery permits duplicate execution near a lease boundary. Tasks must use `TASK_RUN_ID` as an idempotency key. `TASK_ATTEMPT_ID` identifies a particular execution.

## Local development

Requirements: Rust 1.88+, SQLite build prerequisites, and `protoc` is not required because the workspace uses a vendored compiler.

```sh
chmod +x scripts/dev-local.sh
./scripts/dev-local.sh
```

Open `http://127.0.0.1:8080` or the agent proxy at `http://127.0.0.1:8081` and sign in with `dev-admin-token`. The development script intentionally disables TLS and uses a public, insecure encryption key; never reuse those values outside local development.

Create a schedule by replacing `/absolute/path/to/task_scheduler` in `examples/schedules/echo.example.json`, then run:

```sh
export SCHEDULER_ADMIN_TOKEN=dev-admin-token
cargo run -p taskctl -- schedules create --spec examples/schedules/echo.example.json
cargo run -p taskctl -- schedules list
```

The UI can create and edit cron expressions, preview their next five occurrences, pause/resume schedules, trigger runs, rotate webhook secrets, inspect attempt diagnostics and audit history, and edit global or node settings. A failed run shows exactly which component and stage failed, including process exit/signal status or Excel HRESULT when available.

## Production configuration

The coordinator requires:

- `SCHEDULER_MASTER_KEY`: 32 random bytes encoded as base64. Generate one with `taskctl generate-master-key`.
- `SCHEDULER_ADMIN_TOKEN`: the single administrator/API credential.
- `SCHEDULER_ARTIFACT_ROOTS`: comma-separated allowlisted roots for `file://` artifacts.
- `SCHEDULER_DATABASE_URL` and `SCHEDULER_LOCK_PATH`.

Configure HTTPS using `SCHEDULER_HTTP_TLS_CERT` and `SCHEDULER_HTTP_TLS_KEY`. Configure agent mTLS using the coordinator's `SCHEDULER_GRPC_TLS_CERT`, `SCHEDULER_GRPC_TLS_KEY`, and `SCHEDULER_GRPC_CLIENT_CA`, plus each agent's `SCHEDULER_AGENT_TLS_CA`, `SCHEDULER_AGENT_TLS_CERT`, and `SCHEDULER_AGENT_TLS_KEY`.

Set `OTEL_EXPORTER_OTLP_ENDPOINT`, such as `https://collector.example:4317`, on coordinator and agents to export traces, metrics, structured service logs, and bounded task stdout/stderr. SQLite audit events remain authoritative during collector outages.

Bootstrap values—network listeners, database/key/certificate paths, and coordinator addresses—are intentionally not synchronized through the UI.

## Excel macros

Excel macro blueprints are Windows-only and require desktop Excel in a logged-in interactive session. Use [the scheduled-task helper](deploy/windows/Register-Agent.ps1) to start the agent at logon; do not run it as a Windows service.

The executor creates a private Excel COM instance, opens an allowlisted preinstalled workbook, calls `Application.Run` with up to 30 positional JSON scalar arguments, and maps return `0` to success and `1` to task failure. Other values and COM errors are infrastructure failures. Workbooks must be trusted/signed, macros must not show dialogs, and Excel concurrency is capped at one per agent.

Node settings must list absolute `allowed_workbook_roots`. Absolute command paths and working directories likewise require `allowed_command_roots`; commands resolved through `PATH` remain available.

## Validation

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Windows Excel integration requires a self-hosted runner with licensed desktop Excel. The portable tests use a fake boundary and exercise process execution, lease expiry, cryptography, cron parsing, templating, storage transitions, idempotency, and settings locking.

See [docs/architecture.md](docs/architecture.md) and [docs/api.md](docs/api.md) for protocol and operational details.
