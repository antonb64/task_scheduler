# Rust Distributed Task Scheduler

A single-coordinator task scheduler for commands and interactive Windows Excel macros. It provides durable at-least-once delivery, isolated task processes, cron/manual/webhook triggers, machine placement, a functional management UI on every node, and OpenTelemetry export.

## Components

- `coordinator` is the sole scheduling and dispatch authority and stores cluster state in SQLite.
- `agent` connects outbound over gRPC, keeps a local delivery ledger, launches executors, and proxies the management UI.
- `task-executor` supervises one command or Excel macro in a separate process tree.
- `taskctl` administers schedules, runs, and nodes over HTTP.

At-least-once delivery means a run can execute more than once around a lease boundary. Commands receive stable `TASK_RUN_ID` and per-attempt `TASK_ATTEMPT_ID` environment variables. Excel workbooks receive the same values as temporary workbook-scoped defined names. Task code should use the run ID as its application idempotency key.

## Local quick start

Requirements are Rust 1.88 or newer and SQLite build prerequisites. `protoc` is bundled.

```sh
./scripts/dev-local.sh
```

Open <http://127.0.0.1:8080> or the agent proxy at <http://127.0.0.1:8081> and sign in with `dev-admin-token`. The script uses plaintext local networking and a public test encryption key; do not copy its secrets into a real deployment.

In another terminal, copy the example so its absolute `file://` paths point to this checkout:

```sh
cp examples/schedules/echo.example.yaml /tmp/echo-schedule.yaml
# Edit /tmp/echo-schedule.yaml and replace /absolute/path/to/task_scheduler.
export SCHEDULER_ADMIN_TOKEN=dev-admin-token
cargo run -p taskctl -- schedules create --spec /tmp/echo-schedule.yaml
cargo run -p taskctl -- schedules list
```

The local node initially permits commands found through `PATH`. Production nodes should use absolute executable paths and explicit allowlists.

## Production in brief

Build the release binaries and create a master key:

```sh
cargo build --release --locked
./target/release/taskctl generate-master-key
```

A production coordinator needs a file-backed SQLite database, an exclusive lock path, the generated master key, an administrator token, at least one artifact root, HTTPS, and agent mTLS. Agents need a stable ID, their own file-backed SQLite ledger, the `task-executor` path, and client certificate material. Keep every node UI bound to loopback unless a TLS reverse proxy protects it.

Blueprints and base parameters normally come from allowlisted `file://` URIs. `http(s)://` is also supported. Optional named HTTP sidecar connectors can supply blueprints or parameter documents through `connector://name/resource` URIs; they are loaded only when `SCHEDULER_CONNECTOR_CONFIG` is set. Artifact resolution happens when a schedule is created or updated, not before every run.

For Windows Excel execution, run the agent after interactive user logon, not as a Windows service. Desktop Excel must be installed, the workbook must be trusted/signed and preinstalled under an allowed root, and macros must never display dialogs.

## Documentation

- [Installation and node setup](docs/installation.md)
- [Bootstrap and synchronized configuration](docs/configuration.md)
- [Schedules, blueprints, parameters, cron, webhooks, and Excel](docs/tasks.md)
- [Custom artifact connectors](docs/connectors.md)
- [Operations, telemetry, backups, diagnostics, and troubleshooting](docs/operations.md)
- [HTTP API](docs/api.md)
- [Architecture and delivery guarantees](docs/architecture.md)
- [Tests and deterministic simulation](docs/testing.md)

Formal documents are available under [`schemas/`](schemas/). The examples under [`examples/`](examples/) are intended to be copied and edited.

## Development checks

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./scripts/test-simulations.sh
```

Excel integration tests additionally require a self-hosted Windows runner with licensed desktop Excel and `SCHEDULER_TEST_XLSM` pointing to the test workbook.

## Current boundaries

Version 1 has one coordinator and no leader election, workflows, container executor, artifact distribution, or exactly-once guarantee. Process isolation protects scheduler availability but is not a security sandbox. See [Current limitations and security notes](docs/operations.md#current-limitations-and-security-notes) before a production rollout.

Licensed under the [MIT License](LICENSE).
