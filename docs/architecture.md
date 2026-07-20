# Architecture and guarantees

## Durable state machine

A schedule binds versioned blueprint and parameter artifacts to optional cron and webhook triggers. Resolution happens only when the schedule is created or updated. The coordinator encrypts the resolved definition at rest. Each trigger merges validated overrides and creates a separately encrypted, immutable execution snapshot.

Runs transition through `queued → running → succeeded|failed|cancelled`. A failed accepted attempt returns the run to `queued` with bounded exponential backoff until `max_attempts` is reached. Offers that never receive a durable agent acknowledgement do not consume the retry budget.

Before an assignment is sent, its attempt and lease token are committed to SQLite. Before acknowledging it, the agent commits the assignment to its own SQLite ledger. Completed results remain in an agent outbox until the coordinator acknowledges them. Duplicate results are idempotent.

Agents renew 60-second leases every 10 seconds. When connectivity is lost, the agent stops sending local executor keepalives. The executor then terminates the task process tree, and the coordinator eventually creates another attempt. A narrow duplicate-execution window remains unavoidable; exactly-once execution is not claimed.

## Authority and settings

There is exactly one coordinator and no leader election. An exclusive lock prevents a second coordinator from opening the same database.

Global and per-node settings are versioned coordinator documents. Agents persist and acknowledge their applied revision. Editing acquires a renewable two-minute document lock and saves with revision compare-and-swap, protecting against stale browser tabs even though the product has only one administrator identity.

Every agent hosts the same management UI by proxying browser requests through its authenticated gRPC channel. The browser therefore does not require direct coordinator connectivity.

## Isolation boundary

The agent never loads task code. It starts `task-executor`, which starts the command in a Unix process group or Windows Job Object. Timeouts, cancellations, and lease expiry terminate the tree. Output is drained into bounded one-MiB buffers per stream so a noisy task cannot exhaust agent memory.

Excel automation is implemented inside a Windows child PowerShell process using COM. That process and the Excel process it starts inherit the Job Object. This preserves Rust process isolation while avoiding in-process COM failures in the long-lived agent.

## Failure diagnostics

Every completed attempt records a safe diagnostic separately from its encrypted result. The diagnostic identifies the origin and lifecycle stage, provides a stable machine-readable failure code, indicates whether retrying may help, and carries process status information where available. Exit codes are stored in decimal and hexadecimal form; Unix signals and Excel COM HRESULT values are retained explicitly.

The executor distinguishes command failures, operating-system process crashes, spawn/isolation failures, timeout, cancellation, and agent lease loss. Excel diagnostics distinguish application startup, workbook open, macro invocation/VBA failure, macro return `1`, invalid return values, Excel process/RPC disconnection, and cleanup failures. The management UI, REST attempt endpoint, CLI, audit events, logs, and OTLP metrics use the same classification.

Only bounded sizes and truncation flags are included in this public diagnostic record. Raw stdout, stderr, PowerShell/COM exception text, resolved parameters, and environment values remain inside the encrypted result and are never copied into audit metadata or telemetry.

## Security

- Resolved snapshots and results are encrypted with XChaCha20-Poly1305.
- Administrator and webhook secrets are stored as Argon2 hashes.
- REST uses bearer tokens; the UI uses a short-lived, HttpOnly, SameSite session and CSRF tokens.
- Agent gRPC supports mandatory client-certificate verification when TLS paths are configured.
- File adapters and executable/workbook paths are constrained by explicit roots.
- Parameter and environment values are excluded from audit events and telemetry.
