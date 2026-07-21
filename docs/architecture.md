# Architecture and guarantees

## Durable state machine

A schedule binds versioned blueprint and parameter artifacts to optional cron and webhook triggers. Resolution happens only when the schedule is created or updated. The coordinator encrypts the resolved definition at rest. Each trigger merges validated overrides and creates a separately encrypted, immutable execution snapshot.

Cron materialization queues every missed occurrence without coalescing and keeps the
unique `(schedule_id, scheduled_at)` guarantee. The coordinator computes at most
1,000 next instants in one timezone-aware batch per schedule cursor. Creating each
cron run and advancing that cursor is one SQLite transaction fenced by the expected
schedule revision, cron expression, and timezone. If an edit wins the race, a
detached old schedule cannot insert an old-snapshot run or move the new cursor.
Changing, adding, or removing the cron expression/timezone resets the cursor to the
edit time, preventing the new identity from backfilling from `created_at` or the old
identity. Other edits preserve the cursor. Runs committed before an edit retain their
original immutable snapshots.

Runs transition through `queued → running → succeeded|failed|cancelled`. A failed accepted attempt returns the run to `queued` with bounded exponential backoff until `max_attempts` is reached. Offers that never receive a durable agent acknowledgement do not consume the retry budget.

Before an assignment is sent, its attempt and lease token are committed to SQLite. Before acknowledging it, the agent commits the assignment to its own SQLite ledger. Completed results remain in an agent outbox until the coordinator acknowledges them. Duplicate results are idempotent.

Recovery is an explicit second handshake. After restart, the agent first applies
authoritative node settings and then asks the coordinator to reauthorize the
exact attempt ID and lease token. Only an accepted, unexpired attempt owned by
that node whose run is still active is granted. Rejected rows are durably
cancelled locally. An OS-exclusive agent-ledger lock and fenced state writes
prevent two live agent processes from claiming the same assignment.

Agents renew 60-second leases every 10 seconds. The coordinator acknowledges
the exact attempt IDs whose authoritative leases were renewed; only that fresh
evidence produces a local executor keepalive. A half-open stream therefore
cannot keep work alive. The executor terminates the task process tree after
renewal loss, and the coordinator eventually creates another attempt. A narrow
duplicate-execution window remains unavoidable; exactly-once execution is not
claimed.

## Authority and settings

There is exactly one coordinator and no leader election. An exclusive lock prevents a second coordinator from opening the same database.

Global and per-node settings are versioned coordinator documents. Agents persist and acknowledge their applied revision. Editing acquires a renewable two-minute document lock and saves with revision compare-and-swap, protecting against stale browser tabs even though the product has only one administrator identity.

Every agent hosts the same management UI by proxying browser requests through its authenticated gRPC channel. The browser therefore does not require direct coordinator connectivity.

## Isolation boundary

The agent never loads task code. It starts `task-executor`, which starts the command in a Unix process group or Windows Job Object. Timeouts, cancellations, and lease expiry terminate the tree. Output is drained into bounded one-MiB buffers per stream so a noisy task cannot exhaust agent memory.

Excel automation is implemented inside a Windows child PowerShell process using
COM. The Job Object has an unpredictable name. Before opening a workbook, the
host resolves `Application.Hwnd` to the exact Excel PID, rejects pre-existing
Excel PIDs, checks the process identity and start time, explicitly attaches that
PID to the same Job Object, and verifies membership. An identity or isolation
failure aborts without quitting or terminating a pre-existing user instance.
Windows shutdown sends `CTRL_BREAK` before escalating to `TerminateJobObject`.
Output draining also has a fixed deadline, so a descendant which inherits an
exited leader's pipes cannot wedge result delivery.

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
