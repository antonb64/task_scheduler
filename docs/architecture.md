# Architecture and guarantees

## Durable state machine

A schedule binds versioned blueprint and base-parameter artifacts to optional cron, webhook, and parameter-collection behavior. Artifact resolution happens only when the schedule is created or updated. The coordinator encrypts the resolved definition at rest. An ordinary trigger merges validated overrides and creates a separately encrypted, immutable execution snapshot.

A collection trigger first persists a batch containing the immutable schedule/blueprint revision, collection reference/limits, and encrypted overrides. A leased background worker fetches pages and commits page digest, snapshot digest, encrypted cursor, and item quarantine state with generation compare-and-swap. Static sources derive snapshot identity from exact bytes; connectors must carry one immutable snapshot ID and opaque cursor chain. A restart may replay a page, but page and provider-key uniqueness prevent duplicate logical items. Child runs become dispatchable only after final-page validation and transactional finalization.

Final collection parameters merge `base < item < trigger overrides`. Invalid items remain durable quarantined records while valid siblings become one run each. Provider keys and collection cursors are encrypted; keyed digests support duplicate detection/exact authenticated lookup without exposing them in telemetry. Per-batch active-run limits participate in work-conserving dispatch.

Cron materialization queues every missed occurrence without coalescing while a
schedule remains enabled and keeps the unique `(schedule_id, scheduled_at)`
guarantee. Deliberately paused intervals are skipped: the paused-to-enabled
transition atomically advances the cursor to the resume time. The coordinator
computes at most 1,000 next instants in one timezone-aware batch per schedule
cursor. Creating each cron run and advancing that cursor is one SQLite transaction
fenced by the expected schedule revision, cron expression, and timezone. If an edit
wins the race, a detached old schedule cannot insert an old-snapshot run or move the
new cursor.
Changing, adding, or removing the cron expression/timezone resets the cursor to the
edit time, preventing the new identity from backfilling from `created_at` or the old
identity. Other edits preserve the cursor. Runs committed before an edit retain their
original immutable snapshots.

Runs transition through `queued → running → succeeded|failed|cancelled`. A failed accepted attempt returns the run to `queued` with bounded exponential backoff until `max_attempts` is reached. Offers that never receive a durable agent acknowledgement do not consume the retry budget. A structured pre-accept `parameter_binding_failed` or `assignment_policy_rejected` response also preserves the control stream and accepted-attempt budget: the coordinator records a finished diagnostic attempt/audit event, releases the slot, and returns the run to the queue without starting an executor. The rejecting node stays excluded for that run, so it fails over to another eligible node or waits rather than hot-looping.

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

Global/per-node settings and the dashboard are versioned coordinator documents. Agents persist and acknowledge their applied settings revision. Editing acquires a renewable two-minute document lock and saves with revision compare-and-swap, protecting against stale browser tabs even though the product has only one administrator identity. A contending editor receives a read-only current document with lock owner/expiry—not the token—and may perform an explicitly confirmed, audited force release. The displaced editor still cannot save with its invalidated token or a stale revision.

Every agent hosts the same management UI by proxying browser requests through its authenticated gRPC channel. The browser therefore does not require direct coordinator connectivity. The agent listener can terminate TLS with its own PEM certificate/key; otherwise it must remain on loopback behind a trusted HTTPS reverse proxy. This browser boundary is separate from outbound control-plane mTLS.

The dashboard is another coordinator-authoritative versioned document, edited under the same renewable lock and revision fence. Search/catalog/statistics read bounded management views. Request-ID and panic-catching middleware converts handler failures to stable, correlated operator errors instead of allowing a request panic to terminate a process.

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

Blueprint bindings create a deliberately late trust boundary. The coordinator persists only source/name/type/sensitivity declarations and places work only on an agent advertising those logical capabilities. The selected agent resolves an allowlisted environment variable or canonical regular file below a bootstrap secret root, parses and schema-validates it in memory, and sends the final assignment to the executor over stdin. It checks once before durable acceptance and again before launch. Sensitive command values are restricted to environment entries; Excel may receive them as in-memory positional arguments. Resolved values are not added to the coordinator snapshot, agent ledger, fingerprints, API, audit, logs, metrics, or traces.

## Failure diagnostics

Every completed attempt records a safe diagnostic separately from its encrypted result. The diagnostic identifies the origin and lifecycle stage, provides a stable machine-readable failure code, indicates whether retrying may help, and carries process status information where available. Exit codes are stored in decimal and hexadecimal form; Unix signals and Excel COM HRESULT values are retained explicitly.

The executor distinguishes command failures, operating-system process crashes, spawn/isolation/binding failures, timeout, cancellation, and agent lease loss. Excel diagnostics distinguish application startup, workbook open, macro invocation/VBA failure, macro return `1`, invalid return values, Excel process/RPC disconnection, and cleanup failures. The management UI, REST attempt endpoint, CLI, audit events, logs, and OTLP metrics use the same classification.

Only bounded sizes and truncation flags are included in this public diagnostic record. The Excel host deliberately suppresses raw PowerShell/COM exception text; only the safe diagnostic code, lifecycle stage, summary, and HRESULT are retained. Raw command stdout and stderr remain bounded inside the encrypted coordinator result for attempts without sensitive bindings. If any binding is sensitive, the agent removes stdout, stderr, and free-form result text before its local ledger, logs, or coordinator transmission while preserving output sizes and structured status. `SCHEDULER_TASK_OUTPUT_LOGGING` defaults to metadata. The scheduler does not copy resolved parameters, provider keys, collection cursors, environment values, or secret values into audit metadata or telemetry.

## Health attribution

Every terminal attempt produces versioned health evidence keyed by nonsecret blueprint/input fingerprints, node, schedule, failure code, origin, stage, and safe status. Business outcomes (`process_exited_non_zero`, Excel return `1`) are functional evidence: they can fail a run but prove the execution path and cannot poison/quarantine. Ambiguous crash/hang/Excel invocation families can confirm input poison only when reproduced on distinct healthy nodes. A functional success clears older ambiguous correlation.

Node scoring is orthogonal to the enabled setting and caps one current observation per input. It needs diverse fingerprints—and normally diverse schedules—so a single toxic workload cannot quarantine a machine. Fleet-sensitive lease incidents may be suppressed, and evidence later attributed to confirmed poison is retracted. Automatic/manual quarantine stops new placement only. Administrative reset enters capacity-one probation; diverse functional work restores health, while infrastructure failure re-quarantines. All transitions and probes are audited.

## Security

- Resolved snapshots and results are encrypted with XChaCha20-Poly1305.
- Administrator and webhook secrets are stored as Argon2 hashes.
- REST uses bearer tokens; the UI uses a short-lived, HttpOnly, SameSite session and CSRF tokens.
- Production agent gRPC verifies the client CA and binds the exact leaf-certificate SHA-256 fingerprint to the claimed `agent_id`.
- File adapters and executable/workbook paths are constrained by explicit roots.
- Environment names are explicitly allowlisted; secret-file names are logical and canonicalized beneath local roots.
- Parameter, provider-key, environment, and secret values are excluded from audit events, health fingerprints, and telemetry.
