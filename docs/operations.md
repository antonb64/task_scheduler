# Operations, observability, and troubleshooting

The coordinator SQLite database and audit table are the authoritative control-plane record. OTLP export and task output telemetry are operational aids and remain non-blocking when the collector is unavailable.

## Routine checks

Unauthenticated process checks:

```sh
curl -i https://scheduler.example.com:8443/health/live
curl -i https://scheduler.example.com:8443/health/ready
```

Both return `204 No Content` on success. Liveness means the HTTP process is serving. Readiness executes `SELECT 1` against coordinator SQLite; it does not check agents, connectors, the OTLP collector, or artifact roots.

Cluster checks:

```sh
export SCHEDULER_URL=https://scheduler.example.com:8443
export SCHEDULER_ADMIN_TOKEN='<admin-token>'
taskctl nodes
taskctl schedules list
taskctl runs list --limit 100
```

The Nodes page/API reports hostname, connected state, advertised capacity/running count, bootstrap labels, last seen time, desired/applied settings revisions, and the current settings rejection error when present. A disconnected node cannot receive new assignments. A desired revision higher than the applied revision means synchronization is pending or rejected; inspect `settings_error` to distinguish them. Rejections and stale acknowledgements do not advance the applied revision or hot-apply unacknowledged placement settings. After a reconnect, a node with no provable prior applied document is placement-ineligible until it acknowledges the current desired revision. The same state can be inspected in the coordinator database read-only:

```sh
sqlite3 /var/lib/task-scheduler/coordinator.db \
  'SELECT id, desired_settings_revision, applied_settings_revision, settings_error FROM agents ORDER BY id;'
```

## Run and attempt lifecycle

Runs move through:

```text
queued -> running -> succeeded
                  -> failed
                  -> cancelled
```

The coordinator persists an attempt and lease before offering it. The agent persists acceptance before acknowledging it. Offers that never become accepted do not consume an attempt. A failed accepted attempt returns the run to `queued` with exponential backoff until the snapshot's maximum is reached.

The agent retains a finished result in its local outbox until the coordinator acknowledges it. Duplicate offers and results are idempotent at protocol boundaries, but duplicate task execution remains possible around lease expiry. Use `TASK_RUN_ID` inside application logic to make side effects idempotent.

## Structured logs

Coordinator and agent logs are JSON on stdout/stderr. Set a standard tracing filter:

```sh
RUST_LOG=info
RUST_LOG=info,scheduler_store=debug
```

Useful correlation fields include `run_id`, `attempt_id`, `agent_id`, failure code/origin/stage, and settings revision. Do not configure debug output into an untrusted sink.

Task stdout and stderr are captured in bounded buffers and also logged line by line with target `scheduler.task_output`, `stream`, `sequence`, run ID, and attempt ID. A single exported line is capped at 65,536 bytes; the captured stdout and stderr buffers are capped at 1 MiB each. Tasks must never print passwords, tokens, full parameter documents, or other secrets.

## OpenTelemetry

Set the bootstrap endpoint on each process:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=https://otel.example.internal:4317
```

The implementation uses OTLP over gRPC and exports tracing spans, metrics, and bridged structured logs in batches. Export calls have five-second timeouts. Collector unavailability does not stop scheduling, and providers attempt to flush during a normal process drop.

The currently emitted metrics are:

| Metric | Source | Attributes |
| --- | --- | --- |
| `scheduler.dispatch.offers` | coordinator | none |
| `scheduler.lease.expirations` | coordinator | none |
| `scheduler.attempt.results` | coordinator | `outcome` |
| `scheduler.task.completions` | agent | `outcome`, `executor` |
| `scheduler.task.failures` | agent | `code`, `origin`, `stage` |
| `scheduler.task.duration_ms` | agent | none |

The synchronized global/node `otlp_endpoint` fields are currently reserved and do not hot-reconfigure the provider. Restart the relevant process after changing `OTEL_EXPORTER_OTLP_ENDPOINT`.

Useful alerts include sustained lease expirations, repeated infrastructure failure codes, no results following dispatch offers, desired/applied settings divergence, and missing agent-connected logs. Queue depth and connector health are not currently exported as dedicated metrics; query the management API and logs instead.

## Failure diagnostics

`GET /api/v1/runs/{run-id}/attempts`, the run-detail UI, and `taskctl runs show` expose operator-safe attempt diagnostics:

```json
{
  "code": "excel_macro_failed",
  "origin": "excel_macro",
  "stage": "macro_invoke",
  "summary": "Excel or VBA failed while invoking the macro",
  "retryable": true,
  "status": {
    "process_id": 1234,
    "status_code": 12,
    "status_code_hex": "0x0000000C",
    "signal": null,
    "hresult": -2146827284,
    "hresult_hex": "0x800A03EC"
  }
}
```

Diagnostics deliberately exclude parameter values, environment values, raw exception text, raw stdout, and raw stderr. Output metadata reports byte counts and truncation flags.

Stable codes include:

| Area | Codes |
| --- | --- |
| Assignment/agent | `assignment_rejected`, `executor_start_failed`, `executor_process_crashed`, `executor_protocol_error`, `agent_lease_expired`, `infrastructure_error` |
| Command | `process_spawn_failed`, `process_isolation_failed`, `process_exited_non_zero`, `process_crashed`, `process_timed_out`, `cancelled` |
| Excel | `excel_unsupported`, `excel_startup_failed`, `excel_workbook_open_failed`, `excel_correlation_setup_failed`, `excel_macro_failed`, `excel_macro_returned_failure`, `excel_invalid_return`, `excel_process_crashed`, `excel_cleanup_failed` |

Origins identify the component: `coordinator`, `agent`, `task_executor`, `command_process`, `excel_host_process`, `excel_automation`, or `excel_macro`. Stages distinguish placement, validation, executor/process startup, isolation, execution, Excel startup/workbook/correlation/invocation/result/cleanup, result decoding, lease, and cancellation.

Interpret common cases:

- `process_exited_non_zero`: task program ran and returned an application status. Inspect the program outside the scheduler with equivalent non-secret inputs.
- `process_crashed`: the OS reported a crash/signal rather than a normal nonzero result.
- `executor_process_crashed`: `task-executor` itself exited without a valid result. Check agent/executor versions and OS logs.
- `process_timed_out`: the blueprint timeout elapsed and the process tree was terminated.
- `agent_lease_expired`: coordinator heartbeats stopped renewing this exact attempt. A duplicate attempt may follow.
- `excel_macro_returned_failure`: the macro returned integer `1`; this is an application failure.
- `excel_invalid_return`: the macro returned neither integer `0` nor integer `1`.
- `excel_process_crashed`: the private Excel process died or COM/RPC disconnected.
- `excel_macro_failed`: VBA or COM failed during `Application.Run`; inspect the HRESULT and reproduce under the same interactive user.
- `excel_correlation_setup_failed`: reserved workbook names conflict or could not be installed.
- `excel_cleanup_failed`: close, quit, or COM release failed after execution.

The diagnostic `retryable` value is advisory metadata. Current run-state logic retries every non-successful accepted result while attempts remain.

## SQLite backup and restore

There is no built-in backup scheduler, checkpoint endpoint, or retention worker. Arrange backups externally and test restores together with the master key.

### Online coordinator backup

SQLite's backup command obtains a consistent snapshot while WAL mode is active:

```sh
install -d -m 0700 /var/backups/task-scheduler
sqlite3 /var/lib/task-scheduler/coordinator.db \
  ".backup '/var/backups/task-scheduler/coordinator-$(date -u +%Y%m%dT%H%M%SZ).db'"
```

Do not copy only the main `.db` file while the coordinator is running; committed data may still be in `-wal`. If `sqlite3 .backup` is unavailable, stop the coordinator cleanly before copying the database and retain any accompanying WAL/SHM files as one set.

Optional manual checkpoint after a successful backup:

```sh
sqlite3 /var/lib/task-scheduler/coordinator.db 'PRAGMA wal_checkpoint(TRUNCATE);'
```

Run checkpointing during a low-write period. SQLite busy results are expected if another connection prevents truncation.

Back up together:

- Coordinator database.
- `SCHEDULER_MASTER_KEY` and its ID.
- Connector configuration and secret references.
- TLS/CA material or a reproducible certificate-issuance process.
- Artifact files needed to update schedules later.

The local agent ledger is a delivery journal, not the source of cluster history. Backing it up is usually unnecessary, but deleting it while accepted work exists can lose recovery state and cause later duplicate execution.

### Restore

1. Stop the coordinator and verify no second process owns its lock.
2. Preserve the failed database for forensic analysis.
3. Restore the consistent backup to the configured database path with correct ownership.
4. Restore the exact master key used by that database.
5. Remove only a stale lock file after verifying no coordinator process is alive; the OS lock, not file contents, provides exclusivity.
6. Start the coordinator. Migrations run automatically.
7. Verify health, schedules, run history, connector resolution on a noncritical schedule, and agent reconnection.

Restoring an older coordinator snapshot can cause agents to present results or recovery attempts the restored database no longer knows. The coordinator rejects stale attempts; inspect affected runs and retrigger with application-level idempotency.

## Upgrade procedure

1. Review release notes and run the full test suite for the target revision.
2. Take a SQLite backup and confirm the master-key backup.
3. Stop coordinator and agents.
4. Replace `coordinator`, `agent`, `task-executor`, and `taskctl` together to avoid protocol drift.
5. Start the coordinator and allow migrations to finish.
6. Start agents and confirm desired/applied revisions.
7. Run a harmless command schedule; on Windows also run a signed Excel smoke-test macro.

Database migrations are forward-only; restore the pre-upgrade database as part of a binary rollback.

## Troubleshooting

### A run remains queued

Check:

```sh
taskctl nodes
taskctl schedules show <schedule-id>
taskctl runs show <run-id>
```

- At least one node must be connected and enabled.
- Every blueprint/schedule label must exactly match an applied node label.
- `running` must be below the node's applied `max_parallel`.
- Excel requires a Windows node with `excel_max_parallel: 1`.
- A future run remains queued until `scheduled_at`/`not_before`.

### The node is online but rejects an assignment

- Desired settings may not yet be applied.
- An absolute command, working directory, or workbook may not exist or may canonicalize outside an allowlist.
- The node may have been disabled or relabeled between placement and local validation.
- A Windows node's automatic Excel label does not mean desktop Excel is installed.

### The agent does not connect

- Verify the gRPC URL and network route, not the REST port.
- Supply CA, certificate, and key together.
- Verify the server certificate SAN or set `SCHEDULER_AGENT_TLS_DOMAIN` deliberately.
- Check certificate validity, key ACLs, and client-CA chain.
- Ensure no second process owns the same local ledger lock.

### The agent UI returns 503 or 502

- `503 Coordinator is unavailable`: the agent has no active gRPC client.
- `502`: the gRPC management call or coordinator's internal REST request failed.
- Verify `SCHEDULER_INTERNAL_REST_URL`, including HTTP versus HTTPS and certificate trust.

### Schedule creation cannot read an artifact

- For `file://`, the coordinator path must exist and canonicalize under an artifact root. Agent paths are irrelevant.
- For HTTP(S), inspect transport/status logs and the 1 MiB/4 MiB limit.
- For `connector://`, verify connector name, allowed kind, bearer-token environment, service status, response media type, and body size.
- Artifact adapters run only at create/update, so save again after correcting the source.

### Cron appears late or duplicated

- Expressions include seconds; `0 0 9 * * *` is 09:00, not a five-field expression.
- Check the IANA timezone and the UI's next-five preview.
- Spring-forward gaps are skipped; both fall-back folds are valid occurrences.
- Restart catch-up does not coalesce missed or overlapping occurrences.
- At-least-once execution can create duplicate attempts for one run, even though cron run creation is unique per scheduled instant.

### A webhook fails

- `401`: missing/incorrect bearer secret.
- `404`: unknown, disabled, or rotated public ID.
- `422`: merged parameter document failed JSON Schema validation.
- `400`: malformed/non-object request.
- A paused schedule does not accept webhook runs.
- Retry with the same `Idempotency-Key` rather than inventing a new key.

### Excel hangs or fails

- Confirm the agent is running in the logged-in user's interactive session, not a service.
- Open the workbook manually once under that exact account and resolve Trust Center/file-access prompts.
- Remove dialogs, message boxes, link-update prompts, and interactive authentication from the macro.
- Confirm workbook path and allowed root spelling/case.
- Confirm the procedure is public and in a standard VBA module.
- Check argument order/types and the `Long` range.
- Inspect `origin`, `stage`, process status, and HRESULT to separate VBA failure from Excel-process crash.
- Do not open the automation workbook in the user's existing Excel instance during a scheduled run.

## Current limitations and security notes

- There is one coordinator, no election/failover, and no supported active-active database sharing.
- Delivery is at least once, not exactly once. A lease race can execute duplicate attempts.
- Process groups/Job Objects isolate crashes and cleanup; they are not a privilege or filesystem/network sandbox.
- One administrator token grants full management access. Scoped API tokens, accounts, roles, and multi-user audit identity are not implemented.
- mTLS verifies the client CA but does not bind certificate subject/SAN to `agent_id`.
- The agent UI listener is plaintext HTTP. Keep it loopback-only and put a TLS reverse proxy in front for production; direct HTTP is incompatible with the coordinator's recommended Secure session cookie.
- Coordinator snapshots/results are encrypted, but the agent ledger stores rendered assignments, environment/argument values, and results in plaintext for durable delivery. Protect the ledger directory with strict OS ACLs. Acknowledged rows are not automatically pruned.
- Raw task output can contain secrets and is exported to logs/OTLP. Avoid printing secrets.
- Online master-key rotation and multi-key decryption are not implemented.
- `audit_retention_days` does not currently prune audit events.
- Synchronized OTLP endpoint fields do not currently hot-apply.
- Node settings rejection errors are exposed in the API/UI. Rejected, stale, future, and late acknowledgements do not advance the applied revision or alter effective placement settings.
- Built-in backup/checkpoint automation is not implemented.
- Dedicated queue-depth, cron-lag, adapter-health, settings-sync, active-slot, and dropped-telemetry metrics are not yet emitted.
- Readiness covers SQLite only.
- Built-in direct HTTP artifact fetching has no production header configuration. Use a named connector when authentication is required.
- Connector health/discovery and automatic connector-to-file fallback are not implemented.
- There is no workflow DAG, container executor, artifact distribution, or schedule deletion.
- Absolute paths are allowlisted; relative command names use the agent account's `PATH`.
- Windows labels advertise Excel capability by build target and do not probe the installation.
- Office automation can hang and is not supported by Microsoft for unattended service execution; this project requires an interactive logged-in user and still cannot make arbitrary macros safe.
