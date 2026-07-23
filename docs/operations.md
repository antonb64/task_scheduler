# Operations, observability, and troubleshooting

Coordinator SQLite and each agent ledger remain the durable storage engines, but operators do not need to query them to determine processing state. Durable `scheduler.state` OTLP logs reproduce lifecycle history, reconciled gauges project current state, and the shared daily evaluator produces freshness-gated schedule and cluster verdicts. Collector outages remain non-blocking; state events queue and replay with stable IDs. See the [authoritative observability contract](observability.md).

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
taskctl batches list --limit 50
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

The coordinator persists an attempt and lease before offering it. The agent persists acceptance before acknowledging it. Offers that never become accepted do not consume an attempt. A structured binding/policy rejection is retained as a finished diagnostic attempt but leaves the run's accepted-attempt counter unchanged. A failed accepted attempt returns the run to `queued` with exponential backoff until the snapshot's maximum is reached.

The agent retains a finished result in its local outbox until the coordinator acknowledges it. Duplicate offers and results are idempotent at protocol boundaries, but duplicate task execution remains possible around lease expiry. Use `TASK_RUN_ID` inside application logic to make side effects idempotent.

## Structured logs

Coordinator and agent logs are JSON on stdout/stderr. Set a standard tracing filter:

```sh
RUST_LOG=info
RUST_LOG=info,scheduler_store=debug
```

Useful correlation fields include request, schedule, schedule revision, blueprint digest, batch, run, attempt, agent/node, settings revision, trigger, connector, HTTP/gRPC status, process status, and failure code/origin/stage. Not every event has every field. Start with one durable ID from the UI/API, then filter on its linked run/attempt/batch/node IDs. Do not configure debug output into an untrusted sink.

`SCHEDULER_TASK_OUTPUT_LOGGING` controls the agent's `scheduler.task_output` events. Attempts with at least one sensitive binding always suppress stdout, stderr, and free-form result text before any logging, local ledger write, or transmission:

| Mode | Behavior |
| --- | --- |
| `off` | Emits no task-output event. |
| `metadata` | Default. Emits only stdout/stderr byte counts and truncation flags with run/attempt IDs. |
| `content` | Emits captured stdout/stderr line content with stream and sequence. Each exported line is capped at 65,536 UTF-8 bytes. |

The execution capture remains bounded to 1 MiB per stream in every mode and its metadata remains available in attempt diagnostics. `content` is an explicit data-disclosure setting: scheduler redaction cannot reliably remove secrets printed by arbitrary tasks. Keep production at `metadata` or `off`; never print passwords, tokens, provider keys, parameter documents, collection cursors, or secret values.

Useful filters include:

```sh
RUST_LOG=info,scheduler_store=debug
RUST_LOG=info,scheduler.task_output=off
RUST_LOG=info,coordinator.dispatch=debug
```

The last target-specific syntax depends on the log collector's handling of Rust tracing targets. Keep full safe diagnostics in a protected sink; UI `500` pages deliberately show only a request ID.

## OpenTelemetry

Set bootstrap telemetry on each process. gRPC remains the default:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=https://otel.example.internal:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
```

For OTLP HTTP/protobuf:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=https://otel.example.internal:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
```

The exporter supports global and per-signal standard endpoint, protocol, and header variables: `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_{ENDPOINT,PROTOCOL,HEADERS}`. A global HTTP endpoint gets `/v1/traces`, `/v1/metrics`, or `/v1/logs` appended; an explicit per-signal endpoint is used as provided. Headers use the standard comma-separated `key=value` form with percent decoding.

Prefer local secret files over putting credentials in an environment or synchronized settings:

```sh
SCHEDULER_OTLP_CREDENTIAL_FILE=/run/secrets/otel-bearer-token
SCHEDULER_OTLP_HEADERS_FILE=/run/secrets/otel-headers
SCHEDULER_OTLP_TLS_CA_FILE=/etc/task-scheduler/otel-ca.pem
SCHEDULER_OTLP_TLS_CLIENT_CERT_FILE=/etc/task-scheduler/otel-client.pem
SCHEDULER_OTLP_TLS_CLIENT_KEY_FILE=/etc/task-scheduler/otel-client.key
```

The credential file contains the bare bearer token and produces an `Authorization: Bearer ...` header. `SCHEDULER_OTLP_BEARER_TOKEN_FILE` is an alias. The headers file uses the same `key=value,key2=value2` format as `OTEL_EXPORTER_OTLP_HEADERS`; environment headers override duplicate file keys. Standard TLS aliases `OTEL_EXPORTER_OTLP_CERTIFICATE`, `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE`, and `OTEL_EXPORTER_OTLP_CLIENT_KEY` are also accepted. Client certificate/key must be configured together. Each secret file is capped at 64 KiB; endpoints may not contain credentials or query strings. Values and file paths are redacted from telemetry configuration debug output.

Diagnostic traces/logs use bounded best-effort queues. Span/log queues hold 2,048 items and export at most 512 per batch; metrics export every 30 seconds; calls have five-second timeouts. Authoritative `scheduler.state` logs first enter a separate durable SQLite outbox and are acknowledged only after exporter success. Collector unavailability does not stop scheduling; events replay with the same ID. Retention exhaustion creates a durable coverage gap and forces `unknown`.

Authenticated `GET /api/v1/telemetry/status` reports safe per-signal freshness/failure state plus the authoritative snapshot time, outbox depth/oldest event, delivered/expired totals, coverage-gap state, and current/previous cluster verdict. The dashboard renders the same daily evaluator. Status never exposes endpoints, headers, credentials, certificate material, or file paths. Providers attempt to flush during normal shutdown.

The currently emitted metrics are:

| Metric | Source | Attributes |
| --- | --- | --- |
| `scheduler.dispatch.offers` | coordinator | none |
| `scheduler.dispatch.passes` / `scheduler.dispatch.candidates` | coordinator | none |
| `scheduler.queue.depth` | coordinator | `readiness=ready|delayed` |
| `scheduler.dispatch.decisions` | coordinator | `decision` |
| `scheduler.dispatch.latency_ms` | coordinator | none |
| `scheduler.lease.expirations` | coordinator | none |
| `scheduler.agent.acceptances` | coordinator | `result` |
| `scheduler.attempt.results` | coordinator | `outcome` |
| `scheduler.settings.sync` | coordinator/agent | `result` |
| `scheduler.agent.assignment_offers` | agent | `result`, `executor` |
| `scheduler.agent.results` | agent | `result` |
| `scheduler.agent.result_persist_retries` | agent | none |
| `scheduler.task.completions` | agent | `outcome`, `executor` |
| `scheduler.task.failures` | agent | `code`, `origin`, `stage` |
| `scheduler.task.duration_ms` | agent | none |
| `scheduler.executor.lifetime_ms` | agent | executor lifecycle attributes |
| `scheduler.management.proxy.requests` / `scheduler.management.proxy.duration_ms` | agent | bounded `result`, `status_class` |
| `scheduler.collection.source.fetches` / `scheduler.collection.source.fetch.duration_ms` | coordinator | `source=file|http|connector`, `outcome=success|error` |
| `scheduler.collection.errors` | coordinator | bounded safe `stage`, `code` |
| `scheduler.collection.worker.claimed_batches` | coordinator | none |
| `scheduler.collection.batches` | coordinator | `state` |
| `scheduler.collection.state_transitions` | coordinator | `to` |
| `scheduler.collection.items` | coordinator | `state=ready|invalid|poisoned|held` |
| `scheduler.collection.page.commits` / `scheduler.collection.page.items` / `scheduler.collection.page.duration_ms` | coordinator | `source`, `outcome=applied|replayed|stale|error` |
| `scheduler.collection.finalizations` / `scheduler.collection.finalization.duration_ms` | coordinator | `outcome=finalized|already_finalized|error` |
| `scheduler.node_health.transitions` | coordinator | `state` |
| `scheduler.state.entities` / `scheduler.state.overdue` | coordinator | reconciled `schedule.id`, bounded `entity`, `state`/`window` |
| `scheduler.schedule.daily.triggers` / `scheduler.schedule.daily.items` | coordinator | `schedule.id`, `window`, bounded status/state |
| `scheduler.schedule.daily.retries` / `scheduler.schedule.daily.attempt_anomalies` | coordinator | `schedule.id`, `window` |
| `scheduler.schedule.daily.verdict` | coordinator | one-hot `schedule.id`, `window`, `verdict` |
| `scheduler.schedule.daily.operations_day` / `scheduler.schedule.completion_deadline_seconds` | coordinator | schedule/window; operations day is `YYYYMMDD` and carries bounded timezone |
| `scheduler.schedule.last_success_age_seconds` / `scheduler.cron.backlog` | coordinator | `schedule.id` and window where applicable |
| `scheduler.cluster.agents` / `scheduler.cluster.agent.capacity` | coordinator | `agent.id`, bounded connection/health/settings/kind |
| `scheduler.agent.assignment_state` / `scheduler.agent.pending_results` | agent | assignment state or none |
| `scheduler.observability.snapshot.generated_at_unix_seconds` | coordinator/agent | none |
| `scheduler.observability.outbox.depth` / `scheduler.observability.outbox.oldest_age_seconds` / `scheduler.observability.outbox.expired_events` | coordinator/agent | none |
| `scheduler.observability.coverage_gap` | coordinator/agent | none |
| `scheduler.observability.telemetry.export_failures` / `scheduler.observability.telemetry.dropped_items` | coordinator | signal where applicable |

The synchronized global/node `otlp_endpoint` fields are currently reserved and do not hot-reconfigure the provider. All endpoint/protocol/credential/TLS changes require a process restart. Atomic runtime exporter reconfiguration is not implemented; a bad startup configuration fails startup rather than replacing a live exporter.

Collection traces are `coordinator.collection.worker_pass`, `coordinator.collection.batch`, `coordinator.collection.source.fetch`, `coordinator.collection.page.commit`, and `coordinator.collection.batch.finalize`. They correlate batch/schedule/trigger, safe source kind/page size, page generation/count/finality/outcome, and finalization outcome without provider keys, cursors, resource queries, or parameters. `coordinator.health.classify_attempt` correlates run, attempt, and node in a trace; node IDs intentionally remain out of metric labels. Queue depth is observed inside `coordinator.dispatch.pass`, with each candidate in `coordinator.dispatch.candidate`.

Daily, freshness-gated dashboard and alert recipes are specified in [Authoritative observability](observability.md#backend-neutral-dashboard-specification). In particular, stale or missing snapshots and coverage gaps are `unknown`, never green.

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
| Binding | `parameter_binding_failed` |
| Command | `process_spawn_failed`, `process_isolation_failed`, `process_exited_non_zero`, `process_crashed`, `process_timed_out`, `cancelled` |
| Excel | `excel_unsupported`, `excel_startup_failed`, `excel_workbook_open_failed`, `excel_correlation_setup_failed`, `excel_macro_failed`, `excel_macro_returned_failure`, `excel_invalid_return`, `excel_process_crashed`, `excel_cleanup_failed` |

Origins identify the component: `coordinator`, `agent`, `task_executor`, `command_process`, `excel_host_process`, `excel_automation`, or `excel_macro`. Stages distinguish placement, validation, executor/process startup, isolation, execution, Excel startup/workbook/correlation/invocation/result/cleanup, result decoding, lease, and cancellation.

At the control-protocol boundary, the two structured pre-accept rejection codes are `parameter_binding_failed` and `assignment_policy_rejected`. The coordinator persists the latter as diagnostic code `assignment_rejected` with origin `agent` and stage `validation`.

Interpret common cases:

- `process_exited_non_zero`: task program ran and returned an application status. Inspect the program outside the scheduler with equivalent non-secret inputs.
- `process_crashed`: the OS reported a crash/signal rather than a normal nonzero result.
- `executor_process_crashed`: `task-executor` itself exited without a valid result. Check agent/executor versions and OS logs.
- `process_timed_out`: the blueprint timeout elapsed and the process tree was terminated.
- `agent_lease_expired`: coordinator heartbeats stopped renewing this exact attempt. A duplicate attempt may follow.
- `parameter_binding_failed`: the selected node could not safely advertise, read, decode, validate, or render a declared environment/secret-file binding. Confirm the source/name allowlist, service environment or secret root/file ACL, type, size, and schema. The diagnostic intentionally omits the name/path/value.
- `assignment_rejected` at the validation stage: after resolving late bindings, the node found that the assignment violates its current command/workbook allowlist or another execution-policy check. Compare the node's desired/applied settings and the executor path. The wire rejection code is `assignment_policy_rejected`; the persisted diagnostic code is `assignment_rejected`.
- `excel_macro_returned_failure`: the macro returned integer `1`; this is an application failure.
- `excel_invalid_return`: the macro returned neither integer `0` nor integer `1`.
- `excel_process_crashed`: the private Excel process died or COM/RPC disconnected.
- `excel_macro_failed`: VBA or COM failed during `Application.Run`; inspect the HRESULT and reproduce under the same interactive user.
- `excel_correlation_setup_failed`: reserved workbook names conflict or could not be installed.
- `excel_cleanup_failed`: close, quit, or COM release failed after execution.

The diagnostic `retryable` value is advisory metadata. Current run-state logic retries every non-successful accepted result while attempts remain. A `parameter_binding_failed` or validation-stage `assignment_rejected` received before acceptance is different: it is recorded for diagnosis, returns the run to the queue, does not increment the accepted-attempt count, and does not tear down the agent stream or start `task-executor`. The rejecting node stays excluded for that run; if no other eligible node exists, the run waits rather than hot-looping.

## Input poison and node health

Health scoring is separate from `NodeSettings.enabled`. Inspect it from `/nodes/{agent-id}`, the health API, or:

```sh
taskctl nodes health <agent-id>
taskctl nodes evidence <agent-id>
```

`process_exited_non_zero` and `excel_macro_returned_failure` (macro return `1`) are functional/business observations. They prove that the node executed the workload contract and never poison an input or penalize a node. Cancellation is ignored. Crash, timeout, Excel invocation/contract/cleanup, and other ambiguous failures first make an input suspected. The same failure family on the schedule's `poison_distinct_nodes` healthy nodes, at least two, confirms poison unless a later functional success intervenes. Retries prefer a different healthy node; without one the input remains suspected rather than being falsely confirmed.

Confirmed fingerprints are based on blueprint digest plus canonical nonsecret parameters. Sensitive bound values and provider keys are excluded. Future matching collection items become `held`. Release one audited probe from the input-health action in the UI or:

```sh
taskctl input-health probe <64-hex-blueprint-digest> <64-hex-input-fingerprint>
```

Node scoring considers the latest observation per input, at most ten, inside 15 minutes. It becomes `suspect` after at least three failures across three fingerprints and a 50% failure rate. Normal auto-quarantine requires at least five failures, a 60% rate, and four fingerprints across at least two schedules; one-schedule evidence requires six fingerprints. Strong local failures—executor start/protocol, binding availability, process isolation, Excel unsupported/startup—use a five-minute fast path of three fingerprints across two schedules. Lease incidents can be cluster-suppressed, and evidence later confirmed as input poison is retracted from node scoring. One repeated input alone cannot quarantine a node.

Quarantine blocks new placement but does not stop active work:

```sh
taskctl nodes quarantine <agent-id>
taskctl nodes reset <agent-id>
```

Reset is allowed only from manual/automatic quarantine and enters `probation` at capacity one. Five functional observations across three inputs restore `healthy`; an ambiguous/strong infrastructure observation re-quarantines the node. Every transition, retraction, manual action, and probe is audited. Do not reset repeatedly to hide a hardware/Excel installation fault; fix it, then use probation as the controlled verification path.

## Management UI and diagnostics

Every connected node proxies the coordinator-authoritative control panel. Schedule, run, attempt, run-event, batch/item, node, node-health-evidence, and blueprint-revision REST lists use cursor-page objects. They default to 50 entries and clamp requests to 1–200; stable entity-specific timestamps/numbers plus IDs define traversal order. Preserve the opaque cursor unchanged for the same endpoint and filters. `taskctl` prints the page envelope, including any `next_cursor`, rather than silently unwrapping or fetching every page.

Global ID search accepts a canonical ID or unique/ambiguous prefix of at least eight safe characters and searches schedule, run, attempt, node, and loaded blueprint digest records. It shows all matches rather than selecting an ambiguous prefix. The runs page shows each schedule name and supports an exact schedule filter which is preserved across cursor pages. Failed and retrying runs show their latest safe code, origin, lifecycle stage, and summary in the list. Batch/provider-key search is deliberately local to an authenticated batch detail page and exact-match only.

The cluster dashboard is revision-fenced and protected by the same two-minute edit lease as settings. Its lifetime node-throughput table is backed by a transactional rollup and shows completed task attempts, succeeded/failed/cancelled outcomes, active slots, and the latest completion time. A retry counts again on the node which processed it. Up to 100 schedule cards can be ordered. Cards retain the 24-hour operational view, while the daily table uses the shared schedule-local evaluator and shows current/previous operations day, deadline, expected/materialized/completed/pending/overdue work, retries, bad items, last success, and verdict. Telemetry coverage and outbox health are visible beside per-signal exporter freshness.

Loaded-blueprint pages show metadata only: digest, redacted source, source version/load time, executor kind, labels, policy/schema/binding declarations, and current/retained usage. Collection/base parameter values and secret values are never shown.

Management requests carry `X-Request-ID`, route spans, and panic-catching middleware. Normal authenticated UI failures show the bounded, HTML-escaped operator error chain directly and retain the request ID for log correlation. Run and collection failures show structured "what went wrong / where" diagnostics without exposing task output or secret parameter values. Panics and template/proxy failures remain opaque request-ID-only responses because their payloads are uncontrolled; they must not terminate the coordinator/agent. Agent proxy bodies and timeouts are bounded.

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
- The node must be healthy/suspect/probation (quarantined nodes reject placement), and probation has capacity one.
- Every declared binding source/name must be advertised by the node.

### A collection batch fails or stops progressing

```sh
taskctl batches show <batch-id>
taskctl batches items <batch-id> --limit 200
```

- `collection_connector_*`: verify connector config, bearer environment, TLS/route, content type, protocol version, and upstream status. The scheduler never falls back.
- `collection_snapshot_drift`/`collection_snapshot_expired`: the provider changed or expired the immutable view during paging. Extend snapshot lifetime and replay from a stable transaction/version.
- `collection_cursor_cycle`/`collection_cursor_invalid`: the provider repeated, emptied, oversized, or malformed a non-final cursor/page.
- `collection_conflicting_duplicate_key`/`collection_duplicate_key`: stable provider keys were repeated inconsistently. Make keys unique and deterministic within a snapshot.
- `collection_item_limit_exceeded`/`collection_page_too_large`/`collection_source_too_large`: reduce the source/page or increase the schedule limit within the hard cap.
- Item codes `collection_item_invalid_*`, `collection_item_schema_invalid`, `collection_item_merge_failed`, or `collection_item_render_failed` quarantine only bad items; inspect counts and provider keys in the authenticated batch page while valid siblings continue.
- `collection_input_poisoned`/`held`: inspect the linked input-health evidence and release a single probe only after diagnosing the workload.

### The node is online but rejects an assignment

- Desired settings may not yet be applied.
- An absolute command, working directory, or workbook may not exist or may canonicalize outside an allowlist.
- The node may have been disabled or relabeled between placement and local validation.
- A Windows node's automatic Excel label does not mean desktop Excel is installed.

### The agent does not connect

- Verify the gRPC URL and network route, not the REST port.
- Supply CA, certificate, and key together.
- Confirm the coordinator has exactly one `SCHEDULER_AGENT_CERTIFICATE_FINGERPRINTS` entry for the configured `SCHEDULER_AGENT_ID` and that it is the SHA-256 of the presented leaf certificate's DER bytes—not of its PEM file text.
- After client-certificate renewal, update the fingerprint map and restart the coordinator before switching the agent certificate.
- Verify the server certificate SAN or set `SCHEDULER_AGENT_TLS_DOMAIN` deliberately.
- Check certificate validity, key ACLs, and client-CA chain.
- Ensure no second process owns the same local ledger lock.

### The agent UI returns 503 or 502

- `503 Coordinator is unavailable`: the agent has no active gRPC client.
- `502`: the gRPC management call or coordinator's internal REST request failed.
- For native HTTPS, configure both `SCHEDULER_AGENT_UI_TLS_CERT` and `SCHEDULER_AGENT_UI_TLS_KEY` and verify their PEM/hostname. A partial pair, certificate/key load error, UI bind failure, or later listener/server failure terminates the agent; inspect the startup log and service restart status.
- For proxy TLS, keep the agent listener on loopback HTTP and verify the proxy forwards the complete request/response without exposing port 8081.
- Verify `SCHEDULER_INTERNAL_REST_URL`, including HTTP versus HTTPS and certificate trust.

### A settings or dashboard editor is read-only

- Another tab/session owns the two-minute lock. The page shows owner category and expiry but never its token.
- Use **Retry lock** after the other page navigates away or the lease expires.
- Use **Force unlock** only after confirming the other editor is abandoned. The action is CSRF-protected and audited as `settings.lock_force_released`; it invalidates the other page's token but does not bypass revision checks.

### Schedule creation cannot read an artifact

- For `file://`, the coordinator path must exist and canonicalize under an artifact root. Agent paths are irrelevant.
- For HTTP(S), inspect transport/status logs and the 1 MiB/4 MiB limit.
- For `connector://`, verify connector name, allowed kind, bearer-token environment, service status, response media type, and body size.
- Artifact adapters run only at create/update, so save again after correcting the source.

### Cron appears late or duplicated

- Expressions include seconds; `0 0 9 * * *` is 09:00, not a five-field expression.
- Check the IANA timezone and the UI's next-five preview.
- Spring-forward gaps are skipped; both fall-back folds are valid occurrences.
- Restart catch-up for schedules that remained enabled does not coalesce missed or
  overlapping occurrences. A deliberate pause is different: resume skips every
  occurrence in the paused interval.
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
- Production gRPC mTLS binds the exact leaf-certificate SHA-256 fingerprint to `agent_id`; subject/SAN text is not the identity mapping. Renewals require a configured fingerprint cutover and coordinator restart.
- The agent UI can terminate HTTPS with `SCHEDULER_AGENT_UI_TLS_CERT`/`KEY`. Without them, keep its HTTP listener loopback-only and use a trusted TLS reverse proxy; direct HTTP is incompatible with the coordinator's recommended Secure session cookie.
- Coordinator snapshots/results are encrypted, but the agent ledger stores rendered assignments, environment/argument values, and results in plaintext for durable delivery. Protect the ledger directory with strict OS ACLs. Acknowledged rows are not automatically pruned.
- Raw task output can contain secrets. The default exports metadata only. `SCHEDULER_TASK_OUTPUT_LOGGING=content` deliberately exports content only for attempts without sensitive bindings; sensitive-binding attempts suppress task text in every mode. Applications should still avoid printing secrets.
- Online master-key rotation and multi-key decryption are not implemented.
- `audit_retention_days` does not currently prune audit events.
- Synchronized OTLP endpoint fields do not currently hot-apply.
- Node settings rejection errors are exposed in the API/UI. Rejected, stale, future, and late acknowledgements do not advance the applied revision or alter effective placement settings.
- Built-in backup/checkpoint automation is not implemented.
- Queue depth, cron backlog/lag, collection ingestion/state/items/errors/latency, node-health transitions, settings sync, management request latency/status class, capacity, pending results, daily verdict, and telemetry coverage have dedicated reconciled metrics.
- OTLP HTTP/protobuf, header/credential files, custom TLS files, per-signal endpoints, safe per-signal status, and durable state-log replay are startup-supported. Atomic runtime telemetry reconfiguration is not implemented.
- Readiness covers SQLite only.
- Built-in direct HTTP artifact fetching has no production header configuration. Use a named connector when authentication is required.
- Connector health/discovery and automatic connector-to-file fallback are not implemented.
- There is no workflow DAG, container executor, artifact distribution, or schedule deletion.
- Absolute paths are allowlisted; relative command names use the agent account's `PATH`.
- Windows labels advertise Excel capability by build target and do not probe the installation.
- Office automation can hang and is not supported by Microsoft for unattended service execution; this project requires an interactive logged-in user and still cannot make arbitrary macros safe.
