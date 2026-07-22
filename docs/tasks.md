# Tasks, schedules, and triggers

A blueprint describes how to execute work and validates its parameters. A parameter document supplies reusable base values. A schedule binds references to both artifacts, placement labels, and optional cron/webhook triggers.

When a schedule is created or updated, the coordinator fetches the blueprint and base parameters, parses them, and encrypts a resolved snapshot. It does not refetch those artifacts for every trigger. An ordinary trigger deep-merges overrides, validates/renders, and queues one run. A collection trigger fetches its configured collection at trigger time and creates a batch of child runs. Agent-local bindings are resolved only on the selected node.

## Artifact locations

A schedule can reference:

- `file:///absolute/path`: read by the coordinator, constrained by `SCHEDULER_ARTIFACT_ROOTS`.
- `https://host/path` or `http://host/path`: fetched directly by the coordinator with five-second connect and 20-second total timeouts and at most three redirects. The built-in HTTP adapter has no bootstrap authentication setting.
- `connector://name/resource?query`: sent to a configured named connector sidecar. See [Custom connectors](connectors.md).

Blueprint artifacts are limited to 1 MiB and may be YAML or JSON. Parameter artifacts are limited to 4 MiB and must be JSON objects. A `file://` path points to the coordinator filesystem, not the eventual agent filesystem. Paths inside an executor blueprint, such as `workbook_path`, are evaluated on the selected agent.

Connector failures never silently fall back to a file. Select a `file://` reference explicitly when a local artifact is the intended source.

## Schedule document

The formal schema is [`schemas/schedule-v1.schema.json`](../schemas/schedule-v1.schema.json). `taskctl schedules create --spec` accepts JSON or YAML; the REST API accepts JSON.

```yaml
name: Echo every weekday
blueprint_ref:
  uri: file:///srv/task-scheduler/artifacts/echo.yaml
parameters_ref:
  uri: file:///srv/task-scheduler/artifacts/echo.json
required_labels:
  pool: general
cron:
  expression: "0 0 9 * * Mon-Fri"
  timezone: Europe/Vienna
webhook_enabled: true
enabled: true
```

| Field | Required/default | Meaning |
| --- | --- | --- |
| `name` | Required, nonempty | Operator-visible schedule name. |
| `blueprint_ref.uri` | Required | Blueprint artifact URI. |
| `parameters_ref.uri` | Required | Base parameter artifact URI. |
| `parameter_collection` | `null` | Optional per-trigger collection source and ingestion/concurrency limits. When present, a trigger creates a batch rather than one run. |
| `required_labels` | `{}` | Additional exact-match placement labels merged over the blueprint labels. Schedule values win on duplicate keys. |
| `cron` | `null` | `{expression, timezone}` repeated trigger. |
| `webhook_enabled` | `false` | Whether the schedule has an authenticated public webhook. |
| `enabled` | `true` | Paused schedules do not create or accept new runs. |

Creating or updating a schedule fails if either schedule-time artifact cannot be fetched, the blueprint is invalid, or the cron/timezone is invalid. Base parameters are validated immediately for ordinary schedules. Collection schedules and blueprints with late agent bindings defer final validation until collection-item merge or agent binding resolution respectively. Updates increment the schedule revision. Already queued/running runs and batches retain their old execution snapshots.

Copyable examples:

- [`examples/schedules/echo.example.yaml`](../examples/schedules/echo.example.yaml)
- [`examples/schedules/echo.example.json`](../examples/schedules/echo.example.json)
- [`examples/schedules/monthly-collection.example.yaml`](../examples/schedules/monthly-collection.example.yaml)
- [`examples/schedules/process-id-secure-odata.example.yaml`](../examples/schedules/process-id-secure-odata.example.yaml)

## Blueprint document

The formal schema is [`schemas/blueprint-v1.schema.json`](../schemas/blueprint-v1.schema.json).

Version 1 rejects unknown properties in schedule and blueprint documents, including their typed nested objects: artifact references, collection and cron specifications, executor variants, execution policy, and parameter-binding declarations. This catches misspelled configuration keys at deserialization as well as schema validation. Deliberately open maps such as labels, command environment entries, and the blueprint's JSON Schema remain maps whose member names are user-defined. Connector bootstrap documents are strict as well.

```yaml
api_version: scheduler/v1

executor:
  kind: command
  program: /opt/company-tasks/import-customer
  args:
    - --customer-id
    - "{{params.customer.id}}"
    - --mode={{params.mode}}
  env:
    IMPORT_REGION: "{{params.region}}"
  working_directory: /opt/company-tasks

parameters_schema:
  $schema: https://json-schema.org/draft/2020-12/schema
  type: object
  additionalProperties: false
  required: [customer, mode, region]
  properties:
    customer:
      type: object
      additionalProperties: false
      required: [id]
      properties:
        id:
          type: string
          minLength: 1
    mode:
      enum: [preview, apply]
    region:
      type: string

required_labels:
  pool: general

policy:
  max_attempts: 3
  timeout_seconds: 3600
  initial_backoff_seconds: 5
  backoff_cap_seconds: 300
```

Top-level fields:

| Field | Required/default | Meaning |
| --- | --- | --- |
| `api_version` | Required | Must be exactly `scheduler/v1`. |
| `executor` | Required | A `command` or `excel_macro` executor. |
| `parameters_schema` | `{"type":"object"}` | JSON Schema used for base values and every trigger-specific merged document. |
| `parameter_bindings` | `{}` | Agent-local environment or logical secret-file values resolved just before execution. |
| `required_labels` | `{}` | Exact-match placement labels. |
| `policy` | Defaults below | Attempt, timeout, and retry policy. |

Policy fields:

| Field | Default | Rules |
| --- | --- | --- |
| `max_attempts` | Current global `default_max_attempts`, initially 3 | At least 1. Counts durably accepted attempts; transport failures before acceptance do not consume it. |
| `timeout_seconds` | Current global `default_timeout_seconds`, initially 3600 | At least 1. Enforced by `task-executor`. |
| `initial_backoff_seconds` | 5 | Base delay before retry. |
| `backoff_cap_seconds` | 300 | Maximum exponential base delay. |

The retry delay doubles with the accepted attempt count, is capped, and receives up to 20 percent positive jitter. Any non-successful accepted attempt is retried until the snapshot's `max_attempts` is exhausted. This includes a macro return value of `1`, timeouts, lease expiry, and retryable infrastructure failures. Cancellation prevents retries.

## Parameter schema and documents

Parameters use JSON Schema 2020-12. Include `additionalProperties: false` when unexpected input should fail rather than be ignored. For values mapped to VBA `Long`, constrain the range explicitly:

```yaml
parameters_schema:
  $schema: https://json-schema.org/draft/2020-12/schema
  type: object
  additionalProperties: false
  required: [id, enabled]
  properties:
    id:
      type: integer
      minimum: -2147483648
      maximum: 2147483647
    enabled:
      type: boolean
```

The parameter artifact is JSON:

```json
{
  "id": 42,
  "enabled": true
}
```

Manual and webhook overrides must also be JSON objects. Merge behavior is recursive for objects; arrays, scalars, and null replace the saved value at that key. The final merged document is validated before a run is committed.

Mark stored values which must be hidden in the management UI with the JSON
Schema annotation `writeOnly: true`. The coordinator still encrypts the entire
execution snapshot at rest. Run and batch detail pages show the exact captured
parameter document, but replace write-only values with a redaction marker by
default. An authenticated administrator can explicitly enable debug mode on a
detail page to reveal stored values; those responses use
`Cache-Control: no-store`. For backward compatibility, common secret-bearing
property names
such as `password`, `secret`, `token`, `credential`, and `api_key` are also
redacted conservatively. Prefer the explicit schema annotation for new
blueprints.

Agent-local `parameter_bindings` are different: their values are resolved only
on the selected agent and are never returned to or persisted by the
coordinator. The detail page lists those parameters as agent-local, but even
debug mode cannot reveal a value the coordinator never received.

Validation errors returned to operators deliberately identify schema keywords and schema paths without including runtime parameter values or their object keys.

## Parameter collections and batches

Adding `parameter_collection` keeps the one-blueprint/one-schedule model but expands every cron, manual, future, or webhook trigger into one durable batch and one run per valid provider key:

```yaml
parameter_collection:
  source_ref:
    uri: connector://reporting/daily-workbooks
  page_size: 500
  max_items: 10000
  max_active_runs: 32
  poison_distinct_nodes: 2
```

Defaults and limits are:

| Field | Default | Valid range | Effect |
| --- | ---: | ---: | --- |
| `page_size` | 500 | 1–1,000 | Maximum items requested/accepted per connector page; static sources are sliced into pages of this size. |
| `max_items` | 10,000 | 1–10,000 | Per-batch hard limit. A non-final page that reaches the limit fails closed. |
| `max_active_runs` | 32 | 1–1,000 | Maximum concurrently active child runs for this batch. |
| `poison_distinct_nodes` | 2 | 2–32 | Healthy nodes which must reproduce the same ambiguous failure family before the input is confirmed poisoned. |

Each collection item is an object with a stable, nonempty provider key of at most 256 bytes and a parameter object:

```json
{
  "key": "customer-42-monthly",
  "parameters": {
    "workbook_path": "C:\\Reports\\Customer42.xlsm",
    "month": "2026-07"
  }
}
```

Final precedence is `base parameters < collection item parameters < manual/webhook overrides`. Objects deep-merge; arrays, scalars, and null replace. One override therefore applies to every item in that trigger. The final nonsecret parameter document is schema-validated and rendered independently for each item.

`file://` and unauthenticated `http(s)://` collection sources accept any of:

```json
[
  {"key":"customer-42","parameters":{"customer_id":42}},
  {"key":"customer-84","parameters":{"customer_id":84}}
]
```

```json
{
  "api_version":"scheduler/parameter-collection/v1",
  "snapshot_id":"operator-metadata-only",
  "items":[{"key":"customer-42","parameters":{"customer_id":42}}]
}
```

```text
{"key":"customer-42","parameters":{"customer_id":42}}
{"key":"customer-84","parameters":{"customer_id":84}}
```

For static sources, the coordinator hashes the exact file/response bytes for immutable snapshot identity; an envelope `snapshot_id` does not replace that hash. Bodies are capped at 16 MiB. File sources must resolve beneath a coordinator artifact root. Direct HTTP uses GET, no authentication or redirects, a five-second connect timeout, and a 20-second total timeout. Use an authenticated `connector://` collection for production APIs; its paginated protocol is documented in [Custom connectors](connectors.md#parameter-collection-page-protocol).

The coordinator first persists a `scheduled` batch, then claims a 60-second ingestion lease, fetches and transactionally commits pages, and finalizes only after the entire source passes snapshot/cursor/limit consistency checks. Coordinator restart safely replays committed page/cursor state. No child item is dispatchable before finalization. Batch states are `scheduled`, `collecting`, `running`, `succeeded`, `completed_with_errors`, `failed`, and `cancelled`; item states are `ready`, `queued`, `running`, `succeeded`, `failed`, `cancelled`, `invalid`, `suspected_poison`, `poisoned`, and `held`.

Malformed item shapes, missing/invalid keys, invalid parameters, merge/schema/render failure, and known poisoned fingerprints receive safe item failure codes while valid siblings continue. Missing keys cannot be correlated across retries, and conflicting duplicate provider keys fail the batch source consistency check. Provider keys are encrypted at rest and available only in an authenticated batch view; they are excluded from logs, telemetry, audit metadata, and fingerprints.

Trigger and inspect a collection schedule:

```sh
taskctl schedules run <schedule-id> --parameters overrides.json
taskctl batches list --limit 50
taskctl batches show <batch-id>
taskctl batches items <batch-id> --limit 50
taskctl batches cancel <batch-id>
taskctl batches retrigger <batch-id>
```

Retrigger preserves the original immutable schedule/blueprint revision, collection reference, limits, and trigger overrides, but fetches a new collection snapshot. Cancellation durably stops ingestion/finalization and cancels already queued or running child runs. There is no per-item retry command; use ordinary run retry for a terminal child run or retrigger the batch.

## Agent-local environment and secret-file bindings

Bindings let a blueprint declare parameter names whose values exist only on the selected node:

```yaml
parameter_bindings:
  bwp_user:
    source: environment
    name: BWP_USER
    value_type: string
    sensitive: true

  bwp_password:
    source: secret_file
    name: bwp-password
    value_type: string
    sensitive: true
```

`source` is `environment` or `secret_file`; `value_type` is `string` (default), `integer`, `number`, `boolean`, or `json`; `sensitive` defaults to `true`. Binding keys become fields in the final parameter object and must be declared by `parameters_schema` when `additionalProperties: false` is used. Agent-advertised binding availability participates in placement, but only the source/name pair is advertised—never a value.

The agent must explicitly allow an environment name through `SCHEDULER_AGENT_ALLOWED_ENV_BINDINGS`. A secret-file `name` is a logical basename, never a path, and is searched beneath the configured absolute `SCHEDULER_AGENT_SECRET_ROOTS`. Traversal, absolute names, symlink escape, non-regular files, missing/unreadable values, invalid decoding, and values above `SCHEDULER_AGENT_BINDING_MAX_BYTES` are rejected before task acceptance/start with `parameter_binding_failed`. Available files are discovered by logical filename; do not use the same logical name in multiple roots.

Preflight rejection is a structured control-plane response, not an agent-stream failure. `parameter_binding_failed` means binding resolution/decoding/schema rendering failed; `assignment_policy_rejected` means the now-resolved assignment violates the node's current execution policy. In either case the coordinator finishes the offered attempt with an operator-safe diagnostic and `attempt.rejected_before_acceptance` audit event, releases the node slot, and returns the run to the queue. That rejecting node remains excluded for this run; the run waits when no different eligible node exists instead of entering an unbounded offer/rejection loop. The agent remains connected, no task process starts, and the run's accepted-attempt count and retry budget are unchanged. The rejected offered-attempt record remains visible in attempt history so operators can determine which node and preflight stage failed.

Resolution is performed in memory before durable acceptance and checked again immediately before launch. The resolved assignment is sent to `task-executor` over stdin, not command-line arguments. The coordinator snapshot and agent ledger retain the declaration, not resolved values. The final bound document is schema-validated and rendered on the agent.

Sensitive command bindings may be referenced only from `executor.env` values. Referencing one from a program, argument, working directory, or another command field is rejected; there is no unsafe-argument override in version 1. Excel macro argument expressions may receive sensitive bindings in memory, and the COM host suppresses its payload. Non-sensitive bindings can be used anywhere normal parameter templates are accepted.

Example command use:

```yaml
executor:
  kind: command
  program: /opt/company-tasks/export
  args: ["--customer", "{{params.customer_id}}"]
  env:
    BWP_USER: "{{params.bwp_user}}"
    BWP_PASSWORD: "{{params.bwp_password}}"
```

When any binding is sensitive, the agent clears stdout, stderr, and free-form error/diagnostic text before task-output logging, local SQLite outbox persistence, or coordinator transmission. It preserves only byte counts, truncation flags, exit/signal/status fields, and stable diagnostic classifications. Treat `sensitive: false` as an explicit disclosure decision: the scheduler still avoids parameter-value logging, but the target program and chosen output mode can expose what it receives. Binding names and secret roots are bootstrap security policy and cannot be changed through synchronized node settings.

Complete secure Excel examples, with all 17 `processID` arguments but without credentials in the parameter JSON:

- [`examples/blueprints/process-id-secure.yaml`](../examples/blueprints/process-id-secure.yaml)
- [`examples/parameters/process-id-secure.json`](../examples/parameters/process-id-secure.json)
- [`examples/connectors/process-id-secure-odata`](../examples/connectors/process-id-secure-odata) (Rust OData adapter for the `daily` parameter group)

## Parameter templates

Executor string fields can contain `{{params.path.to.value}}` expressions.

- An embedded expression renders strings, numbers, booleans, or null into a string.
- A missing path fails run creation.
- Arrays and objects cannot be embedded in a string.
- For Excel argument entries only, a string consisting entirely of one expression preserves the JSON scalar type.
- Text that does not contain `{{params.` is literal, including unrelated double braces.

Example command arguments:

```yaml
args:
  - "{{params.input_path}}"
  - "--batch={{params.batch_number}}"
```

If `batch_number` is `42`, the command receives `--batch=42`. Arguments are passed directly to the program; no shell parses quotes, substitutions, pipes, or semicolons.

## Command executor

```yaml
executor:
  kind: command
  program: /opt/company-tasks/worker
  args:
    - --input
    - "{{params.input}}"
  env:
    MODE: "{{params.mode}}"
  working_directory: /opt/company-tasks
```

| Field | Required/default | Meaning |
| --- | --- | --- |
| `kind` | Required | `command`. |
| `program` | Required, nonempty | Executable path or a name resolved through the agent account's `PATH`. |
| `args` | `[]` | Structured argument list. |
| `env` | `{}` | Additional environment entries. |
| `working_directory` | Inherited | Optional working directory. Prefer an explicit absolute path. |

The executor sets `TASK_RUN_ID` and `TASK_ATTEMPT_ID`, overriding conflicting entries in the blueprint environment. Absolute program and working-directory paths must canonicalize under an applied `allowed_command_roots` entry. Relative program names resolved through `PATH` are not root-checked.

The task runs in its own Unix process group or Windows Job Object. Timeout, cancellation, or lease loss first attempts a graceful stop and then terminates the process tree. Stdout and stderr are bounded to 1 MiB each; truncation and byte counts appear in attempt metadata.

## Excel macro executor

Excel blueprints are Windows-only:

```yaml
executor:
  kind: excel_macro
  workbook_path: "C:\\TaskWorkbooks\\Process.xlsm"
  module_name: ProcessModule
  macro_name: processID
  args:
    - "{{params.id}}"
    - "{{params.workbook_name}}"
    - "{{params.recipients}}"
    - "{{params.selection_variant}}"
    - "{{params.responsible}}"
    - "{{params.subject}}"
    - "{{params.body}}"
    - "{{params.pdf}}"
    - "{{params.mailfilter}}"
    - "{{params.query1}}"
    - "{{params.query2}}"
    - "{{params.query3}}"
    - "{{params.query4}}"
    - "{{params.query5}}"
    - "{{params.info}}"
    - "{{params.bwp_user}}"
    - "{{params.bwp_password}}"
  read_only: true
  save_changes: false
  visible: false
```

| Field | Required/default | Meaning |
| --- | --- | --- |
| `kind` | Required | `excel_macro`. |
| `workbook_path` | Required | Absolute preinstalled `.xlsm` or `.xlam` path under an applied workbook root. |
| `module_name` | Unset | Preferred unqualified standard VBA module name, such as `ProcessModule`. Must not contain `.` or `!`. |
| `macro_name` | Required | Public procedure name. It must be unqualified when `module_name` is set. |
| `args` | `[]` | Up to 30 positional JSON scalars or parameter expressions. |
| `read_only` | `true` | Passed to `Workbooks.Open`. |
| `save_changes` | `false` | Passed when the workbook is closed. |
| `visible` | `false` | Visibility of the private Excel instance. This does not make service automation supported. |

The exact requested VBA function is supported:

```vb
Function processID(ByVal id As Long, workbookName As String, recipients As String, selectionVariant As String, responsible As String, subject As String, body As String, pdf As Boolean, mailfilter As Boolean, Optional query1 As String = "", Optional query2 As String = "", Optional query3 As String = "", Optional query4 As String = "", Optional query5 As String = "", Optional info As Boolean = False, Optional bwpUser As String = "", Optional bwpPassword As String = "") As Integer
```

Its 17 arguments are passed in the order shown above. The complete tested files are:

- [`examples/blueprints/process-id.yaml`](../examples/blueprints/process-id.yaml)
- [`examples/parameters/process-id.json`](../examples/parameters/process-id.json)

`processID` must be a public Function in the standard VBA module `ProcessModule`, not a worksheet, workbook, class, or private module procedure. The executor constructs the fully qualified target from the name of the workbook it actually opened plus `module_name` and `macro_name`; the blueprint does not hard-code a workbook-qualified macro reference.

For backward compatibility, omitting `module_name` keeps legacy blueprints such as `macro_name: ProcessModule.processID` working. New blueprints should use the separate fields because validation can then reject ambiguous/qualified module input before Excel starts.

JSON values map deterministically to COM variants: booleans to Boolean, strings to String, 32-bit integers to Int32, larger signed/unsigned integers to their 64-bit types, non-integers to Double, and null to `DBNull`. Arrays and objects are rejected. `Application.Run` is positional: omit only trailing optional values. To supply a later optional value, include explicit values such as `""` or `false` for every preceding position. JSON null is not the same as omitting a VBA optional argument.

The executor opens a private Excel COM instance, qualifies the macro with the actual opened workbook name, invokes it, and interprets integer `0` as success and integer `1` as task failure. Other values/types, VBA/COM exceptions, a crashed Excel process, isolation failure, and cleanup failure are classified separately.

Before invocation it creates hidden workbook-scoped names `TASK_RUN_ID` and `TASK_ATTEMPT_ID`. VBA can use them for idempotency:

```vb
Dim runId As String
runId = CStr(Evaluate("TASK_RUN_ID"))
```

Existing names with either reserved identifier cause a safe failure. The executor deletes the temporary names, closes the workbook, quits the private instance, and releases COM objects. A watchdog kills a hung instance. Never run this automation from a Windows service, never attach it to a user's existing Excel process, and never allow macros to display dialogs.

## Create and edit schedules

From the UI, open `/schedules`, choose **New schedule**, enter the artifact URIs and optional cron/labels, and save. Artifact resolution and schema validation happen before the new schedule is committed. Editing repeats resolution and increments the revision.

CLI:

```sh
export SCHEDULER_URL=https://scheduler.example.com:8443
export SCHEDULER_ADMIN_TOKEN='<admin-token>'

taskctl schedules create --spec examples/schedules/echo.example.yaml
taskctl schedules list
taskctl schedules show <schedule-id>
```

There is currently no schedule-delete endpoint. Pause obsolete schedules instead.

## Manual and future runs

Run with saved parameters:

```sh
taskctl schedules run <schedule-id>
```

Merge a JSON override:

```sh
taskctl schedules run <schedule-id> --parameters override.json
```

Queue for a future UTC instant and make client retries idempotent:

```sh
taskctl schedules run <schedule-id> \
  --run-at 2026-08-01T08:00:00Z \
  --idempotency-key customer-import-2026-08-01
```

The initial response is `202 Accepted`. Repeating the same idempotency key for that schedule returns the existing run or batch with `200 OK`. A collection schedule returns `{ "kind":"batch", "batch": ... }`; inspect it with `taskctl batches show`.

## Cron triggers

Cron expressions have seconds and an optional year:

```text
second minute hour day-of-month month day-of-week [year]
```

Examples:

```text
0 */15 * * * *          every 15 minutes
0 0 9 * * Mon-Fri      09:00 on weekdays
0 30 2 * * *            02:30 every local day
```

Use an IANA timezone such as `UTC`, `Europe/Vienna`, or `America/New_York`. The UI previews the next five UTC occurrences. Nonexistent local times in a spring-forward gap are skipped. Both absolute instants of an ambiguous fall-back time are queued.

The coordinator persists every occurrence under a unique `(schedule_id, scheduled_at)` key and does not coalesce overlapping occurrences. When a schedule remains enabled, occurrences missed during coordinator downtime are caught up individually. It scans at most 1,000 next occurrences per schedule per one-second pass, so a large outage backlog drains in batches. A deliberate pause is different: resuming advances the schedule's materialization cursor to the resume time, so cron occurrences missed while paused are skipped instead of caught up. The first new cron occurrence is strictly after the resume time. Repeating resume on an already enabled schedule is idempotent and does not advance the cursor again. Editing the cron expression or timezone resets the materialization cursor at edit time; other edits preserve it. Runs and collection batches already committed before the pause keep their previous snapshots and are not cancelled.

Pause and resume:

```sh
taskctl schedules pause <schedule-id>
taskctl schedules resume <schedule-id>
```

## Webhook triggers

Set `webhook_enabled: true` when creating the schedule. The create response/UI shows a public ID and secret once. Store the secret immediately.

```sh
curl -i \
  -H "Authorization: Bearer $WEBHOOK_SECRET" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: source-event-123' \
  -d '{"query1":"SELECT * FROM CurrentData","info":true}' \
  "https://scheduler.example.com:8443/hooks/v1/$PUBLIC_ID"
```

The body is the parameter override object. It is deep-merged and validated before an ordinary run or collection batch is created. Initial creation returns `202`; repeating the key returns the existing run or batch. A missing/incorrect secret returns `401`; a disabled schedule or old/rotated public ID is not found.

Rotate both the public ID and secret with:

```sh
taskctl schedules rotate-webhook <schedule-id>
```

The old URL and secret stop working. Editing the schedule with `webhook_enabled: false` disables the endpoint without deleting run history.

## Inspect, cancel, and retry

```sh
taskctl runs list --limit 100
taskctl runs show <run-id>
taskctl runs cancel <run-id>
taskctl runs retry <run-id>
```

Cancellation durably marks a queued/running run first, prevents further retries, and asks the agent to terminate an active process tree. Only failed terminal runs can be manually retried. Manual retry resets the retry budget while preserving attempt history and continuing attempt numbering.
