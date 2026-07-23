# HTTP API

The management API and UI share the coordinator REST listener. Unless stated otherwise, endpoints require the single administrator bearer token:

```http
Authorization: Bearer <SCHEDULER_ADMIN_TOKEN>
```

Examples use:

```sh
export SCHEDULER_URL=https://scheduler.example.com:8443
export SCHEDULER_ADMIN_TOKEN='<admin-token>'
```

JSON request bodies require `Content-Type: application/json`.

## Health

No authentication is required.

```text
GET /health/live
GET /health/ready
```

Success is `204 No Content`. Readiness currently checks SQLite only.

### Telemetry exporter status

```text
GET /api/v1/telemetry/status
```

This endpoint requires administrator authentication and reports the coordinator process's safe exporter status:

```json
{
  "configured": true,
  "protocol": "http/protobuf",
  "last_success_unix_ms": 1784606400123,
  "last_error_class": null,
  "failed_signals": [],
  "signals": [
    {
      "signal": "traces",
      "last_success_unix_ms": 1784606400123,
      "failed": false,
      "failed_items": 0
    },
    {
      "signal": "metrics",
      "last_success_unix_ms": 1784606400123,
      "failed": false,
      "failed_items": 0
    },
    {
      "signal": "logs",
      "last_success_unix_ms": 1784606400123,
      "failed": false,
      "failed_items": 0
    }
  ],
  "dropped_telemetry": 0,
  "export_batches_in_flight": 0,
  "span_queue_capacity": 2048,
  "log_queue_capacity": 2048,
  "authoritative_state": {
    "last_snapshot_at": "2026-07-23T08:00:00Z",
    "coverage_gap": false,
    "coverage_gap_reason": null,
    "outbox_depth": 0,
    "outbox_oldest_event_at": null,
    "outbox_delivered_events": 4812,
    "outbox_expired_events": 0,
    "current_day_verdict": "green",
    "previous_day_verdict": "green"
  }
}
```

`protocol` can be `grpc`, `http/protobuf`, `mixed`, or `disabled`. Signal entries provide independent freshness and cumulative failed-item counts. `authoritative_state` exposes the durable log outbox, irreversible coverage loss, last reconciled snapshot, and the shared current/previous daily verdict. The response never includes endpoints, headers, credentials, certificates, keys, or secret-file paths. Exporter signal status covers the coordinator process; agents project their ledger/outbox state with agent resource identity.

## Error envelope

API errors are JSON:

```json
{
  "error": "operator-readable message",
  "code": "invalid_request",
  "status": 400
}
```

Stable envelope codes include:

| HTTP status | Code | Meaning |
| --- | --- | --- |
| 400 | `invalid_request` | Malformed request, invalid cron/settings, wrong run state, or another validation error. |
| 400 | `artifact_resolution_failed` | Blueprint/parameter fetch or blueprint parsing failed. |
| 400 | `connector_not_configured` | A `connector://` reference names no configured connector. |
| 400 | `invalid_artifact_reference` | An artifact URI is malformed, uses an unsupported scheme, or contains an invalid connector reference/encoding. |
| 400 | `connector_kind_not_allowed` | The configured connector does not permit the requested blueprint or parameter kind. |
| 401 | `authentication_required` | Missing or incorrect bearer token/secret. |
| 404 | `resource_not_found` | Unknown or deliberately unavailable resource. |
| 409 | `conflict` | Revision conflict or active edit lock. |
| 412 | `precondition_failed` | `If-Match` is invalid or differs from `expected_revision`. |
| 413 | `artifact_too_large` | A file, HTTP artifact, or connector response exceeds the configured byte limit. |
| 422 | `parameter_validation_failed` | Final parameters failed the blueprint JSON Schema. |
| 428 | `precondition_required` | Settings update omitted `If-Match`. |
| 502 | `connector_transport_failed` | The coordinator could not connect to or read from the connector. |
| 502 | `connector_upstream_failed` | The connector returned a non-success HTTP status. |
| 502 | `connector_invalid_response` | The connector response violated the version, content-type, or body protocol. |
| 504 | `connector_timeout` | Configured connector request timed out. |

Do not parse the free-form `error` text for automation; use `code` and HTTP status.

## Cursor pagination

The schedule, run, attempt, event, agent, health-evidence, and blueprint REST list endpoints return a page object rather than a bare array:

```json
{
  "items": [],
  "next_cursor": "endpoint-specific-opaque-value"
}
```

The `limit` query parameter defaults to 50 and is clamped to 1–200. When `next_cursor` is present, pass it unchanged as the next request's `cursor`; it is omitted on the final page. Cursors are URL-safe, versioned, and bound to the originating list kind. They must not be decoded, modified, or reused with another endpoint. A legacy/unknown version, cross-endpoint cursor, malformed timestamp, or invalid entity identifier returns `400 invalid_request`. Stable order is:

| List | Order |
| --- | --- |
| Schedules | `created_at` descending, then schedule ID descending |
| Runs | `created_at` descending, then run ID descending |
| Attempts for one run | attempt number ascending, then attempt ID ascending |
| Audit events for one run | event ID descending |
| Agents | agent ID ascending |
| Health evidence for one agent | `occurred_at` descending, then evidence ID descending |
| Blueprint revisions | `loaded_at` descending, then blueprint digest descending |

Batch and batch-item lists use the same outer response envelope and limits but retain their own opaque cursor format; their ordering is documented below. `taskctl` prints the response page, including `items` and any `next_cursor`, instead of unwrapping it. The current CLI does not automatically fetch subsequent pages; use the REST API with the returned cursor when a full traversal is required.

## Schedules

### List schedules

```sh
curl -sS "$SCHEDULER_URL/api/v1/schedules" \
  -H "Authorization: Bearer $SCHEDULER_ADMIN_TOKEN"
```

```text
GET /api/v1/schedules?limit=50&cursor=<opaque>
```

Returns a cursor page of schedule views. Views contain `id`, `spec`, `revision`, timestamps, and optional `webhook_public_id`. Secrets and resolved parameter values are never returned.

### Create a schedule

```text
POST /api/v1/schedules
```

```sh
curl -sS "$SCHEDULER_URL/api/v1/schedules" \
  -H "Authorization: Bearer $SCHEDULER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d @examples/schedules/echo.example.json
```

Success is `201 Created`:

```json
{
  "schedule": {
    "id": "...",
    "spec": {},
    "revision": 1,
    "created_at": "...",
    "updated_at": "...",
    "webhook_public_id": "..."
  },
  "webhook_secret": "shown-only-on-create"
}
```

`webhook_secret` is omitted when the webhook is disabled. Copy it immediately.

### Get and update a schedule

```text
GET /api/v1/schedules/{schedule_id}
PUT /api/v1/schedules/{schedule_id}
```

Update body:

```json
{
  "expected_revision": 3,
  "spec": {
    "name": "Echo example",
    "blueprint_ref": {"uri": "file:///srv/task-scheduler/artifacts/echo.yaml"},
    "parameters_ref": {"uri": "file:///srv/task-scheduler/artifacts/echo.json"},
    "required_labels": {"pool": "general"},
    "cron": {"expression": "0 0 9 * * Mon-Fri", "timezone": "Europe/Vienna"},
    "webhook_enabled": true,
    "enabled": true
  }
}
```

The coordinator fetches and validates both artifacts before committing the revision. This schedule update does not use the settings-lock protocol.

### Preview occurrences

```text
GET /api/v1/schedules/{schedule_id}/occurrences
```

Returns the next five UTC instants for the schedule's stored cron expression:

```json
{"occurrences":["2026-07-22T07:00:00Z"]}
```

A schedule without cron returns an error.

### Pause and resume

```text
POST /api/v1/schedules/{schedule_id}/pause
POST /api/v1/schedules/{schedule_id}/resume
```

Success is `204 No Content`. Pausing increments the schedule revision and prevents cron/manual/webhook run creation; it does not cancel already queued/running runs or collection batches. Resuming advances the cron cursor to the resume time, so occurrences missed during the paused interval are not created. The next cron occurrence is strictly after resume. Repeating pause or resume when the schedule is already in that state is an idempotent no-op and does not increment the revision or move the cursor. Schedules that remain enabled still catch up occurrences missed during coordinator downtime.

### Rotate a webhook

```text
POST /api/v1/schedules/{schedule_id}/webhook/rotate
```

```json
{"public_id":"new-public-id","secret":"new-one-time-secret"}
```

Rotation enables the webhook, changes both values, increments the schedule revision, and immediately invalidates the old endpoint credentials.

## Manual and future runs

```text
POST /api/v1/schedules/{schedule_id}/runs
```

The JSON body is required, but both fields are optional; send `{}` for an immediate run with saved parameters:

```json
{
  "parameters": {"name": "manual invocation"},
  "run_at": "2026-08-01T08:00:00Z"
}
```

Omit or set `parameters` to null for no overrides. It must otherwise be an object. `run_at` is RFC 3339 UTC/offset time; omitted means now.

```sh
curl -sS "$SCHEDULER_URL/api/v1/schedules/$SCHEDULE_ID/runs" \
  -H "Authorization: Bearer $SCHEDULER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: source-operation-123' \
  -d '{"parameters":{"name":"manual invocation"}}'
```

For an ordinary schedule, initial creation returns `202 Accepted` with the run view. For a collection schedule it returns a discriminated receipt:

```json
{
  "kind": "batch",
  "batch": {
    "id": "...",
    "schedule_id": "...",
    "schedule_revision": 4,
    "state": "scheduled",
    "item_count": 0
  }
}
```

A repeated idempotency key for the same schedule returns the existing run or batch with `200 OK`. Idempotency keys are shared across manual and webhook triggers for a schedule; use a namespaced value. Collection overrides are merged into every item after base and item parameters.

## Public webhook

```text
POST /hooks/v1/{public_schedule_id}
```

This endpoint uses the one-time schedule webhook secret, not the administrator token:

```sh
curl -sS "$SCHEDULER_URL/hooks/v1/$PUBLIC_ID" \
  -H "Authorization: Bearer $WEBHOOK_SECRET" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: webhook-event-123' \
  -d '{"name":"webhook invocation"}'
```

The body must be an override object. It is deeply merged over base parameters for an ordinary schedule or over every collection item. Initial success is `202`; an idempotent replay is `200`. Collection webhooks return the same discriminated batch receipt shown above. Disabled/paused/rotated schedules are deliberately unavailable.

## Parameter-collection batches

### List and inspect batches

```text
GET /api/v1/batches?limit=50&cursor=<opaque>
GET /api/v1/batches/{batch_id}
GET /api/v1/batches/{batch_id}/items?limit=50&cursor=<opaque>
GET /api/v1/batches/{batch_id}/items?provider_key=<exact-key>
```

Batch and item list pages default to 50 rows and clamp `limit` to 1–200. `next_cursor` is URL-safe opaque state; pass it back unchanged and do not parse or persist assumptions about its contents:

```json
{
  "items": [
    {
      "id": "...",
      "schedule_id": "...",
      "state": "running",
      "item_count": 600,
      "valid_item_count": 598,
      "invalid_item_count": 2,
      "poisoned_item_count": 0,
      "held_item_count": 0,
      "failure_code": null
    }
  ],
  "next_cursor": "eyJjcmVhdGVkX2F0IjoiLi4uIn0"
}
```

Items are ordered by stable item index and ID. The authenticated item view includes decrypted `provider_key`, safe `failure_code`, state, parameter digest, and optional child `run_id`, but never parameter values. `provider_key` is an exact match only, at most 256 bytes, and intentionally scoped to one batch; there is no global provider-key search.

Batch states are `scheduled`, `collecting`, `running`, `succeeded`, `completed_with_errors`, `failed`, and `cancelled`. Item states are `ready`, `queued`, `running`, `succeeded`, `failed`, `cancelled`, `invalid`, `suspected_poison`, `poisoned`, and `held`.

### Cancel and retrigger

```text
POST /api/v1/batches/{batch_id}/cancel
POST /api/v1/batches/{batch_id}/retrigger
```

Cancel returns `202 Accepted`, stops later ingestion/finalization, and cancels queued/running child runs. Retrigger returns `202` with `{ "kind":"batch", "batch": ... }`; it preserves the original immutable schedule/blueprint revision, collection reference/limits, and overrides, then fetches the collection again as a new batch.

## Runs

### List and inspect

```text
GET /api/v1/runs?limit=50&cursor=<opaque>
GET /api/v1/runs/{run_id}
GET /api/v1/runs/{run_id}/attempts?limit=50&cursor=<opaque>
GET /api/v1/runs/{run_id}/events?limit=50&cursor=<opaque>
```

The runs, attempts, and events list calls each return the cursor-page envelope described above.

`/attempts` returns:

- attempt/run/node IDs and attempt number;
- state and outcome;
- accepted, created, and finished timestamps;
- duration, exit code, and Unix signal when applicable;
- bounded output byte/truncation metadata;
- structured failure diagnostic.

Raw stdout/stderr, encrypted result payloads, parameters, and environment values are not returned. `taskctl runs show <run-id>` combines the run object and the first attempts page as `{ "run": ..., "attempts": { "items": [...], "next_cursor": ... } }`.

`/events` returns persisted audit events with ID, event type, safe metadata, and occurrence time.

### Cancel and retry

```text
POST /api/v1/runs/{run_id}/cancel
POST /api/v1/runs/{run_id}/retry
```

Both return `202 Accepted`. Cancel works only for queued/running runs and durably marks the run before signaling agents. Retry works only for a terminal failed run.

## Agents

```text
GET /api/v1/agents?limit=50&cursor=<opaque>
```

Returns a cursor page containing node ID, hostname, labels, capacity/running count, connected state, desired/applied settings revisions, optional `settings_error`, and last-seen timestamp. `settings_error` contains the latest rejection of the current desired revision and is cleared by a new desired revision or a successful current acknowledgement. Node settings are a separate endpoint.

### Node and input health

```text
GET  /api/v1/agents/{agent_id}/health
GET  /api/v1/agents/{agent_id}/health/evidence?limit=50&cursor=<opaque>
POST /api/v1/agents/{agent_id}/health/quarantine
POST /api/v1/agents/{agent_id}/health/reset
POST /api/v1/input-health/{blueprint_digest}/{input_fingerprint}/probe
```

The health view includes `state`, `reason_code`, scoring counts/rate, revision, and transition/update timestamps. States are `healthy`, `suspect`, `auto_quarantined`, `manual_quarantined`, and `probation`. Evidence is cursor-paginated and contains safe classification/failure metadata plus flags indicating cluster suppression or poison retraction; it excludes parameters, provider keys, secret values, and task output.

Manual quarantine blocks new placement without terminating active work and returns the new health view. Reset is valid only for a quarantined node and enters capacity-one probation; it does not erase the audit trail. A probe path requires two exact 64-character hexadecimal digests and releases one audited attempt for a confirmed poisoned input. Repeated probe grants do not create an unbounded bypass.

CLI equivalents are `taskctl nodes health|evidence|quarantine|reset <agent-id>` and `taskctl input-health probe <blueprint-digest> <input-fingerprint>`.

## Blueprint catalog and dashboard

```text
GET /api/v1/blueprints?limit=50&cursor=<opaque>
GET /api/v1/dashboard
PUT /api/v1/dashboard
```

The blueprint catalog returns a cursor page of loaded revisions ordered newest first by load time and digest. Each item contains digest, query/fragment-redacted source reference, optional source version, load time, executor kind, required labels, execution policy, parameter schema, binding declarations, and current/retained schedule counts. It never contains resolved base/collection/environment/secret values. A catalog row is metadata about a loaded immutable revision, not a live refetch.

Dashboard reads include an `ETag` and a versioned `{revision, config}` document. Config fields are:

```json
{
  "schedule_ids": ["018f6f8c-6302-7fc5-a0cb-b4e421fd61af"],
  "widgets": [
    "cluster_capacity",
    "active_batches",
    "recent_failures",
    "quarantined_nodes",
    "connector_health",
    "telemetry_health",
    "selected_schedules"
  ]
}
```

At most 100 unique schedule IDs are allowed; order is display order and widgets must be unique. Update the dashboard through the same lock/revision protocol as settings, using document key `dashboard`, `If-Match`, `expected_revision`, and `lock_token`. The telemetry widget uses the safe live coordinator exporter status and durable outbox state. The dashboard's daily schedule table and cluster verdict use the same authoritative evaluator as the OTLP metric projection. The connector widget reports configuration/observed batch posture. See the [backend-neutral dashboard contract](observability.md#backend-neutral-dashboard-specification).

## Settings

### Read documents

```text
GET /api/v1/settings/global
GET /api/v1/settings/nodes/{agent_id}
```

The response body is the settings document and the response `ETag` is its quoted revision, for example:

```http
ETag: "4"
```

### Acquire, renew, and release a lock

Global document key is `global`; node keys are `node:<agent-id>`.

Acquire:

```text
POST /api/v1/settings/locks/{document_key}
```

```json
{"owner_session":"maintenance-script"}
```

Response:

```json
{
  "document_key": "global",
  "owner_session": "maintenance-script",
  "lock_token": "...",
  "expires_at": "..."
}
```

The lease lasts two minutes.

Renew:

```text
PUT /api/v1/settings/locks/{document_key}
```

```json
{"owner_session":"maintenance-script","lock_token":"..."}
```

Release normally:

```text
DELETE /api/v1/settings/locks/{document_key}
```

```json
{"owner_session":"maintenance-script","lock_token":"..."}
```

Administrative force release omits/ignores the token:

```json
{"owner_session":"administrator","force":true}
```

An existing force-released lock writes the safe audit event `settings.lock_force_released` for entity type `settings` and the document key. In the browser, lock contention renders the current global/node/dashboard document read-only with owner category and expiry, plus Retry and confirmed Force unlock actions; no competing lock token is exposed. Force release does not bypass the subsequent `If-Match`/revision check.

### Update a document

```text
PUT /api/v1/settings/global
PUT /api/v1/settings/nodes/{agent_id}
```

The request must carry the same revision in two places:

```http
If-Match: "4"
```

```json
{
  "expected_revision": 4,
  "lock_token": "...",
  "document": {
    "revision": 4,
    "default_timezone": "UTC",
    "default_max_attempts": 3,
    "default_timeout_seconds": 3600,
    "lease_seconds": 60,
    "heartbeat_seconds": 10,
    "audit_retention_days": 90,
    "otlp_endpoint": null
  }
}
```

Success returns the next revision and ETag:

```json
{"revision":5}
```

After a node update, the coordinator pushes the desired document when that node is connected. The settings write may succeed before agent acknowledgement. Compare desired/applied revisions and inspect `settings_error`: a rejection leaves the applied revision and effective placement settings unchanged, while a successful acknowledgement of the current revision clears the error and hot-applies that exact document.

## UI sessions

The browser UI uses `/login`, an in-memory 12-hour HttpOnly SameSite=Strict session cookie, and CSRF tokens on forms. `SCHEDULER_SECURE_COOKIES=true` adds `Secure`. UI sessions are lost when the coordinator restarts. These routes are intended for browsers and are not a second administrator API.
