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

## Schedules

### List schedules

```sh
curl -sS "$SCHEDULER_URL/api/v1/schedules" \
  -H "Authorization: Bearer $SCHEDULER_ADMIN_TOKEN"
```

```text
GET /api/v1/schedules
```

Returns an array of schedule views. Views contain `id`, `spec`, `revision`, timestamps, and optional `webhook_public_id`. Secrets and resolved parameter values are never returned.

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

Success is `204 No Content`. Pausing increments the schedule revision and prevents cron/manual/webhook run creation; it does not cancel already queued/running runs.

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

Initial creation returns `202 Accepted` with the run view. A repeated idempotency key for the same schedule returns that run with `200 OK`. Idempotency keys are shared across manual and webhook triggers for a schedule; use a namespaced value.

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

The body must be an override object. It is deeply merged over base parameters and validated. Initial success is `202`; an idempotent replay is `200`. Disabled/paused/rotated schedules are deliberately unavailable.

## Runs

### List and inspect

```text
GET /api/v1/runs?limit=100
GET /api/v1/runs/{run_id}
GET /api/v1/runs/{run_id}/attempts
GET /api/v1/runs/{run_id}/events
```

`limit` defaults to 100. The store caps audit event reads at 1,000; run events request the latest 500.

`/attempts` returns:

- attempt/run/node IDs and attempt number;
- state and outcome;
- accepted, created, and finished timestamps;
- duration, exit code, and Unix signal when applicable;
- bounded output byte/truncation metadata;
- structured failure diagnostic.

Raw stdout/stderr, encrypted result payloads, parameters, and environment values are not returned. `taskctl runs show <run-id>` combines the run and attempts endpoints.

`/events` returns persisted audit events with ID, event type, safe metadata, and occurrence time.

### Cancel and retry

```text
POST /api/v1/runs/{run_id}/cancel
POST /api/v1/runs/{run_id}/retry
```

Both return `202 Accepted`. Cancel works only for queued/running runs and durably marks the run before signaling agents. Retry works only for a terminal failed run.

## Agents

```text
GET /api/v1/agents
```

Returns node ID, hostname, labels, capacity/running count, connected state, desired/applied settings revisions, optional `settings_error`, and last-seen timestamp. `settings_error` contains the latest rejection of the current desired revision and is cleared by a new desired revision or a successful current acknowledgement. Node settings are a separate endpoint.

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
