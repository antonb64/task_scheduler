# HTTP API

Administrative endpoints require `Authorization: Bearer <admin-token>`.

## Schedules

- `GET /api/v1/schedules`
- `POST /api/v1/schedules` with a `ScheduleSpec`
- `GET|PUT /api/v1/schedules/{id}`; updates include `expected_revision` and `spec`
- `GET /api/v1/schedules/{id}/occurrences`
- `POST /api/v1/schedules/{id}/runs` with optional `parameters` and RFC 3339 `run_at`
- `POST /api/v1/schedules/{id}/pause|resume`
- `POST /api/v1/schedules/{id}/webhook/rotate`

Manual trigger requests may include `Idempotency-Key`.

## Webhooks

`POST /hooks/v1/{public_schedule_id}` uses the schedule webhook secret as a bearer token. Its JSON body is deeply merged over saved parameters and validated against the blueprint schema. `Idempotency-Key` makes retried HTTP requests return the original run.

## Runs and nodes

- `GET /api/v1/runs?limit=100`
- `GET /api/v1/runs/{id}`, `/attempts`, and `/events`
- `POST /api/v1/runs/{id}/cancel|retry`
- `GET /api/v1/agents`

`/attempts` returns each attempt's node, outcome, duration, exit code, signal, safe output sizes/truncation flags, and structured diagnostic. Diagnostics have a stable `code`, `origin`, `stage`, operator-safe `summary`, `retryable` flag, and optional status details such as hexadecimal process status and Excel HRESULT. Raw stdout, stderr, exception messages, parameters, and environment values are not returned.

`taskctl runs show <run-id>` combines the run and its attempt diagnostics.

API errors use the envelope `{"error":"message","code":"stable_code","status":400}`. The HTTP response carries the same status number. Parameter schema failures use `422`; missing/stale settings preconditions use `428`/`412`.

## Settings locks

- `GET|PUT /api/v1/settings/global`
- `GET|PUT /api/v1/settings/nodes/{agent_id}`
- `POST|PUT|DELETE /api/v1/settings/locks/{document_key}` to acquire, renew, or release a lock

Settings updates carry `expected_revision`, `lock_token`, and the new `document`, and must send the same revision in `If-Match` (for example, `If-Match: "4"`). Reads and successful updates return an `ETag` containing the current revision. Missing preconditions return `428`; mismatched preconditions return `412`.
