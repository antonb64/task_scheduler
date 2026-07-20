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
- `GET /api/v1/runs/{id}` and `/events`
- `POST /api/v1/runs/{id}/cancel|retry`
- `GET /api/v1/agents`

## Settings locks

- `GET|PUT /api/v1/settings/global`
- `GET|PUT /api/v1/settings/nodes/{agent_id}`
- `POST|PUT|DELETE /api/v1/settings/locks/{document_key}` to acquire, renew, or release a lock

Settings updates carry `expected_revision`, `lock_token`, and the new `document`, and must send the same revision in `If-Match` (for example, `If-Match: "4"`). Reads and successful updates return an `ETag` containing the current revision. Missing preconditions return `428`; mismatched preconditions return `412`.
