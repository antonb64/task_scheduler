# Custom artifact connectors

Named connectors let an external HTTP sidecar provide blueprint or parameter artifacts without loading third-party code into the coordinator. A sidecar can read a database, secret manager, enterprise API, or generated configuration and return the final document as raw HTTP response bytes.

Connectors are optional. With no `SCHEDULER_CONNECTOR_CONFIG`, normal `file://` and `http(s)://` references continue to work. A schedule explicitly selects a connector with a `connector://` URI; there is no implicit switch and no fallback to a file after connector failure.

## Resolution lifecycle

The coordinator calls an artifact adapter only when a schedule is created or updated:

1. Fetch the blueprint reference.
2. Fetch the parameter reference.
3. Parse and validate both.
4. Encrypt the resolved schedule snapshot in coordinator SQLite.

A sidecar is not called for each cron/manual/webhook run. To pick up changed connector data, edit and save the schedule. Trigger-specific dynamic input should arrive as a manual/webhook parameter override.

## Connector configuration

Set `SCHEDULER_CONNECTOR_CONFIG` to an absolute YAML or JSON path in the coordinator bootstrap environment:

```sh
SCHEDULER_CONNECTOR_CONFIG=/etc/task-scheduler/connectors.yaml
CUSTOMER_CONNECTOR_TOKEN=replace-with-secret
```

Example `/etc/task-scheduler/connectors.yaml`:

```yaml
api_version: scheduler/connectors/v1
connectors:
  customer-data:
    base_url: https://customer-connector.example.com
    bearer_token_env: CUSTOMER_CONNECTOR_TOKEN
    allowed_kinds:
      - parameters
    connect_timeout_seconds: 5
    timeout_seconds: 20
    allow_insecure_http: false

  local-development:
    base_url: http://127.0.0.1:9010
    allowed_kinds:
      - blueprint
      - parameters
    connect_timeout_seconds: 2
    timeout_seconds: 10
```

The formal configuration schema is [`schemas/connectors-v1.schema.json`](../schemas/connectors-v1.schema.json), and a copyable file is [`examples/connectors.example.yaml`](../examples/connectors.example.yaml).

### Configuration fields

| Field | Required/default | Meaning |
| --- | --- | --- |
| `api_version` | Required | Must be exactly `scheduler/connectors/v1`. |
| `connectors` | `{}` | Map from connector name to endpoint configuration. |
| `base_url` | Required | HTTP(S) service base URL. The coordinator appends `/v1/artifacts/fetch`, preserving a base path. It must not contain user info, query, or fragment. |
| `bearer_token_env` | Unset | Name of an environment variable containing the bearer token. The token is read at coordinator startup and never placed in synchronized settings. |
| `allowed_kinds` | `[parameters]` | Any nonempty subset of `blueprint` and `parameters`. |
| `connect_timeout_seconds` | `5` | TCP/TLS connection timeout, at least 1. |
| `timeout_seconds` | `20` | Total request timeout, at least 1 and not shorter than the connect timeout. |
| `allow_insecure_http` | `false` | Allows plaintext HTTP for a non-loopback host. Loopback `localhost`, IPv4, and IPv6 are allowed over HTTP without it. |

Connector names are 1–63 characters, contain only lowercase ASCII letters, digits, and internal hyphens, and cannot begin/end with a hyphen. Unknown configuration fields are rejected.

If `bearer_token_env` is configured but its name is blank, the variable is missing, or its value is empty, coordinator startup fails. Put the token value in a service secret/environment source, not in the YAML. Restrict the configuration and environment file with OS permissions even though the YAML contains only the variable name.

HTTPS is required for non-loopback services unless `allow_insecure_http: true` is deliberately set. Connector clients do not follow redirects, preventing credentials from being forwarded to a redirected origin.

Connector HTTPS uses the Rustls WebPKI roots built into the coordinator; there is no per-connector CA-file field and installing a CA only in the operating-system store is not sufficient for this build. Use a publicly trusted certificate, or run a loopback HTTP connector behind a local TLS/authentication proxy that the coordinator calls through `127.0.0.1`.

## Connector URI

Use a named connector in either artifact reference:

```yaml
blueprint_ref:
  uri: file:///srv/task-scheduler/artifacts/import.yaml
parameters_ref:
  uri: connector://customer-data/customers/42?environment=production
```

The URI host is the connector name. The path and raw query become the request `resource`:

```text
connector://customer-data/customers/42?environment=production
                           └──────── resource: /customers/42?environment=production
```

User information, fragments, and ports are forbidden in connector URIs. Do not place credentials in the resource or query: it may appear in schedule configuration and operational error context. Use `bearer_token_env` or the sidecar's own secret facilities.

## Sidecar HTTP protocol

For each fetch, the coordinator sends:

```http
POST /v1/artifacts/fetch HTTP/1.1
Content-Type: application/json
Authorization: Bearer <token-if-configured>
```

```json
{
  "api_version": "scheduler.connector/v1",
  "kind": "parameters",
  "resource": "/customers/42?environment=production"
}
```

Notice the version distinction: the bootstrap document is `scheduler/connectors/v1` (plural), while a fetch request is `scheduler.connector/v1` (singular).

Request fields:

| Field | Values |
| --- | --- |
| `api_version` | Exactly `scheduler.connector/v1`. |
| `kind` | `blueprint` or `parameters`. |
| `resource` | URI path plus optional raw query, including the leading slash. Treat it as untrusted input. |

On success, return a `2xx` response whose body is the raw artifact, not a JSON envelope or base64 value:

```http
HTTP/1.1 200 OK
X-Scheduler-Connector-Api-Version: scheduler.connector/v1
Content-Type: application/json
ETag: "customer-42-v7"
Content-Length: 49

{"customer_id":42,"mode":"production"}
```

Every successful response must include `X-Scheduler-Connector-Api-Version: scheduler.connector/v1`. This explicit response handshake prevents a generic or incompatible HTTP service from being mistaken for the configured sidecar.

The content type must match the requested kind:

- Parameters: `application/json` or an `application/*+json` media type.
- Blueprint: a JSON type above; `application/yaml`, `application/x-yaml`, `text/yaml`, or `text/x-yaml`; or a subtype ending in `+yaml`. Prefer `application/yaml` for YAML.

Parameters must contain JSON regardless of response body filename/resource. Blueprints are parsed as YAML when their media type indicates YAML and otherwise as JSON with a YAML fallback. `ETag` is preferred as source-version metadata; `Last-Modified` is used when no ETag exists. Both version headers are optional.

Return a non-2xx status for missing resources, failed authentication, upstream failures, or unavailable data. The coordinator records a safe error containing the connector name and status code/reason. It does not accept the body as an artifact and does not fall back to another adapter. Connector timeout errors become `504 Gateway Timeout` at the schedule API; transport errors, upstream status, and response-protocol errors become `502 Bad Gateway`.

The coordinator enforces the response while streaming it, including when `Content-Length` is absent:

- Blueprint: at most 1,048,576 bytes.
- Parameters: at most 4,194,304 bytes.

## Minimal sidecar example

This Python standard-library example serves parameter objects. It is intentionally small; add real authentication, TLS, request limits, structured logs, and upstream timeouts before production use.

```python
#!/usr/bin/env python3
import hashlib
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOKEN = os.environ.get("CONNECTOR_TOKEN")

PARAMETERS = {
    "/customers/42?environment=production": {
        "customer_id": 42,
        "mode": "production",
    }
}


class Handler(BaseHTTPRequestHandler):
    server_version = "scheduler-example-connector/1"

    def do_POST(self):
        if self.path != "/v1/artifacts/fetch":
            return self.reply(404, b"")
        if TOKEN and self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self.reply(401, b"")
        try:
            length = min(int(self.headers.get("Content-Length", "0")), 65536)
            request = json.loads(self.rfile.read(length))
        except (ValueError, json.JSONDecodeError):
            return self.reply(400, b"")
        if request.get("api_version") != "scheduler.connector/v1":
            return self.reply(400, b"")
        if request.get("kind") != "parameters":
            return self.reply(403, b"")
        document = PARAMETERS.get(request.get("resource"))
        if document is None:
            return self.reply(404, b"")
        body = json.dumps(document, separators=(",", ":")).encode()
        etag = '"' + hashlib.sha256(body).hexdigest() + '"'
        self.send_response(200)
        self.send_header("X-Scheduler-Connector-Api-Version", "scheduler.connector/v1")
        self.send_header("Content-Type", "application/json")
        self.send_header("ETag", etag)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def reply(self, status, body):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def log_message(self, format, *args):
        print(format % args, flush=True)


ThreadingHTTPServer(("127.0.0.1", 9010), Handler).serve_forever()
```

Run it locally:

```sh
export CONNECTOR_TOKEN=development-only
python3 connector.py
```

Configure the coordinator side:

```yaml
api_version: scheduler/connectors/v1
connectors:
  customer-data:
    base_url: http://127.0.0.1:9010
    bearer_token_env: CUSTOMER_CONNECTOR_TOKEN
    allowed_kinds: [parameters]
```

```sh
export CUSTOMER_CONNECTOR_TOKEN=development-only
export SCHEDULER_CONNECTOR_CONFIG=/absolute/path/to/connectors.yaml
```

Test the sidecar contract independently:

```sh
curl -i http://127.0.0.1:9010/v1/artifacts/fetch \
  -H 'Authorization: Bearer development-only' \
  -H 'Content-Type: application/json' \
  -d '{"api_version":"scheduler.connector/v1","kind":"parameters","resource":"/customers/42?environment=production"}'
```

Then create/update a schedule whose `parameters_ref.uri` is `connector://customer-data/customers/42?environment=production`.

## Production connector checklist

- Authenticate the coordinator and authorize the requested kind/resource.
- Terminate TLS at the sidecar or a trusted local proxy.
- Treat `resource` as opaque/untrusted; prevent path traversal and injection into database queries.
- Never include secrets or parameter values in error bodies, URLs, logs, ETags, or telemetry labels.
- Set upstream and database timeouts shorter than the configured coordinator total timeout.
- Make successful output deterministic for a given source version.
- Return the mandatory protocol-version response header and a kind-appropriate Content-Type.
- Return an ETag or Last-Modified value for operator correlation.
- Enforce response-size limits on the sidecar as well as the coordinator.
- Run independently from the coordinator so a connector crash cannot terminate scheduling.
- Test 200, authentication failure, not found, timeout, truncated connection, oversize response, invalid JSON, and wrong artifact kind.

There is no connector discovery or health endpoint in version 1. Connector health is observed when a schedule create/update performs a fetch; `/health/ready` does not probe sidecars.

Each fetch emits a structured log/OTLP event with target `scheduler.connector`. Success includes connector name, artifact kind, byte count, and duration. Failure includes safe error class, connector name when known, artifact kind, upstream status when present, and duration. Resource URIs, bearer tokens, response bodies, and parameter values are deliberately excluded. Dedicated connector counters/health gauges are not currently emitted.
