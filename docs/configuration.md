# Configuration reference

Configuration has two layers:

1. Bootstrap settings are command-line flags or environment variables read at process start. They include addresses, database paths, certificate paths, and secrets. They are intentionally not editable through the cluster UI.
2. Synchronized settings are versioned JSON documents in coordinator SQLite. Global settings affect scheduling policy; node settings are pushed to the corresponding agent.

Command-line flags have the same names as the environment variables below in kebab case. For example, `SCHEDULER_DATABASE_URL` is `--database-url`.

## Coordinator bootstrap settings

| Environment variable | Required/default | Meaning |
| --- | --- | --- |
| `SCHEDULER_DATABASE_URL` | `sqlite://scheduler.db` | File-backed coordinator SQLite URL. Use an absolute path in production. |
| `SCHEDULER_LOCK_PATH` | `scheduler.lock` | OS-exclusive coordinator ownership lock. Its parent directory must exist. |
| `SCHEDULER_REST_ADDR` | `127.0.0.1:8080` | Management UI, REST API, webhooks, and health listener. |
| `SCHEDULER_GRPC_ADDR` | `127.0.0.1:50051` | Agent control-plane listener. |
| `SCHEDULER_INTERNAL_REST_URL` | `http://127.0.0.1:8080` | URL used inside the coordinator when proxying an agent-node management request. It must address the actual REST listener. |
| `SCHEDULER_MASTER_KEY` | Required | Exactly 32 random bytes encoded as base64. Generate with `taskctl generate-master-key`. |
| `SCHEDULER_MASTER_KEY_ID` | `v1` | Identifier written beside newly encrypted records. This is metadata, not a multi-key keyring. |
| `SCHEDULER_ADMIN_TOKEN` | Required | Single administrator bearer token and UI login secret. |
| `SCHEDULER_ARTIFACT_ROOTS` | Required, comma-separated | Canonical roots from which `file://` artifacts may be read. At least one root is currently required even if all schedules use connectors. |
| `SCHEDULER_CONNECTOR_CONFIG` | Unset | Optional path to the named HTTP sidecar connector YAML/JSON document. See [connectors](connectors.md). |
| `SCHEDULER_GRPC_TLS_CERT` | Unset | PEM gRPC server certificate. Must be set together with key and client CA. |
| `SCHEDULER_GRPC_TLS_KEY` | Unset | PEM gRPC server private key. |
| `SCHEDULER_GRPC_CLIENT_CA` | Unset | PEM CA used to verify agent client certificates. |
| `SCHEDULER_AGENT_CERTIFICATE_FINGERPRINTS` | Required with gRPC mTLS, comma-separated | Exact `agent-id=sha256-hex` bindings for allowed agent leaf certificates. SHA-256 may be plain 64-character hex or colon-separated. CLI form is repeatable `--agent-certificate-fingerprint`. |
| `SCHEDULER_HTTP_TLS_CERT` | Unset | PEM management HTTPS certificate. Must be set together with its key. |
| `SCHEDULER_HTTP_TLS_KEY` | Unset | PEM management HTTPS private key. |
| `SCHEDULER_SECURE_COOKIES` | `false` | Adds the `Secure` attribute to UI session cookies. Set true whenever management uses HTTPS. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Unset | Global OTLP endpoint used at process start. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | Global exporter protocol: `grpc` or `http/protobuf`. |
| `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_ENDPOINT` | Global endpoint | Optional per-signal endpoint. |
| `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_PROTOCOL` | Global protocol | Optional per-signal protocol. |
| `OTEL_EXPORTER_OTLP_HEADERS` / per-signal `..._HEADERS` | Empty | Comma-separated, percent-decoded OTLP `key=value` headers. Values are secrets. |
| `SCHEDULER_OTLP_HEADERS_FILE` | Unset | Local file containing the same header list, merged before environment headers. |
| `SCHEDULER_OTLP_CREDENTIAL_FILE` | Unset | Local file containing one bearer token; `SCHEDULER_OTLP_BEARER_TOKEN_FILE` is an alias. |
| `SCHEDULER_OTLP_TLS_CA_FILE` | Unset | PEM CA file; standard alias `OTEL_EXPORTER_OTLP_CERTIFICATE`. |
| `SCHEDULER_OTLP_TLS_CLIENT_CERT_FILE` / `SCHEDULER_OTLP_TLS_CLIENT_KEY_FILE` | Unset | PEM mTLS identity pair; standard aliases `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` and `OTEL_EXPORTER_OTLP_CLIENT_KEY`. |
| `RUST_LOG` | `info` | Standard `tracing_subscriber` filter, for example `info,scheduler_store=debug`. |

TLS tuples are all-or-nothing. Supplying only part of either tuple stops the coordinator with a configuration error. Enabling gRPC mTLS also requires at least one unique agent/fingerprint binding; bindings without mTLS are rejected. Leaving gRPC TLS and fingerprint bindings unset is intended only for loopback development.

The client CA proves that a certificate is trusted; the fingerprint map additionally binds the exact first/leaf client certificate to the `agent_id` claimed in its hello. Unregistered IDs, missing certificates, and mismatched hashes are rejected. Certificate subject/SAN text is not used for this identity decision. A renewed certificate has a different fingerprint even when it reuses the same subject or key, so update the binding and restart the coordinator as part of certificate rotation.

The coordinator opens SQLite with foreign keys, WAL journaling, normal synchronous mode, a five-second busy timeout, up to eight connections, and migrations. The lock file prevents a second coordinator using the same configured lock. Use the same stable database and lock paths after restart.

### Master key handling

Resolved schedule documents, execution snapshots, and coordinator results are encrypted using XChaCha20-Poly1305. Back up the exact master key independently of SQLite. Changing `SCHEDULER_MASTER_KEY_ID` does not rotate ciphertext, and replacing `SCHEDULER_MASTER_KEY` without migrating the stored records makes old data unreadable. Online key rotation is not implemented.

### Internal management URL

The agent management UI is not a second copy of cluster state. The browser request travels through the agent's gRPC connection and the coordinator then requests `SCHEDULER_INTERNAL_REST_URL`. Consequently:

- The URL must use the same HTTP/HTTPS mode as the management listener.
- The direct HTTPS certificate must chain to the WebPKI roots bundled for the internal Rustls HTTP client. There is no separate private-CA setting. A loopback HTTP listener behind an external HTTPS reverse proxy avoids this constraint.
- Do not point it at an untrusted reverse proxy or an address that leaves the trusted management network.

## Agent bootstrap settings

| Environment variable | Required/default | Meaning |
| --- | --- | --- |
| `SCHEDULER_AGENT_ID` | Required | Stable cluster-unique node ID: 1–64 ASCII letters, digits, `.`, `_`, or `-`, starting and ending with a letter or digit. Reusing an ID causes the new authenticated stream to replace the previous connection for that ID. |
| `SCHEDULER_COORDINATOR_URL` | `http://127.0.0.1:50051` | Coordinator gRPC URL. Use `https://` with mTLS. |
| `SCHEDULER_AGENT_DATABASE_URL` | `sqlite://agent.db` | File-backed local delivery ledger. In-memory SQLite is rejected. |
| `SCHEDULER_AGENT_UI_ADDR` | `127.0.0.1:8081` | Management proxy listener. It is HTTP unless the UI TLS certificate/key below are both configured. |
| `SCHEDULER_AGENT_UI_TLS_CERT` | Unset | PEM certificate chain for native HTTPS on the agent management listener. Must be paired with its key. |
| `SCHEDULER_AGENT_UI_TLS_KEY` | Unset | PEM private key for native agent management HTTPS. |
| `SCHEDULER_EXECUTOR_PATH` | `task-executor` | Exact path or `PATH` name of the matching executor binary. |
| `SCHEDULER_AGENT_CAPACITY` | `2` | Capacity advertised in the first hello and used to seed a new node's `max_parallel`. Later synchronized settings are authoritative. |
| `SCHEDULER_AGENT_LABELS` | Empty | Comma-separated `key=value` bootstrap labels, such as `pool=excel,site=vienna`. |
| `SCHEDULER_AGENT_TLS_CA` | Unset | PEM CA used to verify the coordinator. Must be set together with client cert and key. |
| `SCHEDULER_AGENT_TLS_CERT` | Unset | PEM agent client certificate. |
| `SCHEDULER_AGENT_TLS_KEY` | Unset | PEM agent client private key. |
| `SCHEDULER_AGENT_TLS_DOMAIN` | URL hostname | Optional TLS server-name override for cases where the URL host differs from the certificate SAN. |
| `SCHEDULER_AGENT_ALLOWED_ENV_BINDINGS` | Empty, comma-separated | Exact environment-variable names a blueprint may resolve on this node. Names only are advertised; values never leave the node. CLI form may be repeated as `--allow-environment-binding`. |
| `SCHEDULER_AGENT_SECRET_ROOTS` | Empty, comma-separated | Absolute directories containing logical secret files. CLI form may be repeated as `--secret-root`. Roots are local bootstrap policy and are not synchronized. |
| `SCHEDULER_AGENT_BINDING_MAX_BYTES` | `65536` | Per-binding read/decoded-input limit, 1–1,048,576 bytes. |
| `SCHEDULER_TASK_OUTPUT_LOGGING` | `metadata` | `off`, `metadata`, or `content`; controls `scheduler.task_output` events. Attempts with any sensitive binding suppress captured text before logging, local persistence, or transmission in every mode. |
| `OTEL_EXPORTER_OTLP_*`, `SCHEDULER_OTLP_*` | Unset | Same startup-only OTLP endpoint/protocol/header/credential/TLS variables as the coordinator. |
| `RUST_LOG` | `info` | Logging filter. |

The agent adds `os=<Rust target OS>` and `arch=<Rust target architecture>` unless supplied. Windows builds also add `capability=excel` unless supplied. This is a placement label only; it does not verify that Excel is installed or usable.

The local ledger obtains an OS-exclusive companion lock. A second live agent process cannot use the same ledger. It stores assignments and pending results so a process crash or network outage does not lose accepted work.

For native agent HTTPS, configure both UI TLS files and use a certificate valid for the browser-facing node hostname. With neither set, keep the HTTP listener on loopback and terminate HTTPS at a trusted reverse proxy. The management listener is a required agent component: a partial TLS pair, an unreadable/invalid certificate or key, or a listener bind/serve failure causes the agent process to exit instead of continuing without its UI. These UI files are unrelated to `SCHEDULER_AGENT_TLS_CERT`/`KEY`, which authenticate the outbound gRPC client.

Environment and secret-file bindings are local security capabilities. The agent advertises configured/discovered source-and-name pairs only. It performs a safe preflight before acknowledging an assignment and resolves again immediately before starting `task-executor`. Secret-file names cannot be paths and must resolve to regular files under one configured root after canonicalization; symlink escape is rejected. Protect secret roots and the agent environment with the task account's OS ACLs. See [Agent-local environment and secret-file bindings](tasks.md#agent-local-environment-and-secret-file-bindings).

## Taskctl bootstrap settings

| Environment variable | Required/default | Meaning |
| --- | --- | --- |
| `SCHEDULER_URL` | `http://127.0.0.1:8080` | Coordinator management base URL. |
| `SCHEDULER_ADMIN_TOKEN` | Required except for key generation | Administrator bearer token. |

Pass `--url` and `--token` to override them for one invocation.

## Global synchronized settings

The coordinator creates this document at revision 1:

```json
{
  "revision": 1,
  "default_timezone": "UTC",
  "default_max_attempts": 3,
  "default_timeout_seconds": 3600,
  "default_completion_deadline_seconds": 86400,
  "lease_seconds": 60,
  "heartbeat_seconds": 10,
  "audit_retention_days": 90,
  "otlp_endpoint": null
}
```

| Field | Validation and effect |
| --- | --- |
| `revision` | Current optimistic-concurrency revision. Reads overwrite this value from the database row. |
| `default_timezone` | Valid IANA timezone. Used as the default shown by the schedule UI; a schedule stores its own timezone. |
| `default_max_attempts` | At least 1. Applied while resolving a blueprint that omits `policy.max_attempts`. Existing schedules retain their resolved value. |
| `default_timeout_seconds` | At least 1. Applied while resolving a blueprint that omits `policy.timeout_seconds`. |
| `default_completion_deadline_seconds` | At least 1. Applied to a schedule that omits `observability.completion_deadline_seconds`; the effective timezone, operations day, and absolute deadline are captured on every trigger. |
| `lease_seconds` | At least three times `heartbeat_seconds`. Used for new/renewed attempt leases. |
| `heartbeat_seconds` | At least 5. Pushed to connected agents and hot-applied to their heartbeat interval. |
| `audit_retention_days` | At least 1. Retention horizon for coordinator authoritative-event outbox rows. Expiring an undelivered row records a permanent coverage gap; internal audit-row pruning is not implemented. |
| `otlp_endpoint` | Null or absolute HTTP(S) URL. Reserved synchronized field; telemetry is configured only from the bootstrap OTLP environment variables above. |

Changing a policy default does not rewrite existing encrypted schedules. Edit and save a schedule to resolve it again under the new defaults. Existing trigger identities always retain the timezone, operations day, and deadline captured when they were created.

## Node synchronized settings

Example:

```json
{
  "revision": 3,
  "enabled": true,
  "labels": {
    "os": "windows",
    "arch": "x86_64",
    "capability": "excel",
    "site": "vienna"
  },
  "max_parallel": 2,
  "excel_max_parallel": 1,
  "allowed_command_roots": [
    "C:\\Program Files\\TaskScheduler"
  ],
  "allowed_workbook_roots": [
    "C:\\TaskWorkbooks"
  ],
  "otlp_endpoint": null
}
```

| Field | Validation and effect |
| --- | --- |
| `revision` | Desired document revision. The UI displays desired versus agent-applied revisions. |
| `enabled` | Prevents validation/start of later assignments. It does not terminate a task already running. |
| `labels` | Exact-match placement labels used by the coordinator and checked again by the agent. |
| `max_parallel` | At least 1; maximum tasks started concurrently on the node. Accepted recovery reservations may wait for a slot. |
| `excel_max_parallel` | 0 disables Excel; 1 permits one Excel task. Values above 1 are rejected by the agent. |
| `allowed_command_roots` | Absolute roots for absolute command paths and working directories. Paths are canonicalized before execution. |
| `allowed_workbook_roots` | Absolute roots for Excel workbook paths. Workbooks outside them are rejected. |
| `otlp_endpoint` | Reserved node override; it does not currently reconfigure the running telemetry provider. |

A relative command program such as `python3` is resolved through the agent account's `PATH` and is not checked against `allowed_command_roots`. Prefer absolute executable paths for a tightly controlled production node.

Node settings are persisted on the agent before it acknowledges a valid revision. Supported fields hot-apply to future placement and execution checks only after the coordinator records a successful acknowledgement of the current desired revision. On invalid desired settings the agent sends an error, retains its previous effective document, and does not advance its applied revision. The Nodes page/API shows the rejection text and desired versus applied revisions. Stale, future, and late acknowledgements are audited but cannot change the effective coordinator placement settings. On coordinator restart or an otherwise unprovable reconnect state, the node remains placement-ineligible until it acknowledges the current document.

## Editing settings safely

Opening a global settings, node settings, or dashboard edit page acquires a two-minute document lock for the current UI session. The page renews it every 30 seconds and releases it during normal navigation. Saving also compares the revision so an expired lock cannot overwrite a newer document.

If another page/session owns the lock, the UI shows the current document in a read-only textarea, identifies whether this or another administrator session owns it, shows the expiry, and does not expose the lock token. **Retry lock** reloads after normal release/expiry. **Force unlock** requires the authenticated session and CSRF confirmation, deletes the competing lock, writes `settings.lock_force_released` to the settings audit stream, and returns to the edit page. The former editor's token is then invalid; revision fencing still prevents a stale save after another edit.

REST clients use the same protocol:

1. `GET /api/v1/settings/global` and retain its `ETag`, such as `"3"`.
2. `POST /api/v1/settings/locks/global` with `{"owner_session":"maintenance-script"}`.
3. `PUT /api/v1/settings/global` with `If-Match: "3"` and a body containing `expected_revision`, `lock_token`, and `document`.
4. `DELETE /api/v1/settings/locks/global` with the token, or `force: true` for administrative recovery. A force release of an existing lock is audited.

Node document keys use `node:<agent-id>` for lock operations.

## Formal schemas

- [`blueprint-v1.schema.json`](../schemas/blueprint-v1.schema.json)
- [`schedule-v1.schema.json`](../schemas/schedule-v1.schema.json)
- [`parameter-collection-v1.schema.json`](../schemas/parameter-collection-v1.schema.json)
- [`connectors-v1.schema.json`](../schemas/connectors-v1.schema.json)

The Rust implementation remains authoritative if a development checkout temporarily contains a newer field than the checked-in schemas.
