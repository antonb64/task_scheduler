# Installation and node setup

This guide installs one authoritative coordinator and one or more agents. Commands use release binaries built from this repository. Package or copy the four executables in a way appropriate for your operating system.

## 1. Build and lay out the binaries

Rust 1.88 or newer is required. The workspace vendors `protoc`.

```sh
git clone https://github.com/antonb64/task_scheduler.git
cd task_scheduler
cargo build --release --locked
```

The resulting programs are:

```text
target/release/coordinator
target/release/agent
target/release/task-executor
target/release/taskctl
```

On the coordinator, create directories for the database, configuration, certificates, and file artifacts:

```sh
sudo install -d -m 0750 /var/lib/task-scheduler /etc/task-scheduler /srv/task-scheduler/artifacts
sudo install -m 0755 target/release/coordinator target/release/taskctl /usr/local/bin/
```

On a Linux agent:

```sh
sudo install -d -m 0750 /var/lib/task-scheduler /etc/task-scheduler
sudo install -m 0755 target/release/agent target/release/task-executor /usr/local/bin/
```

Use a dedicated operating-system account and restrict the SQLite files, connector configuration, certificate keys, artifact documents, and Excel workbooks to that account.

## 2. Create bootstrap secrets

Generate the 32-byte XChaCha20-Poly1305 master key with the shipped CLI and create an unrelated administrator token:

```sh
taskctl generate-master-key
openssl rand -base64 32
```

Store both in a root-readable environment file or secret manager. Do not put either value in source control. Keep a recoverable backup of the master key: replacing or losing it makes existing encrypted schedule snapshots and results unreadable.

## 3. Issue TLS certificates

Use certificates from your organization or a dedicated private CA for gRPC. The gRPC server certificate must cover the hostname used in each agent's `SCHEDULER_COORDINATOR_URL`, and each agent needs a client certificate accepted by `SCHEDULER_GRPC_CLIENT_CA`.

The management HTTPS certificate must cover its management DNS name. When the coordinator serves HTTPS directly, its internal management-proxy client uses the bundled WebPKI roots and has no custom CA setting. Use a publicly trusted management certificate, or terminate public HTTPS at a reverse proxy while the coordinator REST listener and `SCHEDULER_INTERNAL_REST_URL` remain on loopback HTTP. A private CA remains suitable for gRPC because the agent has an explicit `SCHEDULER_AGENT_TLS_CA` setting.

The following OpenSSL commands demonstrate a small private CA. Protect every `.key` file and adapt subjects, validity periods, and CA policy for production:

```sh
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out scheduler-ca.key
openssl req -x509 -new -sha256 -key scheduler-ca.key -days 3650 \
  -subj '/CN=Task Scheduler CA' -out scheduler-ca.crt

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out coordinator.key
openssl req -new -sha256 -key coordinator.key \
  -subj '/CN=scheduler.example.com' \
  -addext 'subjectAltName=DNS:scheduler.example.com' \
  -addext 'extendedKeyUsage=serverAuth' \
  -out coordinator.csr
openssl x509 -req -sha256 -in coordinator.csr -CA scheduler-ca.crt -CAkey scheduler-ca.key \
  -CAcreateserial -days 825 -copy_extensions copy -out coordinator.crt

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out command-01.key
openssl req -new -sha256 -key command-01.key \
  -subj '/CN=command-01' -addext 'extendedKeyUsage=clientAuth' -out command-01.csr
openssl x509 -req -sha256 -in command-01.csr -CA scheduler-ca.crt -CAkey scheduler-ca.key \
  -CAcreateserial -days 825 -copy_extensions copy -out command-01.crt
```

The current coordinator verifies that the client certificate chains to the configured CA. It does not bind the certificate subject to the agent's self-declared agent ID, so issue client certificates only to trusted nodes and keep the CA narrowly scoped.

## 4. Start the coordinator

Create `/etc/task-scheduler/coordinator.env` with permissions `0600`:

```sh
SCHEDULER_DATABASE_URL=sqlite:///var/lib/task-scheduler/coordinator.db
SCHEDULER_LOCK_PATH=/var/lib/task-scheduler/coordinator.lock
SCHEDULER_REST_ADDR=0.0.0.0:8443
SCHEDULER_GRPC_ADDR=0.0.0.0:50051
SCHEDULER_INTERNAL_REST_URL=https://scheduler.example.com:8443
SCHEDULER_MASTER_KEY=replace-with-taskctl-output
SCHEDULER_MASTER_KEY_ID=v1
SCHEDULER_ADMIN_TOKEN=replace-with-a-different-random-secret
SCHEDULER_ARTIFACT_ROOTS=/srv/task-scheduler/artifacts
SCHEDULER_HTTP_TLS_CERT=/etc/task-scheduler/management-fullchain.pem
SCHEDULER_HTTP_TLS_KEY=/etc/task-scheduler/management-private-key.pem
SCHEDULER_GRPC_TLS_CERT=/etc/task-scheduler/coordinator.crt
SCHEDULER_GRPC_TLS_KEY=/etc/task-scheduler/coordinator.key
SCHEDULER_GRPC_CLIENT_CA=/etc/task-scheduler/scheduler-ca.crt
SCHEDULER_SECURE_COOKIES=true
OTEL_EXPORTER_OTLP_ENDPOINT=https://otel.example.internal:4317
RUST_LOG=info
```

When HTTPS is enabled directly, `SCHEDULER_INTERNAL_REST_URL` must also use an HTTPS URL that reaches the same listener. Its certificate chain must be accepted by the Rustls WebPKI roots because the internal management-proxy client has no separate private-CA option.

Alternatively, use a local reverse proxy: leave `SCHEDULER_HTTP_TLS_CERT`/`KEY` unset, bind `SCHEDULER_REST_ADDR=127.0.0.1:8080`, retain `SCHEDULER_INTERNAL_REST_URL=http://127.0.0.1:8080`, set `SCHEDULER_SECURE_COOKIES=true`, and publish only the proxy's HTTPS listener.

Test interactively first:

```sh
set -a
. /etc/task-scheduler/coordinator.env
set +a
coordinator
```

Example systemd unit `/etc/systemd/system/task-scheduler-coordinator.service`:

```ini
[Unit]
Description=Task Scheduler Coordinator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=task-scheduler
Group=task-scheduler
EnvironmentFile=/etc/task-scheduler/coordinator.env
ExecStart=/usr/local/bin/coordinator
WorkingDirectory=/var/lib/task-scheduler
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now task-scheduler-coordinator
curl https://scheduler.example.com:8443/health/ready
```

`/health/live` and `/health/ready` return `204 No Content` when successful. Readiness currently checks SQLite access only.

## 5. Start a Linux agent

Create `/etc/task-scheduler/agent.env`:

```sh
SCHEDULER_AGENT_ID=command-01
SCHEDULER_COORDINATOR_URL=https://scheduler.example.com:50051
SCHEDULER_AGENT_DATABASE_URL=sqlite:///var/lib/task-scheduler/agent.db
SCHEDULER_AGENT_UI_ADDR=127.0.0.1:8081
SCHEDULER_EXECUTOR_PATH=/usr/local/bin/task-executor
SCHEDULER_AGENT_CAPACITY=4
SCHEDULER_AGENT_LABELS=pool=general
SCHEDULER_AGENT_TLS_CA=/etc/task-scheduler/scheduler-ca.crt
SCHEDULER_AGENT_TLS_CERT=/etc/task-scheduler/command-01.crt
SCHEDULER_AGENT_TLS_KEY=/etc/task-scheduler/command-01.key
OTEL_EXPORTER_OTLP_ENDPOINT=https://otel.example.internal:4317
RUST_LOG=info
```

Example `/etc/systemd/system/task-scheduler-agent.service`:

```ini
[Unit]
Description=Task Scheduler Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=task-agent
Group=task-agent
EnvironmentFile=/etc/task-scheduler/agent.env
ExecStart=/usr/local/bin/agent
WorkingDirectory=/var/lib/task-scheduler
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now task-scheduler-agent
```

The agent connects outbound; no inbound gRPC firewall rule is needed on the node. Its UI proxy is local at `http://127.0.0.1:8081` and returns `503` while the coordinator stream is unavailable. With production `SCHEDULER_SECURE_COOKIES=true`, browsers must reach that proxy through an HTTPS reverse proxy; a Secure session cookie is not sent to a plain `http://` agent URL.

For example, an HTTPS nginx virtual host on the node can forward all paths without exposing port 8081:

```nginx
server {
    listen 443 ssl;
    server_name command-01.example.com;
    ssl_certificate /etc/letsencrypt/live/command-01.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/command-01.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8081;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
    }
}
```

The scheduler does not currently interpret forwarded headers for URL generation, but its routes and cookies are host-relative, so this simple whole-site proxy is sufficient. Apply your normal network allowlist and browser-access authentication policy in front of it.

## 6. Start a macOS agent

Install `agent` and `task-executor` in a stable directory, such as `/usr/local/libexec/task-scheduler`, and use a LaunchAgent or LaunchDaemon appropriate to the task account. A minimal LaunchAgent property list is:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.example.task-scheduler-agent</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/libexec/task-scheduler/agent</string>
    <string>--agent-id</string><string>mac-01</string>
    <string>--coordinator-url</string><string>https://scheduler.example.com:50051</string>
    <string>--database-url</string><string>sqlite:///Users/taskagent/Library/Application Support/TaskScheduler/agent.db</string>
    <string>--executor-path</string><string>/usr/local/libexec/task-scheduler/task-executor</string>
    <string>--tls-ca</string><string>/usr/local/etc/task-scheduler/scheduler-ca.crt</string>
    <string>--tls-cert</string><string>/usr/local/etc/task-scheduler/mac-01.crt</string>
    <string>--tls-key</string><string>/usr/local/etc/task-scheduler/mac-01.key</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/taskagent/Library/Logs/task-scheduler-agent.log</string>
  <key>StandardErrorPath</key><string>/Users/taskagent/Library/Logs/task-scheduler-agent.log</string>
</dict>
</plist>
```

Load it from the intended account with `launchctl bootstrap gui/$(id -u) path/to/com.example.task-scheduler-agent.plist`.

## 7. Start an interactive Windows Excel agent

Excel automation has stricter requirements:

- Install licensed desktop Excel for the task user.
- Log on interactively as that user. Do not use a Windows service identity.
- Trust or sign each `.xlsm`/`.xlam`; do not weaken Trust Center globally.
- Ensure macros never display a custom dialog, prompt for input, or wait for user interaction.
- Place workbooks under a dedicated allowlisted directory such as `C:\TaskWorkbooks`.
- Keep `excel_max_parallel` at 1.

Copy `agent.exe` and `task-executor.exe` into a stable directory, then register the current user with the helper:

```powershell
.\deploy\windows\Register-Agent.ps1 `
  -InstallDirectory 'C:\Program Files\TaskScheduler' `
  -AgentId 'excel-01' `
  -CoordinatorUrl 'https://scheduler.example.com:50051' `
  -DataDirectory (Join-Path $env:LOCALAPPDATA 'RustTaskScheduler') `
  -UiAddress '127.0.0.1:8081' `
  -Capacity 2 `
  -Labels @('pool=excel','site=vienna') `
  -TlsCa (Join-Path $env:LOCALAPPDATA 'RustTaskScheduler\scheduler-ca.crt') `
  -TlsCert (Join-Path $env:LOCALAPPDATA 'RustTaskScheduler\excel-01.crt') `
  -TlsKey (Join-Path $env:LOCALAPPDATA 'RustTaskScheduler\excel-01.key') `
  -OtlpEndpoint 'https://otel.example.internal:4317'
```

Use `-TlsDomain scheduler.example.com` only when the URL host cannot match the certificate SAN directly. The helper creates `agent.db` under `DataDirectory`, passes every setting as an explicit process argument, fixes the working directory to `InstallDirectory`, and registers `RustTaskSchedulerAgent` at logon with an interactive principal. Log off and back on, then inspect it with:

```powershell
Get-ScheduledTask -TaskName RustTaskSchedulerAgent
Get-ScheduledTaskInfo -TaskName RustTaskSchedulerAgent
Start-ScheduledTask -TaskName RustTaskSchedulerAgent
```

The helper creates the data directory but does not provision directory ACLs, certificates, Excel, workbooks, or Trust Center configuration.

## 8. Apply node settings

On first connection, the coordinator creates the node settings document using bootstrap labels and capacity. Open `https://scheduler.example.com:8443/nodes`, select the node, and set its desired state.

Typical command node:

```json
{
  "revision": 1,
  "enabled": true,
  "labels": {"os": "linux", "arch": "x86_64", "pool": "general"},
  "max_parallel": 4,
  "excel_max_parallel": 0,
  "allowed_command_roots": ["/opt/company-tasks"],
  "allowed_workbook_roots": [],
  "otlp_endpoint": null
}
```

Typical Excel node:

```json
{
  "revision": 1,
  "enabled": true,
  "labels": {"os": "windows", "arch": "x86_64", "capability": "excel", "pool": "excel"},
  "max_parallel": 2,
  "excel_max_parallel": 1,
  "allowed_command_roots": ["C:\\Program Files\\TaskScheduler"],
  "allowed_workbook_roots": ["C:\\TaskWorkbooks"],
  "otlp_endpoint": null
}
```

Wait until the Nodes page reports that the desired revision was acknowledged, then verify the intended capacity/placement with a harmless task. A rejected document leaves the applied revision unchanged and displays the agent's validation error. The coordinator does not dispatch using unacknowledged placement settings; after a reconnect it preserves a known prior applied document or keeps the node ineligible until the current revision is acknowledged.

## 9. Verify management access

```sh
export SCHEDULER_URL=https://scheduler.example.com:8443
export SCHEDULER_ADMIN_TOKEN='replace-with-admin-token'
taskctl nodes
taskctl schedules list
```

The same control panel is available through any connected agent UI. The browser authenticates with the same administrator token; agent UI requests are carried over the existing gRPC connection. Plain loopback access is suitable when secure cookies are disabled for local development. In production, publish the loopback listener only through an HTTPS reverse proxy so the browser can use the coordinator's Secure session cookie.
