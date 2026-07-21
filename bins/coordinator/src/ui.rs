use std::collections::BTreeMap;

use askama::Template;
use axum::{
    Form, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use scheduler_core::{
    AgentView, ArtifactRef, CronSpec, GlobalSettings, NodeSettings, ScheduleSpec,
};
use scheduler_store::NewSchedule;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    api::{create_run_from_schedule, resolve_and_encrypt},
    auth::hash_secret,
    state::AppState,
};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/login", get(login_page).post(login))
        .route("/logout", post(logout))
        .route("/schedules", get(schedules))
        .route("/schedules/new", get(new_schedule))
        .route("/schedules/{id}/edit", get(edit_schedule))
        .route("/runs", get(runs))
        .route("/runs/{id}", get(run_detail))
        .route("/nodes", get(nodes))
        .route("/settings/global", get(edit_global_settings))
        .route("/settings/nodes/{id}", get(edit_node_settings))
        .route("/ui/schedules", post(create_schedule))
        .route("/ui/schedules/{id}", post(update_schedule))
        .route("/ui/schedules/{id}/run", post(run_now))
        .route("/ui/schedules/{id}/toggle", post(toggle_schedule))
        .route("/ui/schedules/{id}/webhook", post(rotate_webhook))
        .route("/ui/runs/{id}/cancel", post(cancel_run))
        .route("/ui/runs/{id}/retry", post(retry_run))
        .route("/ui/settings/global", post(save_global_settings))
        .route("/ui/settings/nodes/{id}", post(save_node_settings))
        .route("/ui/settings/lock/renew", post(renew_settings_lock))
        .route("/ui/settings/lock/release", post(release_settings_lock))
        .route("/ui/cron-preview", post(cron_preview))
        .with_state(state)
}

#[derive(Template)]
#[template(
    source = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ title }} · Task Scheduler</title>
<script src="https://unpkg.com/htmx.org@2.0.4"></script>
<style>
:root{--ink:#121711;--paper:#f2efe4;--panel:#e4e0d3;--line:#979c88;--acid:#d8ff3e;--amber:#ffb000;--bad:#b92e22;--good:#287a3a}*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);font-family:"Aptos Mono","IBM Plex Mono","Courier New",monospace;font-size:14px}header{display:flex;align-items:baseline;gap:28px;padding:14px 22px;border-bottom:3px solid var(--ink);background:var(--ink);color:var(--paper)}header strong{font-size:17px;letter-spacing:.08em;text-transform:uppercase}nav a{color:var(--paper);margin-right:18px;text-decoration:none}nav a:hover{color:var(--acid)}main{max-width:1400px;margin:0 auto;padding:24px}h1{font-size:27px;margin:0 0 22px;text-transform:uppercase;letter-spacing:.04em}h2{font-size:16px;margin:28px 0 10px;text-transform:uppercase}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px}.metric,.panel{border:1px solid var(--line);background:#faf8ef;padding:15px}.metric b{display:block;font-size:28px;margin-top:8px}table{width:100%;border-collapse:collapse;background:#faf8ef}th,td{text-align:left;padding:9px 10px;border:1px solid var(--line);vertical-align:top}th{background:var(--panel);text-transform:uppercase;font-size:12px}a{color:#2657a5}button,.button{border:1px solid var(--ink);background:var(--ink);color:white;padding:8px 12px;font:inherit;text-decoration:none;cursor:pointer}button:hover,.button:hover{background:var(--acid);color:var(--ink)}button.danger{background:var(--bad)}input,textarea,select{width:100%;padding:9px;border:1px solid var(--line);background:white;font:inherit}textarea{min-height:260px}label{display:block;margin:14px 0 5px;font-weight:bold}.row{display:grid;grid-template-columns:1fr 1fr;gap:16px}.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.actions form{display:inline}.badge{display:inline-block;border:1px solid currentColor;padding:2px 6px;text-transform:uppercase;font-size:11px}.good{color:var(--good)}.bad{color:var(--bad)}.muted{color:#5d6255}.notice{border-left:7px solid var(--amber);padding:12px;background:#fff3cf;margin:12px 0}.secret{padding:12px;background:var(--ink);color:var(--acid);overflow-wrap:anywhere}code{font-family:inherit}footer{padding:25px;color:#676b5e;text-align:center}@media(max-width:700px){header{display:block}nav{margin-top:12px}.row{grid-template-columns:1fr}main{padding:14px}th:nth-child(n+5),td:nth-child(n+5){display:none}}
</style></head><body><header><strong>Task Control / {{ node_name }}</strong><nav><a href="/">Overview</a><a href="/schedules">Schedules</a><a href="/runs">Runs</a><a href="/nodes">Nodes</a><a href="/settings/global">Settings</a></nav><form method="post" action="/logout" style="margin-left:auto"><input type="hidden" name="csrf" value="{{ csrf }}"><button type="submit">Sign out</button></form></header><main>{{ content|safe }}</main><footer>Single coordinator authority · at-least-once delivery</footer></body></html>"##,
    ext = "html"
)]
struct PageTemplate<'a> {
    title: &'a str,
    node_name: &'a str,
    csrf: &'a str,
    content: &'a str,
}

#[derive(Template)]
#[template(
    source = r#"<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in · Task Scheduler</title><style>body{background:#121711;color:#f2efe4;font:15px "Aptos Mono","Courier New",monospace;display:grid;place-items:center;min-height:100vh}.box{width:min(420px,90vw);border:1px solid #d8ff3e;padding:28px}input,button{width:100%;padding:11px;margin-top:10px;font:inherit}button{background:#d8ff3e;border:0;color:#121711}</style></head><body><form class="box" method="post"><h1>TASK CONTROL</h1><p>Single-administrator access</p>{% if error.len() > 0 %}<p>{{ error }}</p>{% endif %}<label>Administrator token<input type="password" name="token" autofocus required></label><button type="submit">Sign in</button></form></body></html>"#,
    ext = "html"
)]
struct LoginTemplate<'a> {
    error: &'a str,
}

async fn login_page() -> Response {
    Html(LoginTemplate { error: "" }.render().unwrap_or_default()).into_response()
}

#[derive(Deserialize)]
struct LoginForm {
    token: String,
}

async fn login(State(state): State<AppState>, Form(form): Form<LoginForm>) -> Response {
    if !state.auth.verify_secret(&form.token) {
        return (
            StatusCode::UNAUTHORIZED,
            Html(
                LoginTemplate {
                    error: "Invalid token",
                }
                .render()
                .unwrap_or_default(),
            ),
        )
            .into_response();
    }
    let session = state.auth.create_session().await;
    let mut response = Redirect::to("/").into_response();
    if let Ok(value) = state.auth.session_cookie(&session).parse() {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

#[derive(Deserialize)]
struct CsrfForm {
    csrf: String,
}

async fn logout(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<CsrfForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    state.auth.logout(&headers).await;
    let mut response = Redirect::to("/login").into_response();
    if let Ok(value) = state.auth.expired_cookie().parse() {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    response
}

async fn dashboard(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let schedules = state.store.list_schedules().await.unwrap_or_default();
    let runs = state.store.list_runs(500).await.unwrap_or_default();
    let agents = state.store.list_agents().await.unwrap_or_default();
    let queued = runs
        .iter()
        .filter(|run| run.state == scheduler_core::RunState::Queued)
        .count();
    let running = runs
        .iter()
        .filter(|run| run.state == scheduler_core::RunState::Running)
        .count();
    let online = agents.iter().filter(|agent| agent.connected).count();
    let content = format!(
        r#"<h1>Cluster overview</h1><div class="grid"><div class="metric">Schedules<b>{}</b></div><div class="metric">Queued<b>{}</b></div><div class="metric">Running<b>{}</b></div><div class="metric">Nodes online<b>{}/{}</b></div></div><h2>System posture</h2><div class="panel"><p><span class="badge good">Coordinator authoritative</span> Durable SQLite state and leased delivery are active.</p><p class="muted">Use the schedules page to create cron or HTTP-triggered work. Open a node to edit synchronized execution settings.</p></div>"#,
        schedules.len(),
        queued,
        running,
        online,
        agents.len()
    );
    page("Overview", &session.csrf, &content)
}

async fn schedules(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    match state.store.list_schedules().await {
        Ok(items) => {
            let mut rows = String::new();
            for item in items {
                let cron = item
                    .spec
                    .cron
                    .as_ref()
                    .map(|cron| format!("{} · {}", cron.expression, cron.timezone))
                    .unwrap_or_else(|| "manual/webhook".into());
                rows.push_str(&format!(r#"<tr><td><a href="/schedules/{}/edit">{}</a></td><td>{}</td><td><span class="badge {}">{}</span></td><td>{}</td><td>r{}</td><td><div class="actions"><form method="post" action="/ui/schedules/{}/run"><input type="hidden" name="csrf" value="{}"><button>Run now</button></form><form method="post" action="/ui/schedules/{}/toggle"><input type="hidden" name="csrf" value="{}"><button>{}</button></form></div></td></tr>"#, item.id, esc(&item.spec.name), esc(&cron), if item.spec.enabled {"good"} else {"bad"}, if item.spec.enabled {"enabled"} else {"paused"}, if item.spec.webhook_enabled {"yes"} else {"no"}, item.revision, item.id, session.csrf, item.id, session.csrf, if item.spec.enabled {"Pause"} else {"Resume"}));
            }
            let content = format!(
                r#"<div class="actions"><h1 style="margin-right:auto">Schedules</h1><a class="button" href="/schedules/new">New schedule</a></div><table><thead><tr><th>Name</th><th>Trigger</th><th>State</th><th>Webhook</th><th>Revision</th><th>Actions</th></tr></thead><tbody>{rows}</tbody></table>"#
            );
            page("Schedules", &session.csrf, &content)
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn new_schedule(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let settings = match state.store.get_global_settings().await {
        Ok(settings) => settings,
        Err(error) => return error_page(&session.csrf, &error),
    };
    page(
        "New schedule",
        &session.csrf,
        &schedule_form(None, &session.csrf, &settings.default_timezone),
    )
}

async fn edit_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let settings = match state.store.get_global_settings().await {
        Ok(settings) => settings,
        Err(error) => return error_page(&session.csrf, &error),
    };
    match state.store.get_schedule(id).await {
        Ok(Some(item)) => page(
            "Edit schedule",
            &session.csrf,
            &schedule_form(Some(&item), &session.csrf, &settings.default_timezone),
        ),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

#[derive(Debug, Deserialize)]
struct ScheduleForm {
    csrf: String,
    name: String,
    blueprint_ref: String,
    parameters_ref: String,
    cron_expression: String,
    timezone: String,
    labels_json: String,
    webhook_enabled: Option<String>,
    expected_revision: Option<i64>,
}

impl ScheduleForm {
    fn into_spec(self) -> anyhow::Result<ScheduleSpec> {
        let labels = if self.labels_json.trim().is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_str(&self.labels_json)?
        };
        let cron = if self.cron_expression.trim().is_empty() {
            None
        } else {
            Some(CronSpec {
                expression: self.cron_expression,
                timezone: self.timezone,
            })
        };
        Ok(ScheduleSpec {
            name: self.name,
            blueprint_ref: ArtifactRef {
                uri: self.blueprint_ref,
            },
            parameters_ref: ArtifactRef {
                uri: self.parameters_ref,
            },
            required_labels: labels,
            cron,
            webhook_enabled: self.webhook_enabled.is_some(),
            enabled: true,
        })
    }
}

async fn create_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<ScheduleForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let spec = match form.into_spec() {
        Ok(spec) => spec,
        Err(error) => return error_page(&session.csrf, &error),
    };
    if let Some(cron) = &spec.cron {
        if let Err(error) = scheduler_core::schedule::parse_cron(cron) {
            return error_page(&session.csrf, &error);
        }
    }
    let result = async {
        let (encrypted, digest) = resolve_and_encrypt(&state, &spec).await?;
        let (public_id, secret, secret_hash) = if spec.webhook_enabled {
            let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
            (
                Some(Uuid::new_v4().to_string()),
                Some(secret.clone()),
                Some(hash_secret(&secret)?),
            )
        } else {
            (None, None, None)
        };
        let item = state
            .store
            .create_schedule(NewSchedule {
                id: Uuid::new_v4(),
                spec,
                encrypted_snapshot: encrypted,
                snapshot_digest: digest,
                key_id: state.cipher.key_id().into(),
                webhook_public_id: public_id,
                webhook_secret_hash: secret_hash,
            })
            .await?;
        Ok::<_, anyhow::Error>((item, secret))
    }
    .await;
    match result {
        Ok((item, Some(secret))) => page(
            "Webhook created",
            &session.csrf,
            &format!(
                r#"<h1>Schedule created</h1><div class="notice">Copy this webhook secret now. It will not be shown again.</div><div class="secret">{}</div><p>Public ID: <code>{}</code></p><p><a class="button" href="/schedules">Continue</a></p>"#,
                esc(&secret),
                esc(item.webhook_public_id.as_deref().unwrap_or_default())
            ),
        ),
        Ok(_) => Redirect::to("/schedules").into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn update_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Form(form): Form<ScheduleForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let revision = form.expected_revision.unwrap_or_default();
    let mut spec = match form.into_spec() {
        Ok(spec) => spec,
        Err(error) => return error_page(&session.csrf, &error),
    };
    if let Some(cron) = &spec.cron {
        if let Err(error) = scheduler_core::schedule::parse_cron(cron) {
            return error_page(&session.csrf, &error);
        }
    }
    if let Ok(Some(existing)) = state.store.get_schedule(id).await {
        spec.enabled = existing.spec.enabled;
    }
    match resolve_and_encrypt(&state, &spec).await {
        Ok((encrypted, digest)) => match state
            .store
            .update_schedule(
                id,
                revision,
                spec,
                encrypted,
                digest,
                state.cipher.key_id().into(),
            )
            .await
        {
            Ok(_) => Redirect::to("/schedules").into_response(),
            Err(error) => error_page(&session.csrf, &error),
        },
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn run_now(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Form(form): Form<CsrfForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let result = async {
        let schedule = state
            .store
            .get_schedule_record(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("schedule not found"))?;
        create_run_from_schedule(
            &state,
            &schedule,
            &serde_json::json!({}),
            "manual",
            chrono::Utc::now(),
            None,
        )
        .await
    }
    .await;
    match result {
        Ok(_) => Redirect::to("/runs").into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn toggle_schedule(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Form(form): Form<CsrfForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let enabled = state
        .store
        .get_schedule(id)
        .await
        .ok()
        .flatten()
        .map(|item| !item.spec.enabled)
        .unwrap_or(false);
    match state.store.set_schedule_enabled(id, enabled).await {
        Ok(_) => Redirect::to("/schedules").into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn rotate_webhook(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Form(form): Form<CsrfForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let public_id = Uuid::new_v4().to_string();
    let result = match hash_secret(&secret) {
        Ok(hash) => {
            state
                .store
                .rotate_webhook(id, public_id.clone(), hash)
                .await
        }
        Err(error) => Err(error),
    };
    match result {
        Ok(_) => page(
            "Webhook rotated",
            &session.csrf,
            &format!(
                r#"<h1>Webhook rotated</h1><div class="notice">Copy this secret now.</div><div class="secret">{}</div><p>Public ID: <code>{}</code></p><p><a class="button" href="/schedules">Continue</a></p>"#,
                esc(&secret),
                esc(&public_id)
            ),
        ),
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn runs(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    match state.store.list_runs(500).await {
        Ok(items) => {
            let mut rows = String::new();
            for item in items {
                let state_name = item.state.as_str();
                rows.push_str(&format!(r#"<tr><td><a href="/runs/{}"><code>{}</code></a></td><td>{}</td><td><span class="badge {}">{}</span></td><td>{}</td><td>{}</td><td>{}</td></tr>"#, item.id, &item.id.to_string()[..8], item.trigger_kind, if state_name == "succeeded" {"good"} else if state_name == "failed" {"bad"} else {""}, state_name, item.scheduled_at, item.attempt_count, item.updated_at));
            }
            page(
                "Runs",
                &session.csrf,
                &format!(
                    r#"<h1>Runs</h1><table><thead><tr><th>Run</th><th>Trigger</th><th>State</th><th>Scheduled</th><th>Attempts</th><th>Updated</th></tr></thead><tbody>{rows}</tbody></table>"#
                ),
            )
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn run_detail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let result = async {
        let run = state
            .store
            .get_run(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("run not found"))?;
        let events = state
            .store
            .audit_events("run", &id.to_string(), 500)
            .await?;
        let attempts = state.store.run_attempts(id).await?;
        Ok::<_, anyhow::Error>((run, attempts, events))
    }
    .await;
    match result {
        Ok((run, attempts, events)) => {
            let mut event_rows = String::new();
            for event in events {
                event_rows.push_str(&format!(
                    "<tr><td>{}</td><td>{}</td><td><code>{}</code></td></tr>",
                    esc(event["occurred_at"].as_str().unwrap_or_default()),
                    esc(event["event_type"].as_str().unwrap_or_default()),
                    esc(&event["metadata"].to_string())
                ));
            }
            let mut attempt_rows = String::new();
            for attempt in &attempts {
                let diagnostic = attempt.diagnostic.as_ref();
                let location = diagnostic
                    .map(|value| {
                        format!(
                            "{} / {}",
                            json_enum_name(value.origin),
                            json_enum_name(value.stage)
                        )
                    })
                    .unwrap_or_else(|| "—".into());
                let code = diagnostic
                    .map(|value| json_enum_name(value.code))
                    .unwrap_or_else(|| "—".into());
                let status = diagnostic
                    .and_then(|value| value.status.as_ref())
                    .map(|value| {
                        let mut parts = Vec::new();
                        if let Some(exit) = attempt.exit_code {
                            parts.push(format!("exit={exit}"));
                        }
                        if let Some(hex) = &value.status_code_hex {
                            parts.push(hex.clone());
                        }
                        if let Some(signal) = &attempt.signal {
                            parts.push(format!("signal={signal}"));
                        }
                        if let Some(hresult) = &value.hresult_hex {
                            parts.push(format!("HRESULT={hresult}"));
                        }
                        if let Some(pid) = value.process_id {
                            parts.push(format!("pid={pid}"));
                        }
                        if parts.is_empty() {
                            "—".into()
                        } else {
                            parts.join(" · ")
                        }
                    })
                    .unwrap_or_else(|| {
                        attempt
                            .exit_code
                            .map(|code| format!("exit={code}"))
                            .unwrap_or_else(|| "—".into())
                    });
                let output = attempt
                    .output
                    .as_ref()
                    .map(|value| {
                        format!(
                            "out={}B{} · err={}B{}",
                            value.stdout_bytes,
                            if value.stdout_truncated {
                                " (truncated)"
                            } else {
                                ""
                            },
                            value.stderr_bytes,
                            if value.stderr_truncated {
                                " (truncated)"
                            } else {
                                ""
                            }
                        )
                    })
                    .unwrap_or_else(|| "—".into());
                attempt_rows.push_str(&format!(
                    r#"<tr><td>{}</td><td><code>{}</code></td><td>{}</td><td>{}</td><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td></tr>"#,
                    attempt.attempt_number,
                    esc(&attempt.agent_id),
                    esc(attempt.outcome.as_deref().unwrap_or(&attempt.state)),
                    esc(&location),
                    esc(&code),
                    esc(&status),
                    attempt.duration_ms.map(|value| format!("{value} ms")).unwrap_or_else(|| "—".into()),
                    esc(&output),
                    esc(diagnostic.map(|value| value.summary.as_str()).unwrap_or("—")),
                ));
            }
            let latest_diagnosis = attempts
                .iter()
                .rev()
                .find_map(|attempt| attempt.diagnostic.as_ref())
                .map(|diagnostic| {
                    format!(
                        r#"<div class="notice"><b>Latest diagnosis:</b> <code>{}</code> at <b>{} / {}</b> — {} Retryable: <b>{}</b>.</div>"#,
                        esc(&json_enum_name(diagnostic.code)),
                        esc(&json_enum_name(diagnostic.origin)),
                        esc(&json_enum_name(diagnostic.stage)),
                        esc(&diagnostic.summary),
                        if diagnostic.retryable { "yes" } else { "no" },
                    )
                })
                .unwrap_or_default();
            let actions = match run.state {
                scheduler_core::RunState::Queued | scheduler_core::RunState::Running => format!(
                    r#"<form method="post" action="/ui/runs/{id}/cancel"><input type="hidden" name="csrf" value="{}"><button class="danger">Cancel</button></form>"#,
                    session.csrf
                ),
                scheduler_core::RunState::Failed => format!(
                    r#"<form method="post" action="/ui/runs/{id}/retry"><input type="hidden" name="csrf" value="{}"><button>Retry</button></form>"#,
                    session.csrf
                ),
                _ => String::new(),
            };
            page(
                "Run detail",
                &session.csrf,
                &format!(
                    r#"<div class="actions"><h1 style="margin-right:auto">Run <code>{id}</code></h1>{actions}</div><div class="grid"><div class="metric">State<b style="font-size:18px">{}</b></div><div class="metric">Attempts<b>{}</b></div><div class="metric">Trigger<b style="font-size:18px">{}</b></div></div>{latest_diagnosis}<h2>Attempts and diagnostics</h2><table><thead><tr><th>#</th><th>Node</th><th>Outcome</th><th>Where</th><th>Code</th><th>Status</th><th>Duration</th><th>Output</th><th>Summary</th></tr></thead><tbody>{attempt_rows}</tbody></table><h2>Audit trail</h2><table><thead><tr><th>Time</th><th>Event</th><th>Metadata</th></tr></thead><tbody>{event_rows}</tbody></table>"#,
                    run.state.as_str(),
                    run.attempt_count,
                    esc(&run.trigger_kind)
                ),
            )
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn cancel_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Form(form): Form<CsrfForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    match state.store.cancel_run(id).await {
        Ok(attempts) => {
            for (agent_id, attempt_id) in attempts {
                state
                    .send_to_agent(
                        &agent_id,
                        scheduler_protocol::control::CoordinatorMessage {
                            payload: Some(
                                scheduler_protocol::control::coordinator_message::Payload::Cancel(
                                    scheduler_protocol::control::CancelAttempt {
                                        attempt_id: attempt_id.to_string(),
                                    },
                                ),
                            ),
                        },
                    )
                    .await;
            }
            Redirect::to(&format!("/runs/{id}")).into_response()
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn retry_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Form(form): Form<CsrfForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    match state.store.retry_run(id).await {
        Ok(_) => Redirect::to(&format!("/runs/{id}")).into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn nodes(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    match state.store.list_agents().await {
        Ok(items) => {
            let mut rows = String::new();
            for item in items {
                rows.push_str(&node_row(&item));
            }
            page(
                "Nodes",
                &session.csrf,
                &format!(
                    r#"<h1>Nodes</h1><table><thead><tr><th>Node</th><th>Status</th><th>Slots</th><th>Settings</th><th>Labels</th><th>Last seen</th></tr></thead><tbody>{rows}</tbody></table>"#
                ),
            )
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

fn node_row(item: &AgentView) -> String {
    let diverged = item.desired_settings_revision != item.applied_settings_revision;
    let (settings_class, settings_label) = if item.settings_error.is_some() {
        ("bad", "rejected")
    } else if diverged {
        ("bad", "pending")
    } else {
        ("good", "applied")
    };
    let rejection = item
        .settings_error
        .as_deref()
        .map(|error| {
            format!(
                r#"<div class="bad"><strong>Rejected:</strong> {}</div>"#,
                esc(error)
            )
        })
        .unwrap_or_default();
    format!(
        r#"<tr><td><a href="/settings/nodes/{id}">{id}</a><br><span class="muted">{hostname}</span></td><td><span class="badge {connection_class}">{connection}</span></td><td>{running}/{capacity}</td><td><span class="badge {settings_class}">{settings_label}</span><br>desired r{desired} / applied r{applied}{rejection}</td><td><code>{labels}</code></td><td>{last_seen}</td></tr>"#,
        id = esc(&item.id),
        hostname = esc(&item.hostname),
        connection_class = if item.connected { "good" } else { "bad" },
        connection = if item.connected { "online" } else { "offline" },
        running = item.running,
        capacity = item.capacity,
        desired = item.desired_settings_revision,
        applied = item.applied_settings_revision,
        labels = esc(&serde_json::to_string(&item.labels).unwrap_or_default()),
        last_seen = item.last_seen_at,
    )
}

async fn edit_global_settings(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let result = async {
        let settings = state.store.get_global_settings().await?;
        let lock = state.store.acquire_lock("global", &session.id).await?;
        Ok::<_, anyhow::Error>((
            serde_json::to_string_pretty(&settings)?,
            settings.revision,
            lock.lock_token,
        ))
    }
    .await;
    match result {
        Ok((json, revision, token)) => page(
            "Global settings",
            &session.csrf,
            &settings_form(
                "/ui/settings/global",
                "Coordinator settings",
                &json,
                revision,
                &token,
                &session.csrf,
            ),
        ),
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn edit_node_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let result = async {
        let settings = state
            .store
            .get_node_settings(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("node settings not found"))?;
        let lock = state
            .store
            .acquire_lock(&format!("node:{id}"), &session.id)
            .await?;
        Ok::<_, anyhow::Error>((
            serde_json::to_string_pretty(&settings)?,
            settings.revision,
            lock.lock_token,
        ))
    }
    .await;
    match result {
        Ok((json, revision, token)) => page(
            "Node settings",
            &session.csrf,
            &settings_form(
                &format!("/ui/settings/nodes/{id}"),
                &format!("Node settings / {}", esc(&id)),
                &json,
                revision,
                &token,
                &session.csrf,
            ),
        ),
        Err(error) => error_page(&session.csrf, &error),
    }
}

#[derive(Deserialize)]
struct SettingsForm {
    csrf: String,
    revision: i64,
    lock_token: String,
    document: String,
}

#[derive(Deserialize)]
struct LockKeepaliveForm {
    csrf: String,
    document_key: String,
    lock_token: String,
}

async fn renew_settings_lock(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<LockKeepaliveForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    match state
        .store
        .renew_lock(&form.document_key, &form.lock_token)
        .await
    {
        Ok(expires) => Html(format!(
            "<span class=\"muted\">Lock renewed until {expires}</span>"
        ))
        .into_response(),
        Err(error) => (StatusCode::CONFLICT, error.to_string()).into_response(),
    }
}

async fn release_settings_lock(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<LockKeepaliveForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    match state
        .store
        .release_lock(&form.document_key, &form.lock_token, false)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct CronPreviewForm {
    csrf: String,
    cron_expression: String,
    timezone: String,
}

async fn cron_preview(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<CronPreviewForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let spec = CronSpec {
        expression: form.cron_expression,
        timezone: form.timezone,
    };
    match scheduler_core::schedule::next_occurrences(&spec, chrono::Utc::now(), 5) {
        Ok(items) => Html(format!(
            "<div class=\"notice\"><strong>Next occurrences</strong><ol>{}</ol></div>",
            items
                .into_iter()
                .map(|item| format!("<li>{item}</li>"))
                .collect::<String>()
        ))
        .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Html(format!(
                "<div class=\"notice\">{}</div>",
                esc(&error.to_string())
            )),
        )
            .into_response(),
    }
}

async fn save_global_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<SettingsForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let result = async {
        let settings: GlobalSettings = serde_json::from_str(&form.document)?;
        settings.validate()?;
        state
            .store
            .update_settings("global", form.revision, &form.document, &form.lock_token)
            .await?;
        state.push_all_node_settings().await?;
        state
            .store
            .release_lock("global", &form.lock_token, false)
            .await
    }
    .await;
    match result {
        Ok(_) => Redirect::to("/").into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn save_node_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<SettingsForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let result = async {
        let _: NodeSettings = serde_json::from_str(&form.document)?;
        let key = format!("node:{id}");
        state
            .store
            .update_settings(&key, form.revision, &form.document, &form.lock_token)
            .await?;
        state
            .store
            .release_lock(&key, &form.lock_token, false)
            .await?;
        state.push_node_settings(&id).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    match result {
        Ok(_) => Redirect::to("/nodes").into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

fn schedule_form(
    item: Option<&scheduler_core::ScheduleView>,
    csrf: &str,
    default_timezone: &str,
) -> String {
    let action = item
        .map(|item| format!("/ui/schedules/{}", item.id))
        .unwrap_or_else(|| "/ui/schedules".into());
    let spec = item.map(|item| &item.spec);
    let name = spec.map(|spec| spec.name.as_str()).unwrap_or_default();
    let blueprint = spec
        .map(|spec| spec.blueprint_ref.uri.as_str())
        .unwrap_or_default();
    let parameters = spec
        .map(|spec| spec.parameters_ref.uri.as_str())
        .unwrap_or_default();
    let cron = spec.and_then(|spec| spec.cron.as_ref());
    let labels = spec
        .map(|spec| serde_json::to_string_pretty(&spec.required_labels).unwrap_or_default())
        .unwrap_or_else(|| "{}".into());
    let checked = if spec.is_some_and(|spec| spec.webhook_enabled) {
        "checked"
    } else {
        ""
    };
    let revision = item
        .map(|item| {
            format!(
                r#"<input type="hidden" name="expected_revision" value="{}">"#,
                item.revision
            )
        })
        .unwrap_or_default();
    let webhook_action = item.map(|item| format!(r#"<h2>Webhook</h2><p>Public ID: <code>{}</code></p><form method="post" action="/ui/schedules/{}/webhook"><input type="hidden" name="csrf" value="{}"><button type="submit">Rotate webhook secret</button></form>"#, esc(item.webhook_public_id.as_deref().unwrap_or("not created")), item.id, csrf)).unwrap_or_default();
    format!(
        r##"<h1>{}</h1><form method="post" action="{}"><input type="hidden" name="csrf" value="{}">{}<label>Name<input name="name" value="{}" required></label><div class="row"><label>Blueprint URI<input name="blueprint_ref" value="{}" placeholder="file:///opt/tasks/example.yaml" required></label><label>Parameters URI<input name="parameters_ref" value="{}" placeholder="file:///opt/tasks/example.json" required></label></div><div class="row"><label>Cron expression<input name="cron_expression" value="{}" placeholder="0 0 9 * * *"></label><label>IANA timezone<input name="timezone" value="{}" required></label></div><p><button type="button" hx-post="/ui/cron-preview" hx-include="closest form" hx-target="#cron-preview">Preview next five</button></p><div id="cron-preview"></div><label>Required labels (JSON object)<textarea name="labels_json">{}</textarea></label><label><input style="width:auto" type="checkbox" name="webhook_enabled" value="yes" {}> Enable HTTP webhook</label><div class="actions"><button type="submit">Save and validate</button><a href="/schedules">Cancel</a></div></form>{}"##,
        if item.is_some() {
            "Edit schedule"
        } else {
            "New schedule"
        },
        action,
        csrf,
        revision,
        esc(name),
        esc(blueprint),
        esc(parameters),
        esc(cron
            .map(|cron| cron.expression.as_str())
            .unwrap_or_default()),
        esc(cron
            .map(|cron| cron.timezone.as_str())
            .unwrap_or(default_timezone)),
        esc(&labels),
        checked,
        webhook_action
    )
}

fn settings_form(
    action: &str,
    title: &str,
    json: &str,
    revision: i64,
    lock_token: &str,
    csrf: &str,
) -> String {
    let document_key = if action.ends_with("/global") {
        "global".to_owned()
    } else {
        format!("node:{}", action.rsplit('/').next().unwrap_or_default())
    };
    format!(
        r#"<h1>{title}</h1><div class="notice">This settings document is locked to your current session for two minutes. Saving also verifies revision r{revision}. <span id="lock-status"></span></div><form method="post" action="{action}"><input type="hidden" name="csrf" value="{csrf}"><input type="hidden" name="revision" value="{revision}"><input type="hidden" name="lock_token" value="{lock_token}"><label>Settings JSON<textarea name="document" spellcheck="false">{}</textarea></label><button type="submit">Validate and apply</button></form><script>const lockBody=new URLSearchParams({{csrf:'{csrf}',document_key:'{document_key}',lock_token:'{lock_token}'}});setInterval(()=>fetch('/ui/settings/lock/renew',{{method:'POST',headers:{{'Content-Type':'application/x-www-form-urlencoded'}},body:lockBody}}).then(r=>r.text()).then(t=>document.getElementById('lock-status').innerHTML=t),30000);addEventListener('pagehide',()=>fetch('/ui/settings/lock/release',{{method:'POST',headers:{{'Content-Type':'application/x-www-form-urlencoded'}},body:lockBody,keepalive:true}}));</script>"#,
        esc(json)
    )
}

fn page(title: &str, csrf: &str, content: &str) -> Response {
    match (PageTemplate {
        title,
        node_name: "coordinator",
        csrf,
        content,
    })
    .render()
    {
        Ok(body) => Html(body).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

fn error_page(csrf: &str, error: &dyn std::fmt::Display) -> Response {
    page(
        "Error",
        csrf,
        &format!(
            r#"<h1>Request failed</h1><div class="notice">{}</div><p><a href="javascript:history.back()">Go back</a></p>"#,
            esc(&error.to_string())
        ),
    )
}

fn esc(value: &str) -> String {
    html_escape::encode_text(value).into_owned()
}

fn json_enum_name(value: impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent_view() -> AgentView {
        AgentView {
            id: "node-<west>".into(),
            hostname: "desktop & excel".into(),
            labels: BTreeMap::from([("os".into(), "windows".into())]),
            capacity: 2,
            running: 1,
            connected: true,
            desired_settings_revision: 4,
            applied_settings_revision: 3,
            settings_error: None,
            last_seen_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn node_row_marks_revision_divergence_and_escapes_rejection_errors() {
        let mut agent = agent_view();
        let pending = node_row(&agent);
        assert!(pending.contains(r#"class="badge bad">pending"#));
        assert!(pending.contains("desired r4 / applied r3"));

        agent.settings_error = Some("invalid <script>alert('secret')</script> & value".into());
        let rejected = node_row(&agent);
        assert!(rejected.contains(r#"class="badge bad">rejected"#));
        assert!(rejected.contains("<strong>Rejected:</strong>"));
        assert!(rejected.contains("&lt;script&gt;"));
        assert!(rejected.contains("&amp; value"));
        assert!(!rejected.contains("<script>"));
        assert!(!rejected.contains("node-<west>"));
        assert!(rejected.contains("node-&lt;west&gt;"));
    }

    #[test]
    fn node_row_marks_matching_revision_as_applied() {
        let mut agent = agent_view();
        agent.applied_settings_revision = agent.desired_settings_revision;
        let row = node_row(&agent);
        assert!(row.contains(r#"class="badge good">applied"#));
        assert!(!row.contains("Rejected:"));
    }
}
