use std::collections::BTreeMap;

use askama::Template;
use axum::{
    Form, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use scheduler_core::{
    AgentView, ArtifactRef, CronSpec, GlobalSettings, NodeSettings, RunState, RunView, ScheduleSpec,
};
use scheduler_store::{BatchItemView, BatchView, EditLock, NewSchedule};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    api::{create_run_from_schedule, resolve_and_encrypt},
    auth::{UiSession, hash_secret},
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
        .route("/batches", get(batches))
        .route("/batches/{id}", get(batch_detail))
        .route("/nodes", get(nodes))
        .route("/blueprints", get(blueprints))
        .route("/nodes/{id}", get(node_health_detail))
        .route("/search", get(search))
        .route("/dashboard/edit", get(edit_dashboard))
        .route("/settings/global", get(edit_global_settings))
        .route("/settings/nodes/{id}", get(edit_node_settings))
        .route("/ui/schedules", post(create_schedule))
        .route("/ui/schedules/{id}", post(update_schedule))
        .route("/ui/schedules/{id}/run", post(run_now))
        .route("/ui/schedules/{id}/toggle", post(toggle_schedule))
        .route("/ui/schedules/{id}/webhook", post(rotate_webhook))
        .route("/ui/runs/{id}/cancel", post(cancel_run))
        .route("/ui/runs/{id}/retry", post(retry_run))
        .route("/ui/batches/{id}/cancel", post(cancel_batch))
        .route("/ui/batches/{id}/retrigger", post(retrigger_batch))
        .route("/ui/settings/global", post(save_global_settings))
        .route("/ui/settings/nodes/{id}", post(save_node_settings))
        .route("/ui/dashboard", post(save_dashboard))
        .route("/ui/nodes/{id}/quarantine", post(quarantine_node))
        .route("/ui/nodes/{id}/reset", post(reset_node_health))
        .route("/ui/input-health/probe", post(release_input_probe))
        .route("/ui/settings/lock/renew", post(renew_settings_lock))
        .route("/ui/settings/lock/release", post(release_settings_lock))
        .route("/ui/settings/lock/force", post(force_release_settings_lock))
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
</style></head><body><header><strong>Task Control / {{ node_name }}</strong><nav><a href="/">Overview</a><a href="/schedules">Schedules</a><a href="/batches">Batches</a><a href="/runs">Runs</a><a href="/blueprints">Blueprints</a><a href="/nodes">Nodes</a><a href="/settings/global">Settings</a></nav><form method="get" action="/search" class="actions" style="margin-left:auto"><input aria-label="Search by ID" name="q" minlength="8" placeholder="ID or digest" required><button type="submit">Search</button></form><form method="post" action="/logout"><input type="hidden" name="csrf" value="{{ csrf }}"><button type="submit">Sign out</button></form></header><main>{{ content|safe }}</main><footer>Single coordinator authority · at-least-once delivery</footer></body></html>"##,
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
    render_login("")
}

#[derive(Deserialize)]
struct LoginForm {
    token: String,
}

async fn login(State(state): State<AppState>, Form(form): Form<LoginForm>) -> Response {
    if !state.auth.verify_secret(&form.token) {
        let mut response = render_login("Invalid token");
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        return response;
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
    let result = async {
        let schedules = state.store.list_schedules().await?;
        let agents = state.store.list_agents().await?;
        let dashboard = state.store.get_dashboard().await?;
        let queued: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE state='queued'")
                .fetch_one(state.store.pool())
                .await?;
        let running: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE state='running'")
                .fetch_one(state.store.pool())
                .await?;
        let active_batches: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM batches WHERE state IN ('scheduled','collecting','running')",
        )
        .fetch_one(state.store.pool())
        .await?;
        let recent_failures: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM runs WHERE state='failed' AND updated_at>=?",
        )
        .bind((Utc::now() - chrono::Duration::hours(24)).to_rfc3339())
        .fetch_one(state.store.pool())
        .await?;
        let quarantined: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM node_health WHERE state IN ('auto_quarantined','manual_quarantined')",
        )
        .fetch_one(state.store.pool())
        .await?;
        Ok::<_, anyhow::Error>((
            schedules,
            agents,
            dashboard,
            queued,
            running,
            active_batches,
            recent_failures,
            quarantined,
        ))
    }
    .await;
    let (
        schedules,
        agents,
        dashboard,
        queued,
        running,
        active_batches,
        recent_failures,
        quarantined,
    ) = match result {
        Ok(data) => data,
        Err(error) => return error_page(&session.csrf, &error),
    };
    let online = agents.iter().filter(|agent| agent.connected).count();
    let telemetry = scheduler_telemetry::status();
    let configured = dashboard
        .config
        .widgets
        .iter()
        .collect::<std::collections::HashSet<_>>();
    let mut metrics = String::new();
    if configured.contains(&scheduler_core::DashboardWidget::ClusterCapacity) {
        let capacity: u32 = agents.iter().map(|agent| agent.capacity).sum();
        let active: u32 = agents.iter().map(|agent| agent.running).sum();
        metrics.push_str(&format!(
            r#"<div class="metric">Cluster capacity<b>{}/{}</b><span class="muted">{} of {} nodes online</span></div>"#,
            active,
            capacity,
            online,
            agents.len()
        ));
    }
    if configured.contains(&scheduler_core::DashboardWidget::ActiveBatches) {
        metrics.push_str(&format!(
            r#"<div class="metric">Active batches<b>{active_batches}</b></div>"#
        ));
    }
    if configured.contains(&scheduler_core::DashboardWidget::RecentFailures) {
        metrics.push_str(&format!(
            r#"<div class="metric">Failures / 24h<b>{recent_failures}</b></div>"#
        ));
    }
    if configured.contains(&scheduler_core::DashboardWidget::QuarantinedNodes) {
        metrics.push_str(&format!(
            r#"<div class="metric">Quarantined nodes<b class="{}">{quarantined}</b></div>"#,
            if quarantined == 0 { "good" } else { "bad" }
        ));
    }
    if configured.contains(&scheduler_core::DashboardWidget::ConnectorHealth) {
        let connector_count = state.collection_sources.connector_count();
        metrics.push_str(&format!(
            r#"<div class="metric">Connector health<b style="font-size:18px">{}</b><span class="muted">{} configured. Calls expose success/error and latency telemetry; failures are reported per batch and never fall back.</span></div>"#,
            if connector_count == 0 { "disabled" } else { "configured" },
            connector_count,
        ));
    }
    if configured.contains(&scheduler_core::DashboardWidget::TelemetryHealth) {
        let health = if !telemetry.configured {
            "local only"
        } else if telemetry.last_error_class.is_some() {
            "degraded"
        } else {
            "configured"
        };
        let class = if telemetry.last_error_class.is_some() {
            "bad"
        } else {
            "good"
        };
        let failed_signals = if telemetry.failed_signals.is_empty() {
            "none".to_owned()
        } else {
            telemetry.failed_signals.join(", ")
        };
        metrics.push_str(&format!(
            r#"<div class="metric">Telemetry<b class="{class}" style="font-size:18px">{health}</b><span class="muted">{} · {} dropped · {} export batches in flight · failed signals: {}. Collector outages never block scheduling.</span></div>"#,
            esc(&telemetry.protocol),
            telemetry.dropped_telemetry,
            telemetry.export_batches_in_flight,
            esc(&failed_signals),
        ));
    }
    let mut schedule_cards = String::new();
    if configured.contains(&scheduler_core::DashboardWidget::SelectedSchedules) {
        for schedule_id in &dashboard.config.schedule_ids {
            if let Some(schedule) = schedules
                .iter()
                .find(|schedule| schedule.id == *schedule_id)
            {
                match schedule_statistics(state.store.pool(), schedule.id, 24).await {
                    Ok(stats) => schedule_cards.push_str(&schedule_card(schedule, &stats)),
                    Err(error) => return error_page(&session.csrf, &error),
                }
            }
        }
    }
    let content = format!(
        r#"<div class="actions"><h1 style="margin-right:auto">Cluster overview</h1><a class="button" href="/dashboard/edit">Customize</a></div><div class="grid"><div class="metric">Schedules<b>{}</b></div><div class="metric">Queued<b>{queued}</b></div><div class="metric">Running<b>{running}</b></div>{metrics}</div><h2>Selected schedule health</h2><div class="grid">{schedule_cards}</div><h2>System posture</h2><div class="panel"><p><span class="badge good">Coordinator authoritative</span> Durable SQLite state and leased delivery are active.</p><p class="muted">Dashboard settings revision r{} is shared through every node UI.</p></div>"#,
        schedules.len(),
        dashboard.revision,
    );
    page("Overview", &session.csrf, &content)
}

#[derive(Debug, Clone)]
struct ScheduleStatistics {
    queued: i64,
    running: i64,
    succeeded: i64,
    business_failed: i64,
    infrastructure_failed: i64,
    cancelled: i64,
    invalid_items: i64,
    poisoned_items: i64,
    retries: i64,
    node_diversity: i64,
    p50_ms: Option<i64>,
    p95_ms: Option<i64>,
    last_execution: Option<String>,
    last_success: Option<String>,
    last_failure: Option<String>,
}

impl ScheduleStatistics {
    fn terminal(&self) -> i64 {
        self.succeeded + self.business_failed + self.infrastructure_failed + self.cancelled
    }

    fn success_rate(&self) -> f64 {
        let terminal = self.terminal();
        if terminal == 0 {
            0.0
        } else {
            self.succeeded as f64 * 100.0 / terminal as f64
        }
    }
}

async fn schedule_statistics(
    pool: &sqlx::SqlitePool,
    schedule_id: Uuid,
    window_hours: i64,
) -> anyhow::Result<ScheduleStatistics> {
    let cutoff = (Utc::now() - chrono::Duration::hours(window_hours)).to_rfc3339();
    let id = schedule_id.to_string();
    let counts = sqlx::query(
        "SELECT \
         SUM(CASE WHEN state='queued' THEN 1 ELSE 0 END) AS queued,\
         SUM(CASE WHEN state='running' THEN 1 ELSE 0 END) AS running,\
         MAX(scheduled_at) AS last_execution,\
         MAX(CASE WHEN state='succeeded' THEN updated_at END) AS last_success,\
         MAX(CASE WHEN state='failed' THEN updated_at END) AS last_failure \
         FROM runs WHERE schedule_id=?",
    )
    .bind(&id)
    .fetch_one(pool)
    .await?;
    let outcomes = sqlx::query(
        "SELECT \
         SUM(CASE WHEN r.state='succeeded' THEN 1 ELSE 0 END) AS succeeded,\
         SUM(CASE WHEN r.state='cancelled' THEN 1 ELSE 0 END) AS cancelled,\
         SUM(CASE WHEN r.state='failed' AND EXISTS (\
           SELECT 1 FROM attempts a WHERE a.id=(\
             SELECT latest.id FROM attempts latest WHERE latest.run_id=r.id \
             ORDER BY latest.finished_at DESC,latest.created_at DESC,latest.id DESC LIMIT 1\
           ) \
           AND json_extract(a.diagnostic_json,'$.code') IN ('process_exited_non_zero','excel_macro_returned_failure')\
         ) THEN 1 ELSE 0 END) AS business_failed,\
         SUM(CASE WHEN r.state='failed' AND NOT EXISTS (\
           SELECT 1 FROM attempts a WHERE a.id=(\
             SELECT latest.id FROM attempts latest WHERE latest.run_id=r.id \
             ORDER BY latest.finished_at DESC,latest.created_at DESC,latest.id DESC LIMIT 1\
           ) \
           AND json_extract(a.diagnostic_json,'$.code') IN ('process_exited_non_zero','excel_macro_returned_failure')\
         ) THEN 1 ELSE 0 END) AS infrastructure_failed,\
         SUM(CASE WHEN r.attempt_count>1 THEN r.attempt_count-1 ELSE 0 END) AS retries \
         FROM runs r WHERE r.schedule_id=? AND r.updated_at>=?",
    )
    .bind(&id)
    .bind(&cutoff)
    .fetch_one(pool)
    .await?;
    let batch_items = sqlx::query(
        "SELECT \
         COALESCE(SUM(invalid_item_count),0) AS invalid_items,\
         COALESCE(SUM(poisoned_item_count+held_item_count),0) AS poisoned_items \
         FROM batches WHERE schedule_id=? AND updated_at>=?",
    )
    .bind(&id)
    .bind(&cutoff)
    .fetch_one(pool)
    .await?;
    let node_diversity: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT a.agent_id) FROM attempts a JOIN runs r ON r.id=a.run_id \
         WHERE r.schedule_id=? AND a.created_at>=?",
    )
    .bind(&id)
    .bind(&cutoff)
    .fetch_one(pool)
    .await?;
    let durations = sqlx::query_scalar::<_, i64>(
        "SELECT duration_ms FROM (SELECT a.duration_ms,a.finished_at,a.id \
         FROM attempts a JOIN runs r ON r.id=a.run_id \
         WHERE r.schedule_id=? AND a.finished_at>=? AND a.duration_ms IS NOT NULL \
         ORDER BY a.finished_at DESC,a.id DESC LIMIT 10000) ORDER BY duration_ms",
    )
    .bind(&id)
    .bind(&cutoff)
    .fetch_all(pool)
    .await?;
    let percentile = |numerator: usize| {
        (!durations.is_empty()).then(|| {
            let index = ((durations.len() - 1) * numerator).div_ceil(100);
            durations[index.min(durations.len() - 1)]
        })
    };
    Ok(ScheduleStatistics {
        queued: counts.try_get::<Option<i64>, _>("queued")?.unwrap_or(0),
        running: counts.try_get::<Option<i64>, _>("running")?.unwrap_or(0),
        succeeded: outcomes
            .try_get::<Option<i64>, _>("succeeded")?
            .unwrap_or(0),
        business_failed: outcomes
            .try_get::<Option<i64>, _>("business_failed")?
            .unwrap_or(0),
        infrastructure_failed: outcomes
            .try_get::<Option<i64>, _>("infrastructure_failed")?
            .unwrap_or(0),
        cancelled: outcomes
            .try_get::<Option<i64>, _>("cancelled")?
            .unwrap_or(0),
        invalid_items: batch_items.try_get("invalid_items")?,
        poisoned_items: batch_items.try_get("poisoned_items")?,
        retries: outcomes.try_get::<Option<i64>, _>("retries")?.unwrap_or(0),
        node_diversity,
        p50_ms: percentile(50),
        p95_ms: percentile(95),
        last_execution: counts.try_get("last_execution")?,
        last_success: counts.try_get("last_success")?,
        last_failure: counts.try_get("last_failure")?,
    })
}

fn schedule_card(schedule: &scheduler_core::ScheduleView, stats: &ScheduleStatistics) -> String {
    let state = if stats.running > 0 {
        "Still running"
    } else if stats.poisoned_items > 0 {
        "Poisoned input"
    } else if stats.infrastructure_failed > 0 {
        "Infrastructure failure"
    } else if stats.business_failed > 0 {
        "Business failure"
    } else if stats.invalid_items > 0 && stats.succeeded > 0 {
        "Succeeded with quarantined items"
    } else if stats.succeeded > 0 {
        "Succeeded"
    } else if stats.cancelled > 0 {
        "Cancelled"
    } else {
        "No recent execution"
    };
    let class = if matches!(state, "Succeeded" | "Succeeded with quarantined items") {
        "good"
    } else if state == "Still running" || state == "No recent execution" {
        ""
    } else {
        "bad"
    };
    format!(
        r#"<div class="panel"><div class="actions"><strong>{}</strong><span class="badge {}">{}</span></div><p><code>{}</code></p><p>24h success <strong>{:.1}%</strong> · queued {} · running {} · retries {} · nodes {}</p><p>Business {} · infrastructure {} · invalid {} · poisoned/held {}</p><p>Duration p50 {} · p95 {}</p><p class="muted">Last execution {}<br>last success {}<br>last failure {}</p><a href="/schedules/{}/edit">Open schedule</a></div>"#,
        esc(&schedule.spec.name),
        class,
        state,
        schedule.id,
        stats.success_rate(),
        stats.queued,
        stats.running,
        stats.retries,
        stats.node_diversity,
        stats.business_failed,
        stats.infrastructure_failed,
        stats.invalid_items,
        stats.poisoned_items,
        stats
            .p50_ms
            .map_or_else(|| "—".into(), |value| format!("{value} ms")),
        stats
            .p95_ms
            .map_or_else(|| "—".into(), |value| format!("{value} ms")),
        esc(stats.last_execution.as_deref().unwrap_or("—")),
        esc(stats.last_success.as_deref().unwrap_or("—")),
        esc(stats.last_failure.as_deref().unwrap_or("—")),
        schedule.id,
    )
}

async fn edit_dashboard(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let (dashboard, schedules) = match async {
        Ok::<_, anyhow::Error>((
            state.store.get_dashboard().await?,
            state.store.list_schedules().await?,
        ))
    }
    .await
    {
        Ok(value) => value,
        Err(error) => return error_page(&session.csrf, &error),
    };
    let lock = match state.store.acquire_lock("dashboard", &session.id).await {
        Ok(lock) => lock,
        Err(acquire_error) => match state.store.current_lock("dashboard").await {
            Ok(Some(lock)) => {
                let json = match serde_json::to_string_pretty(&dashboard.config) {
                    Ok(json) => json,
                    Err(error) => return error_page(&session.csrf, &error),
                };
                return page(
                    "Customize dashboard (read only)",
                    &session.csrf,
                    &settings_read_only_form(
                        "Customize dashboard",
                        &json,
                        dashboard.revision,
                        "dashboard",
                        &lock,
                        &session,
                    ),
                );
            }
            Ok(None) => return error_page(&session.csrf, &acquire_error),
            Err(error) => return error_page(&session.csrf, &error),
        },
    };
    let schedule_ids = dashboard
        .config
        .schedule_ids
        .iter()
        .map(Uuid::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let mut available = String::new();
    for schedule in schedules {
        available.push_str(&format!(
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
            schedule.id,
            esc(&schedule.spec.name),
            if dashboard.config.schedule_ids.contains(&schedule.id) {
                "selected"
            } else {
                "—"
            }
        ));
    }
    let checked = |widget| {
        if dashboard.config.widgets.contains(&widget) {
            "checked"
        } else {
            ""
        }
    };
    let content = format!(
        r#"<h1>Customize dashboard</h1><div class="notice">This cluster-wide document is locked for two minutes and revision-fenced at r{}. Put one schedule ID per line; line order is card order.</div><form method="post" action="/ui/dashboard"><input type="hidden" name="csrf" value="{}"><input type="hidden" name="revision" value="{}"><input type="hidden" name="lock_token" value="{}"><label>Selected schedule IDs, in display order<textarea name="schedule_ids" spellcheck="false">{}</textarea></label><h2>Widgets</h2><div class="panel"><label><input style="width:auto" type="checkbox" name="cluster_capacity" value="yes" {}> Cluster capacity</label><label><input style="width:auto" type="checkbox" name="active_batches" value="yes" {}> Active batches</label><label><input style="width:auto" type="checkbox" name="recent_failures" value="yes" {}> Recent failures</label><label><input style="width:auto" type="checkbox" name="quarantined_nodes" value="yes" {}> Quarantined nodes</label><label><input style="width:auto" type="checkbox" name="connector_health" value="yes" {}> Connector health</label><label><input style="width:auto" type="checkbox" name="telemetry_health" value="yes" {}> Telemetry health</label><label><input style="width:auto" type="checkbox" name="selected_schedules" value="yes" {}> Selected schedules</label></div><div class="actions"><button>Save dashboard</button><a href="/">Cancel</a></div></form><h2>Available schedules</h2><table><thead><tr><th>ID</th><th>Name</th><th>Current selection</th></tr></thead><tbody>{available}</tbody></table><script>const lockBody=new URLSearchParams({{csrf:'{}',document_key:'dashboard',lock_token:'{}'}});setInterval(()=>fetch('/ui/settings/lock/renew',{{method:'POST',headers:{{'Content-Type':'application/x-www-form-urlencoded'}},body:lockBody}}),30000);addEventListener('pagehide',()=>fetch('/ui/settings/lock/release',{{method:'POST',headers:{{'Content-Type':'application/x-www-form-urlencoded'}},body:lockBody,keepalive:true}}));</script>"#,
        dashboard.revision,
        esc(&session.csrf),
        dashboard.revision,
        esc(&lock.lock_token),
        esc(&schedule_ids),
        checked(scheduler_core::DashboardWidget::ClusterCapacity),
        checked(scheduler_core::DashboardWidget::ActiveBatches),
        checked(scheduler_core::DashboardWidget::RecentFailures),
        checked(scheduler_core::DashboardWidget::QuarantinedNodes),
        checked(scheduler_core::DashboardWidget::ConnectorHealth),
        checked(scheduler_core::DashboardWidget::TelemetryHealth),
        checked(scheduler_core::DashboardWidget::SelectedSchedules),
        esc(&session.csrf),
        esc(&lock.lock_token),
    );
    page("Customize dashboard", &session.csrf, &content)
}

#[derive(Deserialize)]
struct DashboardForm {
    csrf: String,
    revision: i64,
    lock_token: String,
    schedule_ids: String,
    cluster_capacity: Option<String>,
    active_batches: Option<String>,
    recent_failures: Option<String>,
    quarantined_nodes: Option<String>,
    connector_health: Option<String>,
    telemetry_health: Option<String>,
    selected_schedules: Option<String>,
}

async fn save_dashboard(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<DashboardForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let result = async {
        let schedule_ids = form
            .schedule_ids
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(Uuid::parse_str)
            .collect::<Result<Vec<_>, _>>()?;
        let mut widgets = Vec::new();
        for (selected, widget) in [
            (
                form.cluster_capacity.is_some(),
                scheduler_core::DashboardWidget::ClusterCapacity,
            ),
            (
                form.active_batches.is_some(),
                scheduler_core::DashboardWidget::ActiveBatches,
            ),
            (
                form.recent_failures.is_some(),
                scheduler_core::DashboardWidget::RecentFailures,
            ),
            (
                form.quarantined_nodes.is_some(),
                scheduler_core::DashboardWidget::QuarantinedNodes,
            ),
            (
                form.connector_health.is_some(),
                scheduler_core::DashboardWidget::ConnectorHealth,
            ),
            (
                form.telemetry_health.is_some(),
                scheduler_core::DashboardWidget::TelemetryHealth,
            ),
            (
                form.selected_schedules.is_some(),
                scheduler_core::DashboardWidget::SelectedSchedules,
            ),
        ] {
            if selected {
                widgets.push(widget);
            }
        }
        let config = scheduler_core::DashboardConfig {
            schedule_ids,
            widgets,
        };
        config.validate()?;
        let known = state
            .store
            .list_schedules()
            .await?
            .into_iter()
            .map(|schedule| schedule.id)
            .collect::<std::collections::HashSet<_>>();
        if config.schedule_ids.iter().any(|id| !known.contains(id)) {
            anyhow::bail!("dashboard contains a schedule ID which does not exist");
        }
        state
            .store
            .update_dashboard(&config, form.revision, &form.lock_token)
            .await?;
        state
            .store
            .release_lock("dashboard", &form.lock_token, false)
            .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    match result {
        Ok(()) => Redirect::to("/").into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 200;

#[derive(Debug, Default, Deserialize)]
struct PageQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

struct Page<T> {
    items: Vec<T>,
    next_cursor: Option<String>,
    limit: usize,
}

#[cfg(test)]
fn paginate<T>(
    items: Vec<T>,
    query: &PageQuery,
    key: impl Fn(&T) -> String,
) -> anyhow::Result<Page<T>> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let start = match query.cursor.as_deref() {
        Some(cursor) => items
            .iter()
            .position(|item| key(item) == cursor)
            .map(|position| position + 1)
            .ok_or_else(|| anyhow::anyhow!("pagination cursor is no longer available"))?,
        None => 0,
    };
    let mut page_items = items
        .into_iter()
        .skip(start)
        .take(limit + 1)
        .collect::<Vec<_>>();
    let has_more = page_items.len() > limit;
    if has_more {
        page_items.pop();
    }
    let next_cursor = has_more.then(|| page_items.last().map(&key)).flatten();
    Ok(Page {
        items: page_items,
        next_cursor,
        limit,
    })
}

fn pagination_controls(path: &str, page: &Page<impl Sized>) -> String {
    page.next_cursor
        .as_ref()
        .map(|cursor| {
            format!(
                r#"<nav class="actions" aria-label="Pagination"><a class="button" href="{}?cursor={}&amp;limit={}">Next page</a><span class="muted">Showing up to {} rows</span></nav>"#,
                path,
                esc(cursor),
                page.limit,
                page.limit,
            )
        })
        .unwrap_or_else(|| {
            format!(
                r#"<p class="muted">End of results · showing up to {} rows per page</p>"#,
                page.limit
            )
        })
}

fn pagination_controls_with_filter(
    path: &str,
    page: &Page<impl Sized>,
    filter_name: &str,
    filter_value: Option<&str>,
) -> String {
    let Some(cursor) = page.next_cursor.as_ref() else {
        return format!(
            r#"<p class="muted">End of results · showing up to {} rows per page</p>"#,
            page.limit
        );
    };
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("cursor", cursor);
    serializer.append_pair("limit", &page.limit.to_string());
    if let Some(value) = filter_value.filter(|value| !value.is_empty()) {
        serializer.append_pair(filter_name, value);
    }
    format!(
        r#"<nav class="actions" aria-label="Pagination"><a class="button" href="{}?{}">Next page</a><span class="muted">Showing up to {} rows</span></nav>"#,
        path,
        esc(&serializer.finish()),
        page.limit,
    )
}

async fn schedules(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    match paged_schedules(&state, &query).await {
        Ok(page_data) => {
            let mut rows = String::new();
            for item in &page_data.items {
                let cron = item
                    .spec
                    .cron
                    .as_ref()
                    .map(|cron| format!("{} · {}", cron.expression, cron.timezone))
                    .unwrap_or_else(|| "manual/webhook".into());
                rows.push_str(&format!(r#"<tr><td><a href="/schedules/{}/edit">{}</a></td><td>{}</td><td><span class="badge {}">{}</span></td><td>{}</td><td>r{}</td><td><div class="actions"><form method="post" action="/ui/schedules/{}/run"><input type="hidden" name="csrf" value="{}"><button>Run now</button></form><form method="post" action="/ui/schedules/{}/toggle"><input type="hidden" name="csrf" value="{}"><button>{}</button></form></div></td></tr>"#, item.id, esc(&item.spec.name), esc(&cron), if item.spec.enabled {"good"} else {"bad"}, if item.spec.enabled {"enabled"} else {"paused"}, if item.spec.webhook_enabled {"yes"} else {"no"}, item.revision, item.id, session.csrf, item.id, session.csrf, if item.spec.enabled {"Pause"} else {"Resume"}));
            }
            let content = format!(
                r#"<div class="actions"><h1 style="margin-right:auto">Schedules</h1><a class="button" href="/schedules/new">New schedule</a></div><table><thead><tr><th>Name</th><th>Trigger</th><th>State</th><th>Webhook</th><th>Revision</th><th>Actions</th></tr></thead><tbody>{rows}</tbody></table>{}"#,
                pagination_controls("/schedules", &page_data)
            );
            page("Schedules", &session.csrf, &content)
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn paged_schedules(
    state: &AppState,
    query: &PageQuery,
) -> anyhow::Result<Page<scheduler_core::ScheduleView>> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let cursor = query.cursor.as_deref().map(decode_run_cursor).transpose()?;
    let mut items = state
        .store
        .list_schedules_page(
            cursor.as_ref().map(|cursor| cursor.created_at.as_str()),
            cursor.as_ref().map(|cursor| cursor.id.as_str()),
            (limit + 1) as u32,
        )
        .await?;
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next_cursor = if has_more {
        items
            .last()
            .map(|schedule| {
                encode_run_cursor(&RunCursor {
                    created_at: schedule
                        .created_at
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    id: schedule.id.to_string(),
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(Page {
        items,
        next_cursor,
        limit,
    })
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
    let item = match state.store.get_schedule(id).await {
        Ok(Some(item)) => item,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return error_page(&session.csrf, &error),
    };
    let stats = async {
        Ok::<_, anyhow::Error>([
            (
                "24 hours",
                schedule_statistics(state.store.pool(), id, 24).await?,
            ),
            (
                "7 days",
                schedule_statistics(state.store.pool(), id, 24 * 7).await?,
            ),
            (
                "30 days",
                schedule_statistics(state.store.pool(), id, 24 * 30).await?,
            ),
        ])
    }
    .await;
    let stats = match stats {
        Ok(stats) => stats,
        Err(error) => return error_page(&session.csrf, &error),
    };
    page(
        "Edit schedule",
        &session.csrf,
        &format!(
            "{}{}",
            schedule_statistics_table(&stats),
            schedule_form(Some(&item), &session.csrf, &settings.default_timezone)
        ),
    )
}

fn schedule_statistics_table(stats: &[(&str, ScheduleStatistics)]) -> String {
    let mut rows = String::new();
    for (label, stats) in stats {
        rows.push_str(&format!(
            r#"<tr><td>{label}</td><td>{:.1}%</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}/{}</td><td>{}</td><td>{}</td><td>{}</td></tr>"#,
            stats.success_rate(),
            stats.succeeded,
            stats.business_failed,
            stats.infrastructure_failed,
            stats.cancelled,
            stats.queued,
            stats.running,
            stats.invalid_items,
            stats.poisoned_items,
            stats.retries,
        ));
    }
    format!(
        r#"<h1>Schedule execution health</h1><table><thead><tr><th>Window</th><th>Success</th><th>Succeeded</th><th>Business failure</th><th>Infrastructure failure</th><th>Cancelled</th><th>Queued/running</th><th>Invalid</th><th>Poisoned/held</th><th>Retries</th></tr></thead><tbody>{rows}</tbody></table>"#
    )
}

#[derive(Debug, Deserialize)]
struct ScheduleForm {
    csrf: String,
    name: String,
    blueprint_ref: String,
    parameters_ref: String,
    #[serde(default)]
    parameter_collection_uri: String,
    #[serde(default)]
    collection_page_size: Option<u32>,
    #[serde(default)]
    collection_max_items: Option<u32>,
    #[serde(default)]
    collection_max_active_runs: Option<u32>,
    #[serde(default)]
    collection_poison_distinct_nodes: Option<u32>,
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
        let parameter_collection = if self.parameter_collection_uri.trim().is_empty() {
            None
        } else {
            let collection = scheduler_core::ParameterCollectionSpec {
                source_ref: ArtifactRef {
                    uri: self.parameter_collection_uri,
                },
                page_size: self.collection_page_size.unwrap_or(500),
                max_items: self.collection_max_items.unwrap_or(10_000),
                max_active_runs: self.collection_max_active_runs.unwrap_or(32),
                poison_distinct_nodes: self.collection_poison_distinct_nodes.unwrap_or(2),
            };
            collection.validate()?;
            Some(collection)
        };
        Ok(ScheduleSpec {
            name: self.name,
            blueprint_ref: ArtifactRef {
                uri: self.blueprint_ref,
            },
            parameters_ref: ArtifactRef {
                uri: self.parameters_ref,
            },
            parameter_collection,
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
        if schedule.view.spec.parameter_collection.is_some() {
            let batch = crate::collection_runtime::create_batch_from_schedule(
                &state,
                &schedule,
                &serde_json::json!({}),
                "manual",
                chrono::Utc::now(),
                None,
            )
            .await?;
            Ok::<_, anyhow::Error>(("batch", batch.id))
        } else {
            let run = create_run_from_schedule(
                &state,
                &schedule,
                &serde_json::json!({}),
                "manual",
                chrono::Utc::now(),
                None,
            )
            .await?;
            Ok(("run", run.id))
        }
    }
    .await;
    match result {
        Ok(("batch", id)) => Redirect::to(&format!("/batches/{id}")).into_response(),
        Ok((_, id)) => Redirect::to(&format!("/runs/{id}")).into_response(),
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
    let enabled = match state.store.get_schedule(id).await {
        Ok(Some(item)) => !item.spec.enabled,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return error_page(&session.csrf, &error),
    };
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

#[derive(Debug, Default, Deserialize)]
struct BatchPageQuery {
    cursor: Option<String>,
    limit: Option<usize>,
    provider_key: Option<String>,
}

async fn batches(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<BatchPageQuery>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let page_data = match paged_batches(&state, &query).await {
        Ok(page) => page,
        Err(error) => return error_page(&session.csrf, &error),
    };
    let mut rows = String::new();
    for batch in &page_data.items {
        let state_name = serde_json::to_value(batch.state)
            .ok()
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .unwrap_or_else(|| "unknown".into());
        let class = if state_name == "succeeded" {
            "good"
        } else if matches!(state_name.as_str(), "failed" | "completed_with_errors") {
            "bad"
        } else {
            ""
        };
        rows.push_str(&format!(
            r#"<tr><td><a href="/batches/{}"><code>{}</code></a></td><td><a href="/schedules/{}/edit"><code>{}</code></a><br>r{}</td><td><span class="badge {}">{}</span></td><td>{}</td><td>{} total · {} valid · {} invalid · {} poisoned · {} held</td><td>{}</td></tr>"#,
            batch.id,
            &batch.id.to_string()[..8],
            batch.schedule_id,
            &batch.schedule_id.to_string()[..8],
            batch.schedule_revision,
            class,
            esc(&state_name),
            esc(&batch.trigger_kind),
            batch.item_count,
            batch.valid_item_count,
            batch.invalid_item_count,
            batch.poisoned_item_count,
            batch.held_item_count,
            batch.updated_at,
        ));
    }
    page(
        "Batches",
        &session.csrf,
        &format!(
            r#"<h1>Parameter collection batches</h1><table><thead><tr><th>Batch</th><th>Schedule revision</th><th>State</th><th>Trigger</th><th>Items</th><th>Updated</th></tr></thead><tbody>{rows}</tbody></table>{}"#,
            pagination_controls("/batches", &page_data)
        ),
    )
}

async fn batch_detail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Query(query): Query<BatchPageQuery>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let batch = match state.store.get_batch(id).await {
        Ok(Some(batch)) => batch,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return error_page(&session.csrf, &error),
    };
    let page_data = match paged_batch_items(&state, id, &query).await {
        Ok(page) => page,
        Err(error) => return error_page(&session.csrf, &error),
    };
    let mut rows = String::new();
    for item in &page_data.items {
        let item_state = serde_json::to_value(item.state)
            .ok()
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .unwrap_or_else(|| "unknown".into());
        let run = item.run_id.map_or_else(
            || "—".into(),
            |run| {
                format!(
                    r#"<a href="/runs/{run}"><code>{}</code></a>"#,
                    &run.to_string()[..8]
                )
            },
        );
        rows.push_str(&format!(
            r#"<tr><td>{}</td><td><code>{}</code></td><td><span class="badge {}">{}</span></td><td>{}</td><td>{}</td><td>{}</td></tr>"#,
            item.item_index,
            &item.id.to_string()[..8],
            if item_state == "succeeded" { "good" } else if matches!(item_state.as_str(), "failed" | "invalid" | "poisoned" | "held") { "bad" } else { "" },
            esc(&item_state),
            esc(item.failure_code.as_deref().unwrap_or("—")),
            run,
            item.updated_at,
        ));
    }
    let state_name = serde_json::to_value(batch.view.state)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".into());
    page(
        &format!("Batch {id}"),
        &session.csrf,
        &format!(
            r#"<div class="actions"><h1 style="margin-right:auto">Batch <code>{id}</code></h1><form method="post" action="/ui/batches/{id}/cancel"><input type="hidden" name="csrf" value="{}"><button class="danger">Cancel batch</button></form><form method="post" action="/ui/batches/{id}/retrigger"><input type="hidden" name="csrf" value="{}"><button>Retrigger</button></form></div><div class="grid"><div class="metric">State<b style="font-size:18px">{}</b></div><div class="metric">Items<b>{}</b></div><div class="metric">Valid / invalid<b>{}/{}</b></div><div class="metric">Poisoned / held<b>{}/{}</b></div></div><div class="notice">Failure code: <code>{}</code>. Provider keys and parameter values are excluded from this page; exact provider-key lookup is authenticated and returns item metadata only.</div><form method="get"><label>Exact provider key<input name="provider_key" value="{}"></label><button>Find item</button></form><h2>Items</h2><table><thead><tr><th>Index</th><th>Item ID</th><th>State</th><th>Safe failure code</th><th>Run</th><th>Updated</th></tr></thead><tbody>{rows}</tbody></table>{}"#,
            esc(&session.csrf),
            esc(&session.csrf),
            esc(&state_name),
            batch.view.item_count,
            batch.view.valid_item_count,
            batch.view.invalid_item_count,
            batch.view.poisoned_item_count,
            batch.view.held_item_count,
            esc(batch.view.failure_code.as_deref().unwrap_or("none")),
            esc(query.provider_key.as_deref().unwrap_or("")),
            pagination_controls_with_filter(
                &format!("/batches/{id}"),
                &page_data,
                "provider_key",
                query.provider_key.as_deref(),
            ),
        ),
    )
}

async fn paged_batches(
    state: &AppState,
    query: &BatchPageQuery,
) -> anyhow::Result<Page<BatchView>> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let cursor = query.cursor.as_deref().map(decode_run_cursor).transpose()?;
    let created_at = cursor.as_ref().map(|cursor| cursor.created_at.as_str());
    let cursor_id = cursor.as_ref().map(|cursor| cursor.id.as_str());
    let rows = sqlx::query(
        "SELECT * FROM batches WHERE (? IS NULL OR created_at<? OR (created_at=? AND id<?)) \
         ORDER BY created_at DESC,id DESC LIMIT ?",
    )
    .bind(created_at)
    .bind(created_at)
    .bind(created_at)
    .bind(cursor_id)
    .bind((limit + 1) as i64)
    .fetch_all(state.store.pool())
    .await?;
    let mut items = rows
        .into_iter()
        .map(batch_view_from_ui_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next_cursor = if has_more {
        items
            .last()
            .map(|batch| {
                encode_run_cursor(&RunCursor {
                    created_at: batch
                        .created_at
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    id: batch.id.to_string(),
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(Page {
        items,
        next_cursor,
        limit,
    })
}

fn batch_view_from_ui_row(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<BatchView> {
    Ok(BatchView {
        id: Uuid::parse_str(&row.try_get::<String, _>("id")?)?,
        schedule_id: Uuid::parse_str(&row.try_get::<String, _>("schedule_id")?)?,
        schedule_revision: row.try_get("schedule_revision")?,
        state: scheduler_core::BatchState::parse(&row.try_get::<String, _>("state")?)?,
        trigger_kind: row.try_get("trigger_kind")?,
        scheduled_at: parse_ui_time(row.try_get("scheduled_at")?)?,
        item_count: row.try_get::<i64, _>("item_count")?.try_into()?,
        valid_item_count: row.try_get::<i64, _>("valid_item_count")?.try_into()?,
        invalid_item_count: row.try_get::<i64, _>("invalid_item_count")?.try_into()?,
        poisoned_item_count: row.try_get::<i64, _>("poisoned_item_count")?.try_into()?,
        held_item_count: row.try_get::<i64, _>("held_item_count")?.try_into()?,
        failure_code: row.try_get("failure_code")?,
        created_at: parse_ui_time(row.try_get("created_at")?)?,
        updated_at: parse_ui_time(row.try_get("updated_at")?)?,
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct BatchItemUiCursor {
    item_index: u32,
    id: String,
}

async fn paged_batch_items(
    state: &AppState,
    batch_id: Uuid,
    query: &BatchPageQuery,
) -> anyhow::Result<Page<BatchItemView>> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    if query
        .provider_key
        .as_ref()
        .is_some_and(|key| key.is_empty() || key.len() > 256)
    {
        anyhow::bail!("provider key must contain between 1 and 256 bytes");
    }
    let provider_digest = query
        .provider_key
        .as_deref()
        .map(|key| {
            state.cipher.input_fingerprint(
                "collection-provider-key",
                &serde_json::Value::String(key.to_owned()),
            )
        })
        .transpose()?;
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_batch_item_cursor)
        .transpose()?;
    let item_index = cursor.as_ref().map(|cursor| i64::from(cursor.item_index));
    let cursor_id = cursor.as_ref().map(|cursor| cursor.id.as_str());
    let rows = sqlx::query(
        "SELECT id,batch_id,item_index,parameters_digest,state,failure_code,run_id,created_at,updated_at \
         FROM batch_items WHERE batch_id=? AND (? IS NULL OR provider_key_hmac=?) \
         AND (? IS NULL OR item_index>? OR (item_index=? AND id>?)) \
         ORDER BY item_index,id LIMIT ?",
    )
    .bind(batch_id.to_string())
    .bind(&provider_digest)
    .bind(&provider_digest)
    .bind(item_index)
    .bind(item_index)
    .bind(cursor_id)
    .bind((limit + 1) as i64)
    .fetch_all(state.store.pool())
    .await?;
    let mut items = rows
        .into_iter()
        .map(batch_item_view_from_ui_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next_cursor = if has_more {
        items
            .last()
            .map(|item| {
                encode_batch_item_cursor(&BatchItemUiCursor {
                    item_index: item.item_index,
                    id: item.id.to_string(),
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(Page {
        items,
        next_cursor,
        limit,
    })
}

fn batch_item_view_from_ui_row(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<BatchItemView> {
    Ok(BatchItemView {
        id: Uuid::parse_str(&row.try_get::<String, _>("id")?)?,
        batch_id: Uuid::parse_str(&row.try_get::<String, _>("batch_id")?)?,
        item_index: row.try_get::<i64, _>("item_index")?.try_into()?,
        parameters_digest: row.try_get("parameters_digest")?,
        state: scheduler_core::BatchItemState::parse(&row.try_get::<String, _>("state")?)?,
        failure_code: row.try_get("failure_code")?,
        run_id: row
            .try_get::<Option<String>, _>("run_id")?
            .map(|id| Uuid::parse_str(&id))
            .transpose()?,
        created_at: parse_ui_time(row.try_get("created_at")?)?,
        updated_at: parse_ui_time(row.try_get("updated_at")?)?,
    })
}

fn encode_batch_item_cursor(cursor: &BatchItemUiCursor) -> anyhow::Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor)?))
}

fn decode_batch_item_cursor(cursor: &str) -> anyhow::Result<BatchItemUiCursor> {
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| anyhow::anyhow!("invalid batch item pagination cursor"))?;
    let cursor: BatchItemUiCursor = serde_json::from_slice(&decoded)
        .map_err(|_| anyhow::anyhow!("invalid batch item pagination cursor"))?;
    Uuid::parse_str(&cursor.id)
        .map_err(|_| anyhow::anyhow!("invalid batch item pagination cursor"))?;
    Ok(cursor)
}

async fn cancel_batch(
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
    match state.store.cancel_batch(id).await {
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
            Redirect::to(&format!("/batches/{id}")).into_response()
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn retrigger_batch(
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
        state
            .store
            .retrigger_batch_snapshot(id, Uuid::new_v4(), Utc::now())
            .await
    }
    .await;
    match result {
        Ok(batch) => Redirect::to(&format!("/batches/{}", batch.id)).into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn runs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    match paged_runs(&state, &query).await {
        Ok(page_data) => {
            let mut rows = String::new();
            for item in &page_data.items {
                let state_name = item.state.as_str();
                rows.push_str(&format!(r#"<tr><td><a href="/runs/{}"><code>{}</code></a></td><td>{}</td><td><span class="badge {}">{}</span></td><td>{}</td><td>{}</td><td>{}</td></tr>"#, item.id, &item.id.to_string()[..8], item.trigger_kind, if state_name == "succeeded" {"good"} else if state_name == "failed" {"bad"} else {""}, state_name, item.scheduled_at, item.attempt_count, item.updated_at));
            }
            page(
                "Runs",
                &session.csrf,
                &format!(
                    r#"<h1>Runs</h1><table><thead><tr><th>Run</th><th>Trigger</th><th>State</th><th>Scheduled</th><th>Attempts</th><th>Updated</th></tr></thead><tbody>{rows}</tbody></table>{}"#,
                    pagination_controls("/runs", &page_data)
                ),
            )
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RunCursor {
    created_at: String,
    id: String,
}

async fn paged_runs(state: &AppState, query: &PageQuery) -> anyhow::Result<Page<RunView>> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let cursor = query.cursor.as_deref().map(decode_run_cursor).transpose()?;
    let cursor_created_at = cursor.as_ref().map(|cursor| cursor.created_at.as_str());
    let cursor_id = cursor.as_ref().map(|cursor| cursor.id.as_str());
    let rows = sqlx::query(
        "SELECT id,schedule_id,state,trigger_kind,scheduled_at,attempt_count,created_at,updated_at FROM runs WHERE (? IS NULL OR created_at < ? OR (created_at = ? AND id < ?)) ORDER BY created_at DESC,id DESC LIMIT ?",
    )
    .bind(cursor_created_at)
    .bind(cursor_created_at)
    .bind(cursor_created_at)
    .bind(cursor_id)
    .bind((limit + 1) as i64)
    .fetch_all(state.store.pool())
    .await?;
    let mut items = rows
        .into_iter()
        .map(run_view_from_ui_row)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next_cursor = if has_more {
        items
            .last()
            .map(|run| {
                encode_run_cursor(&RunCursor {
                    created_at: run
                        .created_at
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    id: run.id.to_string(),
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(Page {
        items,
        next_cursor,
        limit,
    })
}

fn run_view_from_ui_row(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<RunView> {
    let state = match row.try_get::<String, _>("state")?.as_str() {
        "queued" => RunState::Queued,
        "running" => RunState::Running,
        "succeeded" => RunState::Succeeded,
        "failed" => RunState::Failed,
        "cancelled" => RunState::Cancelled,
        other => anyhow::bail!("database contains unknown run state {other}"),
    };
    Ok(RunView {
        id: Uuid::parse_str(&row.try_get::<String, _>("id")?)?,
        schedule_id: Uuid::parse_str(&row.try_get::<String, _>("schedule_id")?)?,
        state,
        trigger_kind: row.try_get("trigger_kind")?,
        scheduled_at: parse_ui_time(row.try_get("scheduled_at")?)?,
        attempt_count: row.try_get::<i64, _>("attempt_count")?.try_into()?,
        created_at: parse_ui_time(row.try_get("created_at")?)?,
        updated_at: parse_ui_time(row.try_get("updated_at")?)?,
    })
}

fn parse_ui_time(value: String) -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&value)?.with_timezone(&Utc))
}

fn encode_run_cursor(cursor: &RunCursor) -> anyhow::Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor)?))
}

fn decode_run_cursor(cursor: &str) -> anyhow::Result<RunCursor> {
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| anyhow::anyhow!("invalid run pagination cursor"))?;
    let cursor: RunCursor = serde_json::from_slice(&decoded)
        .map_err(|_| anyhow::anyhow!("invalid run pagination cursor"))?;
    Uuid::parse_str(&cursor.id).map_err(|_| anyhow::anyhow!("invalid run pagination cursor"))?;
    DateTime::parse_from_rfc3339(&cursor.created_at)
        .map_err(|_| anyhow::anyhow!("invalid run pagination cursor"))?;
    Ok(cursor)
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

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: String,
}

struct SearchResult {
    kind: &'static str,
    id: String,
    href: String,
    context: String,
}

async fn search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SearchQuery>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let needle = query.q.trim();
    if needle.len() < 8
        || needle.len() > 128
        || !needle
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        let mut response = page(
            "Search",
            &session.csrf,
            "<h1>Search</h1><div class=\"notice\">Enter at least eight letters, numbers, dots, underscores, or hyphens.</div>",
        );
        *response.status_mut() = StatusCode::BAD_REQUEST;
        return response;
    }

    let like = format!("{}%", escape_like(needle));
    let result = search_existing_entities(&state, &like).await;
    match result {
        Ok(items) => {
            let mut rows = String::new();
            for item in items {
                rows.push_str(&format!(
                    r#"<tr><td>{}</td><td><a href="{}"><code>{}</code></a></td><td>{}</td></tr>"#,
                    esc(item.kind),
                    esc(&item.href),
                    esc(&item.id),
                    esc(&item.context),
                ));
            }
            if rows.is_empty() {
                rows.push_str("<tr><td colspan=\"3\" class=\"muted\">No matching IDs</td></tr>");
            }
            page(
                "Search",
                &session.csrf,
                &format!(
                    r#"<h1>ID search</h1><p>Results for <code>{}</code>. Prefix matches are shown without selecting an ambiguous result.</p><table><thead><tr><th>Type</th><th>ID</th><th>Context</th></tr></thead><tbody>{rows}</tbody></table>"#,
                    esc(needle)
                ),
            )
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn search_existing_entities(
    state: &AppState,
    like: &str,
) -> anyhow::Result<Vec<SearchResult>> {
    let mut results = Vec::new();
    for row in
        sqlx::query("SELECT id FROM schedules WHERE id LIKE ? ESCAPE '\\' ORDER BY id LIMIT 200")
            .bind(like)
            .fetch_all(state.store.pool())
            .await?
    {
        let id: String = row.try_get("id")?;
        results.push(SearchResult {
            kind: "schedule",
            href: format!("/schedules/{id}/edit"),
            id,
            context: "schedule definition".into(),
        });
    }
    for row in sqlx::query("SELECT id FROM runs WHERE id LIKE ? ESCAPE '\\' ORDER BY id LIMIT 200")
        .bind(like)
        .fetch_all(state.store.pool())
        .await?
    {
        let id: String = row.try_get("id")?;
        results.push(SearchResult {
            kind: "run",
            href: format!("/runs/{id}"),
            id,
            context: "task execution".into(),
        });
    }
    for row in sqlx::query(
        "SELECT id,run_id FROM attempts WHERE id LIKE ? ESCAPE '\\' ORDER BY id LIMIT 200",
    )
    .bind(like)
    .fetch_all(state.store.pool())
    .await?
    {
        let id: String = row.try_get("id")?;
        let run_id: String = row.try_get("run_id")?;
        results.push(SearchResult {
            kind: "attempt",
            href: format!("/runs/{run_id}"),
            id,
            context: format!("run {run_id}"),
        });
    }
    for row in sqlx::query(
        "SELECT id,schedule_id FROM batches WHERE id LIKE ? ESCAPE '\\' ORDER BY id LIMIT 200",
    )
    .bind(like)
    .fetch_all(state.store.pool())
    .await?
    {
        let id: String = row.try_get("id")?;
        let schedule_id: String = row.try_get("schedule_id")?;
        results.push(SearchResult {
            kind: "batch",
            href: format!("/batches/{id}"),
            id,
            context: format!("schedule {schedule_id}"),
        });
    }
    for row in sqlx::query(
        "SELECT id,batch_id FROM batch_items WHERE id LIKE ? ESCAPE '\\' ORDER BY id LIMIT 200",
    )
    .bind(like)
    .fetch_all(state.store.pool())
    .await?
    {
        let id: String = row.try_get("id")?;
        let batch_id: String = row.try_get("batch_id")?;
        results.push(SearchResult {
            kind: "batch item",
            href: format!("/batches/{batch_id}"),
            id,
            context: format!("batch {batch_id}"),
        });
    }
    for row in
        sqlx::query("SELECT id FROM agents WHERE id LIKE ? ESCAPE '\\' ORDER BY id LIMIT 200")
            .bind(like)
            .fetch_all(state.store.pool())
            .await?
    {
        let id: String = row.try_get("id")?;
        results.push(SearchResult {
            kind: "node",
            href: format!("/settings/nodes/{id}"),
            id,
            context: "agent node".into(),
        });
    }
    for row in sqlx::query(
        "SELECT digest,executor_kind FROM blueprint_revisions \
         WHERE digest LIKE ? ESCAPE '\\' ORDER BY digest LIMIT 200",
    )
    .bind(like)
    .fetch_all(state.store.pool())
    .await?
    {
        let digest: String = row.try_get("digest")?;
        let executor_kind: String = row.try_get("executor_kind")?;
        results.push(SearchResult {
            kind: "blueprint",
            href: "/blueprints".into(),
            id: digest,
            context: format!("{executor_kind} revision"),
        });
    }
    results.sort_by(|left, right| (left.kind, &left.id).cmp(&(right.kind, &right.id)));
    results.truncate(200);
    Ok(results)
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

async fn nodes(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    match paged_agents(&state, &query).await {
        Ok(page_data) => {
            let mut rows = String::new();
            for item in &page_data.items {
                let health = match state.store.node_health(&item.id).await {
                    Ok(health) => health,
                    Err(error) => return error_page(&session.csrf, &error),
                };
                rows.push_str(&node_row(item, health.as_ref()));
            }
            page(
                "Nodes",
                &session.csrf,
                &format!(
                    r#"<h1>Nodes</h1><table><thead><tr><th>Node</th><th>Connection</th><th>Health</th><th>Slots</th><th>Settings</th><th>Labels</th><th>Last seen</th></tr></thead><tbody>{rows}</tbody></table>{}"#,
                    pagination_controls("/nodes", &page_data)
                ),
            )
        }
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn paged_agents(state: &AppState, query: &PageQuery) -> anyhow::Result<Page<AgentView>> {
    if let Some(cursor) = query.cursor.as_deref() {
        scheduler_core::validate_agent_id(cursor)
            .map_err(|_| anyhow::anyhow!("invalid node pagination cursor"))?;
    }
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let mut items = state
        .store
        .list_agents_page(query.cursor.as_deref(), (limit + 1) as u32)
        .await?;
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next_cursor = has_more
        .then(|| items.last().map(|agent| agent.id.clone()))
        .flatten();
    Ok(Page {
        items,
        next_cursor,
        limit,
    })
}

async fn blueprints(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<PageQuery>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let page_data = match paged_blueprints(&state, &query).await {
        Ok(page) => page,
        Err(error) => return error_page(&session.csrf, &error),
    };
    let mut rows = String::new();
    for revision in &page_data.items {
        let digest_label = revision.digest.get(..12).unwrap_or(&revision.digest);
        rows.push_str(&format!(
            r#"<tr><td><code title="{}">{}</code><br><span class="muted">loaded {}</span></td><td>{}<br><code>{}</code></td><td><span class="badge">{}</span></td><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td>current schedules {}<br>retained revisions {}</td></tr>"#,
            esc(&revision.digest),
            esc(digest_label),
            revision.loaded_at,
            esc(&revision.source_ref),
            esc(revision.source_version.as_deref().unwrap_or("unversioned")),
            esc(&revision.executor_kind),
            esc(&serde_json::to_string(&revision.required_labels).unwrap_or_else(|_| "{}".into())),
            esc(&serde_json::to_string(&revision.parameter_schema).unwrap_or_else(|_| "{}".into())),
            esc(&serde_json::to_string(&revision.binding_declarations).unwrap_or_else(|_| "{}".into())),
            revision.current_schedule_count,
            revision.retained_schedule_revision_count,
        ));
    }
    page(
        "Blueprints",
        &session.csrf,
        &format!(
            r#"<h1>Loaded blueprint revisions</h1><div class="notice">Metadata only. Resolved parameter, environment, and secret values are never displayed. Older revisions remain catalogued while immutable work refers to them.</div><table><thead><tr><th>Digest / loaded</th><th>Source / version</th><th>Executor</th><th>Labels</th><th>Parameter names/types</th><th>Binding declarations</th><th>Usage</th></tr></thead><tbody>{rows}</tbody></table>{}"#,
            pagination_controls("/blueprints", &page_data)
        ),
    )
}

#[derive(Debug, Serialize, Deserialize)]
struct BlueprintCursor {
    loaded_at: String,
    digest: String,
}

async fn paged_blueprints(
    state: &AppState,
    query: &PageQuery,
) -> anyhow::Result<Page<scheduler_store::BlueprintRevisionView>> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_blueprint_cursor)
        .transpose()?;
    let mut items = state
        .store
        .list_blueprint_revisions_page(
            cursor.as_ref().map(|cursor| cursor.loaded_at.as_str()),
            cursor.as_ref().map(|cursor| cursor.digest.as_str()),
            (limit + 1) as u32,
        )
        .await?;
    let has_more = items.len() > limit;
    if has_more {
        items.pop();
    }
    let next_cursor = if has_more {
        items
            .last()
            .map(|revision| {
                encode_blueprint_cursor(&BlueprintCursor {
                    loaded_at: revision
                        .loaded_at
                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    digest: revision.digest.clone(),
                })
            })
            .transpose()?
    } else {
        None
    };
    Ok(Page {
        items,
        next_cursor,
        limit,
    })
}

fn encode_blueprint_cursor(cursor: &BlueprintCursor) -> anyhow::Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor)?))
}

fn decode_blueprint_cursor(cursor: &str) -> anyhow::Result<BlueprintCursor> {
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| anyhow::anyhow!("invalid blueprint pagination cursor"))?;
    let cursor: BlueprintCursor = serde_json::from_slice(&decoded)
        .map_err(|_| anyhow::anyhow!("invalid blueprint pagination cursor"))?;
    DateTime::parse_from_rfc3339(&cursor.loaded_at)
        .map_err(|_| anyhow::anyhow!("invalid blueprint pagination cursor"))?;
    if cursor.digest.len() != 64 || !cursor.digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("invalid blueprint pagination cursor");
    }
    Ok(cursor)
}

fn node_row(item: &AgentView, health: Option<&scheduler_store::NodeHealthView>) -> String {
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
    let health_state = health.map_or("healthy".into(), |health| {
        format!("{:?}", health.state).to_ascii_lowercase()
    });
    let health_class = if health.is_some_and(|health| {
        matches!(
            health.state,
            scheduler_core::health::NodeHealthState::AutoQuarantined
                | scheduler_core::health::NodeHealthState::ManualQuarantined
        )
    }) {
        "bad"
    } else if health.is_some_and(|health| {
        matches!(
            health.state,
            scheduler_core::health::NodeHealthState::Suspect
                | scheduler_core::health::NodeHealthState::Probation
        )
    }) {
        ""
    } else {
        "good"
    };
    format!(
        r#"<tr><td><a href="/nodes/{id}">{id}</a><br><span class="muted">{hostname}</span><br><a href="/settings/nodes/{id}">settings</a></td><td><span class="badge {connection_class}">{connection}</span></td><td><span class="badge {health_class}">{health_state}</span></td><td>{running}/{capacity}</td><td><span class="badge {settings_class}">{settings_label}</span><br>desired r{desired} / applied r{applied}{rejection}</td><td><code>{labels}</code></td><td>{last_seen}</td></tr>"#,
        id = esc(&item.id),
        hostname = esc(&item.hostname),
        connection_class = if item.connected { "good" } else { "bad" },
        connection = if item.connected { "online" } else { "offline" },
        health_class = health_class,
        health_state = esc(&health_state),
        running = item.running,
        capacity = item.capacity,
        desired = item.desired_settings_revision,
        applied = item.applied_settings_revision,
        labels = esc(&serde_json::to_string(&item.labels).unwrap_or_default()),
        last_seen = item.last_seen_at,
    )
}

async fn node_health_detail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if let Err(error) = scheduler_core::validate_agent_id(&id) {
        return error_page(&session.csrf, &error);
    }
    let result = async {
        let agent = state
            .store
            .list_agents()
            .await?
            .into_iter()
            .find(|agent| agent.id == id)
            .ok_or_else(|| anyhow::anyhow!("node not found"))?;
        let health = state.store.node_health(&id).await?;
        let evidence = state.store.list_health_evidence(Some(&id), 200).await?;
        Ok::<_, anyhow::Error>((agent, health, evidence))
    }
    .await;
    let (agent, health, evidence) = match result {
        Ok(value) => value,
        Err(error) => return error_page(&session.csrf, &error),
    };
    let state_name = health
        .as_ref()
        .and_then(|health| serde_json::to_value(health.state).ok())
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "healthy".into());
    let reason = health
        .as_ref()
        .and_then(|health| health.reason_code.as_deref())
        .unwrap_or("no health threshold has been crossed");
    let evaluation = health.as_ref().map_or_else(
        || "0 observations · 0 failed inputs · 0.0% failure rate".into(),
        |health| {
            format!(
                "{} observations · {} failed inputs across {} schedules · {:.1}% failure rate",
                health.evaluation.considered_observations,
                health.evaluation.distinct_failed_inputs,
                health.evaluation.distinct_schedules,
                health.evaluation.failure_rate * 100.0
            )
        },
    );
    let action = if health.as_ref().is_some_and(|health| {
        matches!(
            health.state,
            scheduler_core::health::NodeHealthState::AutoQuarantined
                | scheduler_core::health::NodeHealthState::ManualQuarantined
        )
    }) {
        format!(
            r#"<form method="post" action="/ui/nodes/{}/reset"><input type="hidden" name="csrf" value="{}"><button>Reset into probation (capacity 1)</button></form>"#,
            esc(&id),
            esc(&session.csrf)
        )
    } else {
        format!(
            r#"<form method="post" action="/ui/nodes/{}/quarantine"><input type="hidden" name="csrf" value="{}"><button class="danger">Manually quarantine</button></form>"#,
            esc(&id),
            esc(&session.csrf)
        )
    };
    let mut evidence_rows = String::new();
    for row in evidence {
        let diagnostic = row.diagnostic.as_ref();
        let status = diagnostic
            .and_then(|diagnostic| diagnostic.status.as_ref())
            .and_then(|status| serde_json::to_string(status).ok())
            .unwrap_or_else(|| "—".into());
        let diagnostic_summary = diagnostic.map_or("—", |diagnostic| diagnostic.summary.as_str());
        let diagnostic_location = diagnostic.map_or_else(
            || "—".into(),
            |diagnostic| {
                format!(
                    "{:?} / {:?} / {:?}",
                    diagnostic.code, diagnostic.origin, diagnostic.stage
                )
            },
        );
        let probe = match state
            .store
            .input_health(&row.blueprint_digest, &row.input_fingerprint)
            .await
        {
            Ok(Some(input))
                if input.state == scheduler_core::health::InputHealthState::Confirmed =>
            {
                format!(
                    r#"<form method="post" action="/ui/input-health/probe"><input type="hidden" name="csrf" value="{}"><input type="hidden" name="blueprint_digest" value="{}"><input type="hidden" name="input_fingerprint" value="{}"><button {}>Release one probe</button></form>"#,
                    esc(&session.csrf),
                    esc(&row.blueprint_digest),
                    esc(&row.input_fingerprint),
                    if input.probe_available {
                        "disabled"
                    } else {
                        ""
                    }
                )
            }
            Ok(_) => String::new(),
            Err(error) => return error_page(&session.csrf, &error),
        };
        evidence_rows.push_str(&format!(
            r#"<tr><td>{}</td><td><a href="/runs/{}"><code>{}</code></a><br><code>{}</code></td><td>{:?}<br>{:?}</td><td>{}<br>{}<br><code>{}</code></td><td>{}</td><td>{}</td></tr>"#,
            row.occurred_at,
            row.run_id,
            &row.run_id.to_string()[..8],
            &row.attempt_id.map_or_else(|| "—".into(), |id| id.to_string()),
            row.evidence_class,
            row.failure_family,
            esc(diagnostic_summary),
            esc(&diagnostic_location),
            esc(&status),
            if row.retracted { "retracted as input poison" } else if row.cluster_suppressed { "cluster-suppressed" } else { "active" },
            probe,
        ));
    }
    page(
        &format!("Node {id}"),
        &session.csrf,
        &format!(
            r#"<div class="actions"><h1 style="margin-right:auto">Node <code>{}</code></h1><a class="button" href="/settings/nodes/{}">Edit settings</a>{}</div><div class="grid"><div class="metric">Connection<b style="font-size:18px">{}</b></div><div class="metric">Health<b style="font-size:18px">{}</b></div><div class="metric">Capacity<b>{}/{}</b></div></div><div class="notice"><strong>{}</strong><br>{}</div><h2>Health evidence</h2><table><thead><tr><th>Time</th><th>Run / attempt</th><th>Classification</th><th>Diagnostic / status</th><th>Attribution</th><th>Action</th></tr></thead><tbody>{}</tbody></table>"#,
            esc(&agent.id),
            esc(&agent.id),
            action,
            if agent.connected { "online" } else { "offline" },
            esc(&state_name),
            agent.running,
            agent.capacity,
            esc(reason),
            esc(&evaluation),
            evidence_rows,
        ),
    )
}

async fn quarantine_node(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<CsrfForm>,
) -> Response {
    mutate_node_health(state, headers, id, form.csrf, true).await
}

async fn reset_node_health(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<CsrfForm>,
) -> Response {
    mutate_node_health(state, headers, id, form.csrf, false).await
}

async fn mutate_node_health(
    state: AppState,
    headers: HeaderMap,
    id: String,
    csrf: String,
    quarantine: bool,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let result = async {
        scheduler_core::validate_agent_id(&id)?;
        let view = state
            .store
            .set_node_manual_quarantine(&id, quarantine)
            .await?;
        state.apply_agent_health_view(&view).await;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    match result {
        Ok(()) => Redirect::to(&format!("/nodes/{id}")).into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

#[derive(Deserialize)]
struct ProbeInputForm {
    csrf: String,
    blueprint_digest: String,
    input_fingerprint: String,
}

async fn release_input_probe(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<ProbeInputForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    match state
        .store
        .grant_input_probe(&form.blueprint_digest, &form.input_fingerprint)
        .await
    {
        Ok(_) => Redirect::to("/nodes").into_response(),
        Err(error) => error_page(&session.csrf, &error),
    }
}

async fn edit_global_settings(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return Redirect::to("/login").into_response();
    };
    let settings = match state.store.get_global_settings().await {
        Ok(settings) => settings,
        Err(error) => return error_page(&session.csrf, &error),
    };
    let json = match serde_json::to_string_pretty(&settings) {
        Ok(json) => json,
        Err(error) => return error_page(&session.csrf, &error),
    };
    match state.store.acquire_lock("global", &session.id).await {
        Ok(lock) => page(
            "Global settings",
            &session.csrf,
            &settings_form(
                "/ui/settings/global",
                "Coordinator settings",
                &json,
                settings.revision,
                &lock.lock_token,
                &session.csrf,
            ),
        ),
        Err(acquire_error) => match state.store.current_lock("global").await {
            Ok(Some(lock)) => page(
                "Global settings (read only)",
                &session.csrf,
                &settings_read_only_form(
                    "Coordinator settings",
                    &json,
                    settings.revision,
                    "global",
                    &lock,
                    &session,
                ),
            ),
            Ok(None) => error_page(&session.csrf, &acquire_error),
            Err(error) => error_page(&session.csrf, &error),
        },
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
    let settings = match state.store.get_node_settings(&id).await {
        Ok(Some(settings)) => settings,
        Ok(None) => return error_page(&session.csrf, &"node settings not found"),
        Err(error) => return error_page(&session.csrf, &error),
    };
    let json = match serde_json::to_string_pretty(&settings) {
        Ok(json) => json,
        Err(error) => return error_page(&session.csrf, &error),
    };
    let document_key = format!("node:{id}");
    match state.store.acquire_lock(&document_key, &session.id).await {
        Ok(lock) => page(
            "Node settings",
            &session.csrf,
            &settings_form(
                &format!("/ui/settings/nodes/{id}"),
                &format!("Node settings / {}", esc(&id)),
                &json,
                settings.revision,
                &lock.lock_token,
                &session.csrf,
            ),
        ),
        Err(acquire_error) => match state.store.current_lock(&document_key).await {
            Ok(Some(lock)) => page(
                "Node settings (read only)",
                &session.csrf,
                &settings_read_only_form(
                    &format!("Node settings / {}", esc(&id)),
                    &json,
                    settings.revision,
                    &document_key,
                    &lock,
                    &session,
                ),
            ),
            Ok(None) => error_page(&session.csrf, &acquire_error),
            Err(error) => error_page(&session.csrf, &error),
        },
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
struct ForceUnlockForm {
    csrf: String,
    document_key: String,
}

async fn force_release_settings_lock(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<ForceUnlockForm>,
) -> Response {
    let Some(session) = state.auth.session(&headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if session.csrf != form.csrf {
        return StatusCode::FORBIDDEN.into_response();
    }
    let redirect = if form.document_key == "global" {
        "/settings/global".to_owned()
    } else if form.document_key == "dashboard" {
        "/dashboard/edit".to_owned()
    } else if let Some(node_id) = form.document_key.strip_prefix("node:") {
        if node_id.is_empty() || node_id.contains('/') {
            return StatusCode::BAD_REQUEST.into_response();
        }
        format!("/settings/nodes/{node_id}")
    } else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    match state.store.release_lock(&form.document_key, "", true).await {
        Ok(()) => Redirect::to(&redirect).into_response(),
        Err(error) => error_page(&session.csrf, &error),
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
    let collection = spec.and_then(|spec| spec.parameter_collection.as_ref());
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
        r##"<h1>{}</h1><form method="post" action="{}"><input type="hidden" name="csrf" value="{}">{}<label>Name<input name="name" value="{}" required></label><div class="row"><label>Blueprint URI<input name="blueprint_ref" value="{}" placeholder="file:///opt/tasks/example.yaml" required></label><label>Base parameters URI<input name="parameters_ref" value="{}" placeholder="file:///opt/tasks/example.json" required></label></div><h2>Optional parameter collection</h2><label>Collection source URI<input name="parameter_collection_uri" value="{}" placeholder="connector://reporting/daily-workbooks"></label><div class="row"><label>Page size<input type="number" min="1" max="1000" name="collection_page_size" value="{}"></label><label>Maximum items<input type="number" min="1" max="10000" name="collection_max_items" value="{}"></label><label>Maximum active child runs<input type="number" min="1" max="1000" name="collection_max_active_runs" value="{}"></label><label>Distinct healthy nodes to confirm poison<input type="number" min="2" max="32" name="collection_poison_distinct_nodes" value="{}"></label></div><p class="muted">Collection schedules create a durable batch for every cron, manual, future, or webhook trigger. Valid items continue when other items are quarantined.</p><div class="row"><label>Cron expression<input name="cron_expression" value="{}" placeholder="0 0 9 * * *"></label><label>IANA timezone<input name="timezone" value="{}" required></label></div><p><button type="button" hx-post="/ui/cron-preview" hx-include="closest form" hx-target="#cron-preview">Preview next five</button></p><div id="cron-preview"></div><label>Required labels (JSON object)<textarea name="labels_json">{}</textarea></label><label><input style="width:auto" type="checkbox" name="webhook_enabled" value="yes" {}> Enable HTTP webhook</label><div class="actions"><button type="submit">Save and validate</button><a href="/schedules">Cancel</a></div></form>{}"##,
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
        esc(collection
            .map(|collection| collection.source_ref.uri.as_str())
            .unwrap_or_default()),
        collection.map_or(500, |collection| collection.page_size),
        collection.map_or(10_000, |collection| collection.max_items),
        collection.map_or(32, |collection| collection.max_active_runs),
        collection.map_or(2, |collection| collection.poison_distinct_nodes),
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

fn settings_read_only_form(
    title: &str,
    json: &str,
    revision: i64,
    document_key: &str,
    lock: &EditLock,
    session: &UiSession,
) -> String {
    let owner = if lock.owner_session == session.id {
        "this administrator session (another page)"
    } else {
        "another administrator session"
    };
    format!(
        r#"<h1>{title}</h1><div class="notice"><strong>Read only.</strong> Locked by {owner} until {expires}. Revision r{revision} cannot be changed from this page.</div><label>Settings JSON<textarea spellcheck="false" readonly>{json}</textarea></label><div class="actions"><a class="button" href="">Retry lock</a><form method="post" action="/ui/settings/lock/force" onsubmit="return confirm('Force-unlock this settings document? Any unsaved changes in the other editor will be lost.')"><input type="hidden" name="csrf" value="{csrf}"><input type="hidden" name="document_key" value="{document_key}"><button class="danger" type="submit">Force unlock</button></form></div>"#,
        title = title,
        owner = owner,
        expires = lock.expires_at,
        revision = revision,
        json = esc(json),
        csrf = esc(&session.csrf),
        document_key = esc(document_key),
    )
}

fn render_login(error_message: &str) -> Response {
    match (LoginTemplate {
        error: error_message,
    })
    .render()
    {
        Ok(body) => Html(body).into_response(),
        Err(error) => {
            let request_id = crate::management::current_request_id();
            tracing::error!(%request_id, error = %error, "login template rendering failed");
            stable_ui_error(&request_id)
        }
    }
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
        Err(error) => {
            let request_id = crate::management::current_request_id();
            tracing::error!(%request_id, error = %error, "management template rendering failed");
            stable_ui_error(&request_id)
        }
    }
}

fn error_page(csrf: &str, error: &dyn std::fmt::Display) -> Response {
    let request_id = crate::management::current_request_id();
    tracing::error!(%request_id, error = %error, "management operation failed");
    let mut response = page(
        "Error",
        csrf,
        &format!(
            r#"<h1>Request failed</h1><div class="notice">The operation did not complete. Use request ID <code>{}</code> to locate the diagnostic log.</div><p><a href="javascript:history.back()">Go back</a></p>"#,
            esc(&request_id)
        ),
    );
    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    response
}

fn stable_ui_error(request_id: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Html(format!(
            "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><title>Request failed</title></head><body><main><h1>Request failed</h1><p>Use request ID <code>{}</code> to locate the diagnostic log.</p></main></body></html>",
            esc(request_id)
        )),
    )
        .into_response()
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
        let pending = node_row(&agent, None);
        assert!(pending.contains(r#"class="badge bad">pending"#));
        assert!(pending.contains("desired r4 / applied r3"));

        agent.settings_error = Some("invalid <script>alert('secret')</script> & value".into());
        let rejected = node_row(&agent, None);
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
        let row = node_row(&agent, None);
        assert!(row.contains(r#"class="badge good">applied"#));
        assert!(!row.contains("Rejected:"));
    }

    #[test]
    fn cursor_pagination_is_bounded_and_stable() {
        let items = (0..205).map(|value| format!("id-{value:03}")).collect();
        let first = paginate(
            items,
            &PageQuery {
                cursor: None,
                limit: Some(500),
            },
            Clone::clone,
        )
        .expect("first page");
        assert_eq!(first.items.len(), MAX_PAGE_SIZE);
        assert_eq!(first.next_cursor.as_deref(), Some("id-199"));

        let second = paginate(
            (0..205).map(|value| format!("id-{value:03}")).collect(),
            &PageQuery {
                cursor: first.next_cursor,
                limit: Some(200),
            },
            Clone::clone,
        )
        .expect("second page");
        assert_eq!(
            second.items,
            ["id-200", "id-201", "id-202", "id-203", "id-204"]
        );
        assert!(second.next_cursor.is_none());
    }

    #[test]
    fn cursor_pagination_rejects_unknown_cursors() {
        let error = paginate(
            vec!["a".to_owned()],
            &PageQuery {
                cursor: Some("missing".into()),
                limit: None,
            },
            Clone::clone,
        )
        .err()
        .expect("invalid cursor");
        assert!(error.to_string().contains("cursor"));
    }

    #[test]
    fn id_search_escapes_sql_wildcards() {
        assert_eq!(escape_like("node_50%"), "node\\_50\\%");
    }

    #[test]
    fn run_cursor_is_url_safe_and_validated() {
        let cursor = RunCursor {
            created_at: "2026-07-21T08:09:10.123Z".into(),
            id: Uuid::new_v4().to_string(),
        };
        let encoded = encode_run_cursor(&cursor).expect("encode");
        assert!(
            encoded
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        );
        let decoded = decode_run_cursor(&encoded).expect("decode");
        assert_eq!(decoded.created_at, cursor.created_at);
        assert_eq!(decoded.id, cursor.id);
        assert!(decode_run_cursor("not-a-valid-cursor").is_err());
    }

    #[test]
    fn schedule_form_builds_a_valid_collection_without_changing_base_parameters() {
        let spec = ScheduleForm {
            csrf: "csrf".into(),
            name: "Monthly reports".into(),
            blueprint_ref: "file:///blueprint.yaml".into(),
            parameters_ref: "file:///defaults.json".into(),
            parameter_collection_uri: "connector://reporting/monthly".into(),
            collection_page_size: Some(250),
            collection_max_items: Some(10_000),
            collection_max_active_runs: Some(16),
            collection_poison_distinct_nodes: Some(2),
            cron_expression: "0 0 6 1 * *".into(),
            timezone: "Europe/Vienna".into(),
            labels_json: "{}".into(),
            webhook_enabled: Some("yes".into()),
            expected_revision: None,
        }
        .into_spec()
        .expect("collection schedule");
        assert_eq!(spec.parameters_ref.uri, "file:///defaults.json");
        let collection = spec.parameter_collection.expect("collection");
        assert_eq!(collection.source_ref.uri, "connector://reporting/monthly");
        assert_eq!(collection.max_active_runs, 16);
    }

    #[test]
    fn schedule_card_uses_explicit_operator_facing_failure_classes() {
        let schedule = scheduler_core::ScheduleView {
            id: Uuid::new_v4(),
            spec: ScheduleSpec {
                name: "Reporting".into(),
                blueprint_ref: ArtifactRef {
                    uri: "file:///b".into(),
                },
                parameters_ref: ArtifactRef {
                    uri: "file:///p".into(),
                },
                parameter_collection: None,
                required_labels: BTreeMap::new(),
                cron: None,
                webhook_enabled: false,
                enabled: true,
            },
            revision: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            webhook_public_id: None,
        };
        let stats = ScheduleStatistics {
            queued: 0,
            running: 0,
            succeeded: 0,
            business_failed: 0,
            infrastructure_failed: 1,
            cancelled: 0,
            invalid_items: 0,
            poisoned_items: 0,
            retries: 2,
            node_diversity: 2,
            p50_ms: Some(10),
            p95_ms: Some(20),
            last_execution: None,
            last_success: None,
            last_failure: None,
        };
        let card = schedule_card(&schedule, &stats);
        assert!(card.contains("Infrastructure failure"));
        assert!(card.contains("retries 2"));
    }

    #[test]
    fn contended_settings_are_read_only_and_do_not_expose_the_lock_token() {
        let now = Utc::now();
        let lock = EditLock {
            document_key: "global".into(),
            owner_session: "different-session".into(),
            lock_token: "must-not-leak".into(),
            expires_at: now + chrono::Duration::minutes(2),
        };
        let session = UiSession {
            id: "viewer-session".into(),
            csrf: "csrf-token".into(),
            expires_at: now + chrono::Duration::hours(1),
        };
        let page = settings_read_only_form(
            "Coordinator settings",
            r#"{"revision":7}"#,
            7,
            "global",
            &lock,
            &session,
        );
        assert!(page.contains("Read only"));
        assert!(page.contains("another administrator session"));
        assert!(page.contains("Force unlock"));
        assert!(page.contains("readonly"));
        assert!(!page.contains("must-not-leak"));
        assert!(!page.contains("different-session"));
    }

    #[test]
    fn batch_item_pagination_preserves_and_encodes_provider_key_filter() {
        let page = Page {
            items: vec!["item"],
            next_cursor: Some("cursor_value".into()),
            limit: 50,
        };
        let controls = pagination_controls_with_filter(
            "/batches/id",
            &page,
            "provider_key",
            Some("customer 42/monthly&final"),
        );
        assert!(controls.contains("cursor=cursor_value"));
        assert!(controls.contains("provider_key=customer+42%2Fmonthly%26final"));
    }
}
