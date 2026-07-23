use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use chrono::Utc;
use scheduler_core::{
    DailyWindow, ExecutionAssignment, ExecutionOutcome, ExecutionResult, ExecutionSnapshot,
    FailureCode, FailureDiagnostic, FailureOrigin, FailureStage, OutputMetadata,
};
use scheduler_protocol::control::{
    Assignment, CancelAttempt, CoordinatorMessage, coordinator_message,
};
use scheduler_store::RunRecord;
use tracing::{debug, error, info, instrument, warn};

use crate::{api::materialize_cron, state::AppState};

pub fn spawn(state: AppState) {
    let mut durable_log_acks = scheduler_telemetry::subscribe_durable_log_acks();
    let ack_state = state.clone();
    tokio::spawn(async move {
        loop {
            match durable_log_acks.recv().await {
                Ok(event_id) => {
                    if let Err(error) = ack_state
                        .store
                        .acknowledge_observability_event(&event_id)
                        .await
                    {
                        error!(%event_id, error = %error, "cannot acknowledge exported state event");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        skipped,
                        "state-event export acknowledgements lagged; events will replay"
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let event_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            match event_state.store.claim_observability_events(256).await {
                Ok(events) => {
                    for event in events {
                        let span = tracing::info_span!(
                            "coordinator.observability.state_event",
                            event.id = %event.event_id
                        );
                        let _entered = span.enter();
                        scheduler_telemetry::set_current_span_parent(
                            event.traceparent.as_deref(),
                            event.tracestate.as_deref(),
                        );
                        tracing::info!(
                            target: "scheduler.state",
                            event_id = %event.event_id,
                            event_sequence = event.sequence,
                            event_name = %event.event_name,
                            event_occurred_at = %event.occurred_at,
                            entity.type = %event.entity_type,
                            entity.id = %event.entity_id,
                            event_attributes_json = %event.attributes_json,
                            "authoritative scheduler state transition"
                        );
                    }
                }
                Err(error) => {
                    error!(error = %error, "cannot publish durable state events");
                }
            }
        }
    });

    let retention_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3_600));
        loop {
            interval.tick().await;
            let result = async {
                let retention = retention_state
                    .store
                    .get_global_settings()
                    .await?
                    .audit_retention_days;
                retention_state
                    .store
                    .expire_observability_events(retention)
                    .await
            }
            .await;
            match result {
                Ok(expired) if expired > 0 => {
                    error!(
                        expired,
                        "undelivered state events expired; coverage is now incomplete"
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    error!(error = %error, "state-event retention pass failed");
                }
            }
        }
    });

    let cron_state = state.clone();
    let collection_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            if let Err(error) =
                crate::collection_runtime::collection_worker_pass(&collection_state).await
            {
                error!(error = %error, "collection ingestion pass failed");
            }
        }
    });

    let health_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            if let Err(error) = crate::health::reconcile_unclassified(&health_state).await {
                error!(error = %error, "health evidence reconciliation failed");
            }
        }
    });

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if let Err(error) = materialize_cron(&cron_state).await {
                error!(error = %error, "cron materialization failed");
            }
        }
    });

    let dispatch_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            if let Err(error) = dispatch(&dispatch_state).await {
                error!(error = %error, "dispatch pass failed");
            }
        }
    });

    let lease_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            if let Err(error) = expire_leases(&lease_state).await {
                error!(error = %error, "lease expiry pass failed");
            }
        }
    });

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Err(error) = project_observability(&state).await {
                error!(error = %error, "authoritative observability projection failed");
            }
        }
    });
}

#[instrument(name = "coordinator.observability.project", skip_all)]
async fn project_observability(state: &AppState) -> Result<()> {
    use opentelemetry::KeyValue;

    let now = Utc::now();
    let meter = opentelemetry::global::meter("scheduler-coordinator");
    let entity_gauge = meter.u64_gauge("scheduler.state.entities").build();
    let actual = state.store.processing_state_counts().await?;
    let actual = actual
        .into_iter()
        .map(|count| ((count.schedule_id, count.entity, count.state), count.count))
        .collect::<HashMap<_, _>>();
    let schedules = state.store.list_schedules().await?;
    const ENTITY_STATES: &[(&str, &[&str])] = &[
        (
            "run",
            &["queued", "running", "succeeded", "failed", "cancelled"],
        ),
        (
            "batch",
            &[
                "scheduled",
                "collecting",
                "running",
                "succeeded",
                "completed_with_errors",
                "failed",
                "cancelled",
            ],
        ),
        (
            "item",
            &[
                "ready",
                "queued",
                "running",
                "succeeded",
                "failed",
                "cancelled",
                "invalid",
                "suspected_poison",
                "poisoned",
                "held",
            ],
        ),
        (
            "attempt",
            &[
                "offered",
                "accepted",
                "expiring",
                "finished",
                "offer_failed",
                "late_result",
            ],
        ),
    ];
    for schedule in &schedules {
        let schedule_id = schedule.id.to_string();
        for (entity, states) in ENTITY_STATES {
            for entity_state in *states {
                let value = actual
                    .get(&(
                        schedule_id.clone(),
                        (*entity).into(),
                        (*entity_state).into(),
                    ))
                    .copied()
                    .unwrap_or(0);
                entity_gauge.record(
                    value,
                    &[
                        KeyValue::new("schedule.id", schedule_id.clone()),
                        KeyValue::new("entity", *entity),
                        KeyValue::new("state", *entity_state),
                    ],
                );
            }
        }
    }

    let trigger_gauge = meter.u64_gauge("scheduler.schedule.daily.triggers").build();
    let item_gauge = meter.u64_gauge("scheduler.schedule.daily.items").build();
    let anomaly_gauge = meter
        .u64_gauge("scheduler.schedule.daily.attempt_anomalies")
        .build();
    let retry_gauge = meter.u64_gauge("scheduler.schedule.daily.retries").build();
    let verdict_gauge = meter.u64_gauge("scheduler.schedule.daily.verdict").build();
    let operations_day_gauge = meter
        .u64_gauge("scheduler.schedule.daily.operations_day")
        .build();
    let deadline_gauge = meter
        .u64_gauge("scheduler.schedule.completion_deadline_seconds")
        .build();
    let cron_backlog_gauge = meter.u64_gauge("scheduler.cron.backlog").build();
    let overdue_gauge = meter.u64_gauge("scheduler.state.overdue").build();
    let last_success_age = meter
        .u64_gauge("scheduler.schedule.last_success_age_seconds")
        .build();
    for window in [DailyWindow::Current, DailyWindow::Previous] {
        let snapshot = state
            .store
            .daily_observability_snapshot(now, window)
            .await?;
        for status in snapshot.schedules {
            let schedule_id = status.schedule_id.to_string();
            let common = [
                KeyValue::new("schedule.id", schedule_id.clone()),
                KeyValue::new("window", window.as_str()),
            ];
            for (trigger_status, value) in [
                ("expected", status.expected_triggers),
                ("materialized", status.materialized_triggers),
                ("succeeded", status.succeeded),
                ("failed", status.failed),
                ("cancelled", status.cancelled),
                ("pending", status.pending),
                ("overdue", status.overdue),
                ("missing", status.missing_due),
            ] {
                trigger_gauge.record(
                    value,
                    &[
                        common[0].clone(),
                        common[1].clone(),
                        KeyValue::new("status", trigger_status),
                    ],
                );
            }
            for (item_state, value) in [
                ("ready", status.ready_items),
                ("queued", status.queued_items),
                ("running", status.running_items),
                ("succeeded", status.succeeded_items),
                ("failed", status.failed_items),
                ("cancelled", status.cancelled_items),
                ("invalid", status.invalid_items),
                ("suspected_poison", status.suspected_poison_items),
                ("poisoned", status.poisoned_items),
                ("held", status.held_items),
            ] {
                item_gauge.record(
                    value,
                    &[
                        common[0].clone(),
                        common[1].clone(),
                        KeyValue::new("state", item_state),
                    ],
                );
            }
            anomaly_gauge.record(status.attempt_anomalies, &common);
            retry_gauge.record(status.retries, &common);
            deadline_gauge.record(status.completion_deadline_seconds, &common);
            let operations_day = status
                .operations_day
                .replace('-', "")
                .parse::<u64>()
                .unwrap_or_default();
            operations_day_gauge.record(
                operations_day,
                &[
                    common[0].clone(),
                    common[1].clone(),
                    KeyValue::new("operations.timezone", status.operations_timezone),
                ],
            );
            cron_backlog_gauge.record(status.missing_due, &common);
            overdue_gauge.record(
                status.overdue,
                &[
                    KeyValue::new("schedule.id", schedule_id.clone()),
                    KeyValue::new("entity", "trigger"),
                    KeyValue::new("window", window.as_str()),
                ],
            );
            for verdict in ["idle", "green", "pending", "degraded", "red", "unknown"] {
                verdict_gauge.record(
                    u64::from(status.verdict.as_str() == verdict),
                    &[
                        common[0].clone(),
                        common[1].clone(),
                        KeyValue::new("verdict", verdict),
                    ],
                );
            }
            if window == DailyWindow::Current
                && let Some(last_success) = status.last_success_at
            {
                last_success_age.record(
                    u64::try_from((now - last_success).num_seconds().max(0)).unwrap_or(u64::MAX),
                    &[KeyValue::new("schedule.id", schedule_id)],
                );
            }
        }
    }

    let outbox = state.store.observability_outbox_status().await?;
    meter
        .u64_gauge("scheduler.observability.snapshot.generated_at_unix_seconds")
        .build()
        .record(u64::try_from(now.timestamp()).unwrap_or_default(), &[]);
    meter
        .u64_gauge("scheduler.observability.outbox.depth")
        .build()
        .record(outbox.pending_events, &[]);
    meter
        .u64_gauge("scheduler.observability.outbox.oldest_age_seconds")
        .build()
        .record(
            outbox
                .oldest_pending_at
                .map(|oldest| {
                    u64::try_from((now - oldest).num_seconds().max(0)).unwrap_or(u64::MAX)
                })
                .unwrap_or(0),
            &[],
        );
    meter
        .u64_gauge("scheduler.observability.coverage_gap")
        .build()
        .record(u64::from(outbox.coverage_gap), &[]);
    meter
        .u64_gauge("scheduler.observability.outbox.expired_events")
        .build()
        .record(outbox.expired_events, &[]);
    let telemetry = scheduler_telemetry::status();
    meter
        .u64_gauge("scheduler.observability.telemetry.dropped_items")
        .build()
        .record(telemetry.dropped_telemetry, &[]);
    let exporter_failures = meter
        .u64_gauge("scheduler.observability.telemetry.export_failures")
        .build();
    for signal in telemetry.signals {
        exporter_failures.record(
            signal.failed_items,
            &[KeyValue::new("signal", signal.signal)],
        );
    }

    let agent_gauge = meter.u64_gauge("scheduler.cluster.agents").build();
    let agent_capacity = meter.u64_gauge("scheduler.cluster.agent.capacity").build();
    for agent in state.store.list_agents().await? {
        let health = state
            .store
            .node_health(&agent.id)
            .await?
            .map(|view| match view.state {
                scheduler_core::health::NodeHealthState::Healthy => "healthy",
                scheduler_core::health::NodeHealthState::Suspect => "suspect",
                scheduler_core::health::NodeHealthState::AutoQuarantined => "auto_quarantined",
                scheduler_core::health::NodeHealthState::ManualQuarantined => "manual_quarantined",
                scheduler_core::health::NodeHealthState::Probation => "probation",
            })
            .unwrap_or("unknown");
        let settings_state = if agent.settings_error.is_some() {
            "rejected"
        } else if agent.applied_settings_revision < agent.desired_settings_revision {
            "pending"
        } else {
            "applied"
        };
        agent_gauge.record(
            1,
            &[
                KeyValue::new("agent.id", agent.id.clone()),
                KeyValue::new(
                    "connection_state",
                    if agent.connected {
                        "connected"
                    } else {
                        "disconnected"
                    },
                ),
                KeyValue::new("health_state", health),
                KeyValue::new("settings_state", settings_state),
            ],
        );
        agent_capacity.record(
            u64::from(agent.capacity),
            &[
                KeyValue::new("agent.id", agent.id.clone()),
                KeyValue::new("kind", "maximum"),
            ],
        );
        agent_capacity.record(
            u64::from(agent.running),
            &[
                KeyValue::new("agent.id", agent.id),
                KeyValue::new("kind", "running"),
            ],
        );
    }
    Ok(())
}

#[instrument(name = "coordinator.dispatch.pass", skip_all)]
async fn dispatch(state: &AppState) -> Result<()> {
    use opentelemetry::KeyValue;

    let meter = opentelemetry::global::meter("scheduler-coordinator");
    meter
        .u64_counter("scheduler.dispatch.passes")
        .build()
        .add(1, &[]);
    let lease_seconds = state.store.get_global_settings().await?.lease_seconds;
    let queue_depth = state.store.queue_depth().await?;
    let queue_depth_gauge = meter.u64_gauge("scheduler.queue.depth").build();
    queue_depth_gauge.record(queue_depth.ready, &[KeyValue::new("readiness", "ready")]);
    queue_depth_gauge.record(
        queue_depth.delayed,
        &[KeyValue::new("readiness", "delayed")],
    );
    let queued = state.store.queued_runs(100).await?;
    meter
        .u64_histogram("scheduler.dispatch.candidates")
        .build()
        .record(queued.len() as u64, &[]);
    for run in queued {
        dispatch_run(state, lease_seconds, run).await?;
    }
    Ok(())
}

#[instrument(
    name = "coordinator.dispatch.candidate",
    parent = None,
    skip_all,
    fields(
        run_id = %run.view.id,
        schedule_id = %run.view.schedule_id,
        attempt_id = tracing::field::Empty
    )
)]
async fn dispatch_run(state: &AppState, lease_seconds: u64, run: RunRecord) -> Result<()> {
    use opentelemetry::KeyValue;

    let trigger_trace_context = state.store.run_trace_context(run.view.id).await?;
    scheduler_telemetry::link_current_span(
        trigger_trace_context.traceparent.as_deref(),
        trigger_trace_context.tracestate.as_deref(),
    );
    let meter = opentelemetry::global::meter("scheduler-coordinator");
    let plaintext = state.cipher.decrypt(&run.encrypted_snapshot)?;
    let snapshot: ExecutionSnapshot = serde_json::from_slice(&plaintext)?;
    let avoided_agents = state.store.retry_avoided_agents(run.view.id).await?;
    let requires_alternative = state
        .store
        .retry_requires_alternative_agent(run.view.id)
        .await?;
    let reservation = state
        .reserve_agent_avoiding(&snapshot, &avoided_agents)
        .await;
    let reservation =
        if reservation.is_none() && !avoided_agents.is_empty() && !requires_alternative {
            // If no independent healthy node exists the input remains
            // suspected; retrying on the same node is preferable to silently
            // discarding the run, but can never confirm poison.
            state.reserve_agent(&snapshot).await
        } else {
            reservation
        };
    let Some((agent_id, tx)) = reservation else {
        meter
            .u64_counter("scheduler.dispatch.decisions")
            .build()
            .add(1, &[KeyValue::new("decision", "no_eligible_agent")]);
        debug!("no eligible agent for queued run");
        return Ok(());
    };
    let Some(attempt) = state
        .store
        .create_attempt(run.view.id, &agent_id, lease_seconds)
        .await?
    else {
        state.release_agent_slot(&agent_id).await;
        meter
            .u64_counter("scheduler.dispatch.decisions")
            .build()
            .add(1, &[KeyValue::new("decision", "reservation_lost")]);
        debug!(agent_id, "run was no longer available after placement");
        return Ok(());
    };
    tracing::Span::current().record("attempt_id", tracing::field::display(attempt.id));
    let trace_context = scheduler_telemetry::current_trace_context();
    let assignment = ExecutionAssignment {
        schedule_id: run.view.schedule_id,
        run_id: run.view.id,
        attempt_id: attempt.id,
        attempt_number: attempt.attempt_number,
        lease_token: attempt.lease_token,
        lease_seconds,
        snapshot,
        traceparent: trace_context.traceparent.clone(),
        tracestate: trace_context.tracestate.clone(),
    };
    let message = CoordinatorMessage {
        payload: Some(coordinator_message::Payload::Assignment(Assignment {
            assignment_json: serde_json::to_string(&assignment)?,
            traceparent: trace_context.traceparent,
            tracestate: trace_context.tracestate,
        })),
    };
    if tx.send(Ok(message)).await.is_err() {
        warn!(agent_id, attempt_id = %attempt.id, "assignment stream closed before offer");
        state.store.release_offer(attempt.id).await?;
        state.release_agent_slot(&agent_id).await;
        meter
            .u64_counter("scheduler.dispatch.decisions")
            .build()
            .add(1, &[KeyValue::new("decision", "stream_closed")]);
    } else {
        meter
            .u64_counter("scheduler.dispatch.offers")
            .build()
            .add(1, &[]);
        meter
            .u64_counter("scheduler.dispatch.decisions")
            .build()
            .add(1, &[KeyValue::new("decision", "offered")]);
        let latency_ms = (Utc::now() - run.view.created_at).num_milliseconds().max(0) as f64;
        meter
            .f64_histogram("scheduler.dispatch.latency_ms")
            .build()
            .record(latency_ms, &[]);
        info!(agent_id, run_id = %run.view.id, attempt_id = %attempt.id, "assignment offered");
    }
    Ok(())
}

#[instrument(name = "coordinator.lease.expiry_pass", skip_all)]
async fn expire_leases(state: &AppState) -> Result<()> {
    let heartbeat_seconds = state.store.get_global_settings().await?.heartbeat_seconds;
    for attempt in state
        .store
        .claim_expired_attempts(500, heartbeat_seconds)
        .await?
    {
        let health_context = state.store.attempt_health_context(attempt.id).await?;
        let now = Utc::now();
        let result = ExecutionResult {
            outcome: ExecutionOutcome::LeaseExpired,
            exit_code: None,
            signal: None,
            stdout: String::new(),
            stderr: String::new(),
            started_at: now,
            finished_at: now,
            error: Some("agent stopped renewing the execution lease".into()),
            output: OutputMetadata::default(),
            diagnostic: Some(FailureDiagnostic::new(
                FailureCode::AgentLeaseExpired,
                FailureOrigin::Agent,
                FailureStage::Lease,
                "agent disconnected or stopped renewing the execution lease",
                true,
            )),
        };
        let encrypted = state.cipher.encrypt(&serde_json::to_vec(&result)?)?;
        let expired = state
            .store
            .finish_expired_attempt(
                attempt.id,
                &attempt.lease_token,
                &result,
                encrypted,
                state.cipher.key_id(),
            )
            .await?;
        if expired.is_none() {
            // The attempt was renewed or completed after the expiry scan.
            continue;
        }
        if let Some(context) = health_context
            && let Err(error) = crate::health::record_attempt_result(state, &context, &result).await
        {
            // The durable result remains authoritative and the reconciliation
            // worker will retry this idempotently.
            warn!(attempt_id = %attempt.id, error = %error, "deferred lease health classification");
        }
        state
            .send_to_agent(
                &attempt.agent_id,
                CoordinatorMessage {
                    payload: Some(coordinator_message::Payload::Cancel(CancelAttempt {
                        attempt_id: attempt.id.to_string(),
                    })),
                },
            )
            .await;
        state.release_agent_slot(&attempt.agent_id).await;
        opentelemetry::global::meter("scheduler-coordinator")
            .u64_counter("scheduler.lease.expirations")
            .build()
            .add(1, &[]);
        warn!(attempt_id = %attempt.id, agent_id = attempt.agent_id, "execution lease expired");
    }
    Ok(())
}
