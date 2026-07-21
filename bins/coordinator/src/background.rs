use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use scheduler_core::{
    ExecutionAssignment, ExecutionOutcome, ExecutionResult, ExecutionSnapshot, FailureCode,
    FailureDiagnostic, FailureOrigin, FailureStage, OutputMetadata,
};
use scheduler_protocol::control::{
    Assignment, CancelAttempt, CoordinatorMessage, coordinator_message,
};
use scheduler_store::RunRecord;
use tracing::{debug, error, info, instrument, warn};

use crate::{api::materialize_cron, state::AppState};

pub fn spawn(state: AppState) {
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

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            if let Err(error) = expire_leases(&state).await {
                error!(error = %error, "lease expiry pass failed");
            }
        }
    });
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
    skip_all,
    fields(
        run_id = %run.view.id,
        schedule_id = %run.view.schedule_id,
        attempt_id = tracing::field::Empty
    )
)]
async fn dispatch_run(state: &AppState, lease_seconds: u64, run: RunRecord) -> Result<()> {
    use opentelemetry::KeyValue;

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
    let assignment = ExecutionAssignment {
        schedule_id: run.view.schedule_id,
        run_id: run.view.id,
        attempt_id: attempt.id,
        attempt_number: attempt.attempt_number,
        lease_token: attempt.lease_token,
        lease_seconds,
        snapshot,
    };
    let message = CoordinatorMessage {
        payload: Some(coordinator_message::Payload::Assignment(Assignment {
            assignment_json: serde_json::to_string(&assignment)?,
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
