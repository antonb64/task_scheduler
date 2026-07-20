use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use scheduler_core::{ExecutionAssignment, ExecutionOutcome, ExecutionResult, ExecutionSnapshot};
use scheduler_protocol::control::{
    Assignment, CancelAttempt, CoordinatorMessage, coordinator_message,
};
use tracing::{error, info, warn};

use crate::{api::materialize_cron, state::AppState};

pub fn spawn(state: AppState) {
    let cron_state = state.clone();
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

async fn dispatch(state: &AppState) -> Result<()> {
    let lease_seconds = state.store.get_global_settings().await?.lease_seconds;
    for run in state.store.queued_runs(100).await? {
        let plaintext = state.cipher.decrypt(&run.encrypted_snapshot)?;
        let snapshot: ExecutionSnapshot = serde_json::from_slice(&plaintext)?;
        let Some((agent_id, tx)) = state.reserve_agent(&snapshot.required_labels).await else {
            continue;
        };
        let Some(attempt) = state
            .store
            .create_attempt(run.view.id, &agent_id, lease_seconds)
            .await?
        else {
            state.release_agent_slot(&agent_id).await;
            continue;
        };
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
        } else {
            opentelemetry::global::meter("scheduler-coordinator")
                .u64_counter("scheduler.dispatch.offers")
                .build()
                .add(1, &[]);
            info!(agent_id, run_id = %run.view.id, attempt_id = %attempt.id, "assignment offered");
        }
    }
    Ok(())
}

async fn expire_leases(state: &AppState) -> Result<()> {
    let heartbeat_seconds = state.store.get_global_settings().await?.heartbeat_seconds;
    for attempt in state
        .store
        .claim_expired_attempts(500, heartbeat_seconds)
        .await?
    {
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
        };
        let encrypted = state.cipher.encrypt(&serde_json::to_vec(&result)?)?;
        let expired = state
            .store
            .finish_expired_attempt(
                attempt.id,
                &attempt.lease_token,
                encrypted,
                state.cipher.key_id(),
            )
            .await?;
        if expired.is_none() {
            // The attempt was renewed or completed after the expiry scan.
            continue;
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
