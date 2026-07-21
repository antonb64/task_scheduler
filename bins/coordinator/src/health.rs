use anyhow::{Context, Result};
use scheduler_core::{
    ExecutionResult, ExecutionSnapshot,
    health::{HealthEvidenceClass, NodeHealthState, classify_result},
};
use scheduler_store::{AttemptHealthContext, NewHealthEvidence};
use sha2::{Digest, Sha256};
use tracing::{info, instrument, warn};

use crate::state::AppState;

#[instrument(
    name = "coordinator.health.classify_attempt",
    skip_all,
    fields(
        run_id = %context.run_id,
        attempt_id = %context.attempt_id,
        node_id = context.agent_id.as_str()
    )
)]
pub async fn record_attempt_result(
    state: &AppState,
    context: &AttemptHealthContext,
    result: &ExecutionResult,
) -> Result<()> {
    let snapshot_plaintext = state.cipher.decrypt(&context.encrypted_snapshot)?;
    let snapshot: ExecutionSnapshot = serde_json::from_slice(&snapshot_plaintext)
        .context("persisted execution snapshot is invalid")?;
    // The scope digest is immutable for this run and intentionally covers the
    // executor revision as well as its rendered non-secret inputs. The keyed
    // fingerprint prevents offline guessing from the health database.
    let blueprint_scope = if snapshot.blueprint_digest.is_empty() {
        // Backward compatibility for immutable runs created before execution
        // snapshots carried the independently calculated blueprint digest.
        hex::encode(Sha256::digest(&snapshot_plaintext))
    } else {
        snapshot.blueprint_digest.clone()
    };
    let input_fingerprint = state.cipher.input_fingerprint(
        &blueprint_scope,
        &serde_json::json!({"parameters_digest": snapshot.parameters_digest}),
    )?;
    let classification = classify_result(result.outcome, result.diagnostic.as_ref());
    let node_was_healthy = state
        .store
        .node_health(&context.agent_id)
        .await?
        .is_none_or(|view| view.state == NodeHealthState::Healthy);
    let threshold = state
        .store
        .poison_distinct_nodes_for_run(context.run_id)
        .await?;
    let (input, nodes) = state
        .store
        .record_health_evidence(
            &NewHealthEvidence {
                attempt_id: Some(context.attempt_id),
                run_id: context.run_id,
                schedule_id: context.schedule_id,
                agent_id: context.agent_id.clone(),
                blueprint_digest: blueprint_scope,
                input_fingerprint,
                classification,
                diagnostic: result.diagnostic.clone(),
                node_was_healthy,
                // Lease/transport observations are classified as
                // cluster-sensitive and excluded from node scoring. A later
                // fleet incident correlator can additionally mark the row.
                cluster_suppressed: classification.class == HealthEvidenceClass::ClusterSensitive,
                occurred_at: result.finished_at,
            },
            threshold,
        )
        .await?;
    for node in nodes {
        state.apply_agent_health_view(&node).await;
        if matches!(
            node.state,
            NodeHealthState::AutoQuarantined | NodeHealthState::ManualQuarantined
        ) {
            warn!(
                agent_id = node.agent_id,
                node_health = ?node.state,
                reason_code = node.reason_code,
                failure_rate = node.evaluation.failure_rate,
                distinct_inputs = node.evaluation.distinct_failed_inputs,
                "node is quarantined from new placement"
            );
        }
    }
    info!(
        run_id = %context.run_id,
        attempt_id = %context.attempt_id,
        agent_id = context.agent_id,
        evidence_class = ?classification.class,
        failure_family = ?classification.family,
        input_health = ?input.state,
        distinct_healthy_nodes = input.distinct_healthy_nodes,
        "attempt health evidence applied"
    );
    Ok(())
}

/// Replays the gap between durable result storage and health classification.
/// This makes a coordinator crash at that boundary eventually lossless while
/// the unique attempt index keeps normal result handling idempotent.
pub async fn reconcile_unclassified(state: &AppState) -> Result<u32> {
    let rows = state.store.unclassified_attempts(500).await?;
    let count = rows.len() as u32;
    for row in rows {
        let plaintext = state.cipher.decrypt(&row.encrypted_result)?;
        let result: ExecutionResult =
            serde_json::from_slice(&plaintext).context("persisted execution result is invalid")?;
        record_attempt_result(state, &row.context, &result).await?;
    }
    Ok(count)
}
