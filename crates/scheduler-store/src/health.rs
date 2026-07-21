use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use scheduler_core::{
    FailureDiagnostic,
    health::{
        FailureFamily, HealthClassification, HealthEvidenceClass, InputHealthObservation,
        InputHealthState, NodeHealthEvaluation, NodeHealthObservation, NodeHealthState,
        apply_node_health_evaluation, evaluate_input_health, evaluate_node_health,
    },
};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::Row;
use uuid::Uuid;

use crate::{Store, append_audit_tx, format_time, parse_time};

#[derive(Debug, Clone)]
pub struct NewHealthEvidence {
    pub attempt_id: Option<Uuid>,
    pub run_id: Uuid,
    pub schedule_id: Uuid,
    pub agent_id: String,
    pub blueprint_digest: String,
    pub input_fingerprint: String,
    pub classification: HealthClassification,
    pub diagnostic: Option<FailureDiagnostic>,
    pub node_was_healthy: bool,
    pub cluster_suppressed: bool,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct AttemptHealthContext {
    pub attempt_id: Uuid,
    pub run_id: Uuid,
    pub schedule_id: Uuid,
    pub agent_id: String,
    pub encrypted_snapshot: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct UnclassifiedAttempt {
    pub context: AttemptHealthContext,
    pub encrypted_result: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthEvidenceView {
    pub id: Uuid,
    pub attempt_id: Option<Uuid>,
    pub run_id: Uuid,
    pub schedule_id: Uuid,
    pub agent_id: String,
    pub blueprint_digest: String,
    pub input_fingerprint: String,
    pub classifier_version: u32,
    pub evidence_class: HealthEvidenceClass,
    pub failure_family: FailureFamily,
    pub diagnostic: Option<FailureDiagnostic>,
    pub node_was_healthy: bool,
    pub cluster_suppressed: bool,
    pub retracted: bool,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InputHealthView {
    pub blueprint_digest: String,
    pub input_fingerprint: String,
    pub state: InputHealthState,
    pub failure_family: Option<FailureFamily>,
    pub distinct_healthy_nodes: u32,
    pub probe_available: bool,
    pub revision: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeHealthView {
    pub agent_id: String,
    pub state: NodeHealthState,
    pub reason_code: Option<String>,
    pub evaluation: NodeHealthEvaluation,
    pub revision: i64,
    pub transitioned_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Store {
    pub async fn attempt_health_context(
        &self,
        attempt_id: Uuid,
    ) -> Result<Option<AttemptHealthContext>> {
        let row = sqlx::query(
            "SELECT a.id AS attempt_id,a.run_id,a.agent_id,r.schedule_id,r.encrypted_snapshot \
             FROM attempts a JOIN runs r ON r.id=a.run_id WHERE a.id=?",
        )
        .bind(attempt_id.to_string())
        .fetch_optional(self.pool())
        .await?;
        row.map(|row| {
            Ok(AttemptHealthContext {
                attempt_id: Uuid::parse_str(&row.get::<String, _>("attempt_id"))?,
                run_id: Uuid::parse_str(&row.get::<String, _>("run_id"))?,
                schedule_id: Uuid::parse_str(&row.get::<String, _>("schedule_id"))?,
                agent_id: row.get("agent_id"),
                encrypted_snapshot: row.get("encrypted_snapshot"),
            })
        })
        .transpose()
    }

    pub async fn unclassified_attempts(&self, limit: u32) -> Result<Vec<UnclassifiedAttempt>> {
        let rows = sqlx::query(
            "SELECT a.id AS attempt_id,a.run_id,a.agent_id,a.encrypted_result,\
             r.schedule_id,r.encrypted_snapshot FROM attempts a \
             JOIN runs r ON r.id=a.run_id \
             LEFT JOIN health_evidence h ON h.attempt_id=a.id \
             WHERE a.state='finished' AND a.encrypted_result IS NOT NULL AND h.id IS NULL \
             ORDER BY a.finished_at,a.id LIMIT ?",
        )
        .bind(i64::from(limit.clamp(1, 1_000)))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(UnclassifiedAttempt {
                    context: AttemptHealthContext {
                        attempt_id: Uuid::parse_str(&row.get::<String, _>("attempt_id"))?,
                        run_id: Uuid::parse_str(&row.get::<String, _>("run_id"))?,
                        schedule_id: Uuid::parse_str(&row.get::<String, _>("schedule_id"))?,
                        agent_id: row.get("agent_id"),
                        encrypted_snapshot: row.get("encrypted_snapshot"),
                    },
                    encrypted_result: row.get("encrypted_result"),
                })
            })
            .collect()
    }

    /// Nodes which produced an ambiguous or strong node-local failure for this
    /// run are avoided on the next attempt. This both supports independent
    /// poison confirmation and prevents malformed local bindings/policy from
    /// creating a tight redispatch loop. Business failures do not contribute.
    pub async fn retry_avoided_agents(&self, run_id: Uuid) -> Result<HashSet<String>> {
        let rows = sqlx::query(
            "SELECT DISTINCT agent_id FROM health_evidence \
             WHERE run_id=? AND evidence_class IN ('ambiguous','strong_node_local') \
             AND retracted=0",
        )
        .bind(run_id.to_string())
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<String, _>("agent_id"))
            .collect())
    }

    /// Pre-acceptance rejections identify a node-local inability to execute
    /// this assignment. Falling back to the same node would not consume the
    /// accepted-attempt budget and could create an unbounded offer/rejection
    /// loop, so these runs must wait for another eligible node (or a retrigger
    /// after the local policy/credential problem is corrected).
    pub async fn retry_requires_alternative_agent(&self, run_id: Uuid) -> Result<bool> {
        let required = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM health_evidence \
             WHERE run_id=? AND failure_code IN ('parameter_binding_failed','assignment_rejected') \
             AND retracted=0)",
        )
        .bind(run_id.to_string())
        .fetch_one(self.pool())
        .await?;
        Ok(required != 0)
    }

    pub async fn poison_distinct_nodes_for_run(&self, run_id: Uuid) -> Result<u32> {
        let value = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT b.poison_distinct_nodes FROM runs r \
             LEFT JOIN batches b ON b.id=r.batch_id WHERE r.id=?",
        )
        .bind(run_id.to_string())
        .fetch_optional(self.pool())
        .await?
        .flatten()
        .unwrap_or(2);
        Ok(u32::try_from(value).unwrap_or(2).clamp(2, 32))
    }

    /// Records one immutable observation and updates input and node health in
    /// the same transaction. Attempt IDs make result replay idempotent.
    pub async fn record_health_evidence(
        &self,
        evidence: &NewHealthEvidence,
        poison_distinct_nodes: u32,
    ) -> Result<(InputHealthView, Vec<NodeHealthView>)> {
        let now = Utc::now();
        let id = Uuid::new_v4();
        let diagnostic = evidence
            .diagnostic
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let status = evidence
            .diagnostic
            .as_ref()
            .and_then(|diagnostic| diagnostic.status.as_ref())
            .map(serde_json::to_string)
            .transpose()?;
        let mut tx = self.pool().begin().await?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO health_evidence(\
             id,attempt_id,run_id,schedule_id,agent_id,blueprint_digest,input_fingerprint,\
             classifier_version,evidence_class,failure_family,failure_code,failure_origin,\
             failure_stage,diagnostic_json,safe_status_json,node_was_healthy,cluster_suppressed,retracted,\
             occurred_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)",
        )
        .bind(id.to_string())
        .bind(evidence.attempt_id.map(|value| value.to_string()))
        .bind(evidence.run_id.to_string())
        .bind(evidence.schedule_id.to_string())
        .bind(&evidence.agent_id)
        .bind(&evidence.blueprint_digest)
        .bind(&evidence.input_fingerprint)
        .bind(i64::from(evidence.classification.version))
        .bind(enum_string(evidence.classification.class)?)
        .bind(enum_string(evidence.classification.family)?)
        .bind(
            evidence
                .diagnostic
                .as_ref()
                .map(|diagnostic| enum_string(diagnostic.code))
                .transpose()?,
        )
        .bind(
            evidence
                .diagnostic
                .as_ref()
                .map(|diagnostic| enum_string(diagnostic.origin))
                .transpose()?,
        )
        .bind(
            evidence
                .diagnostic
                .as_ref()
                .map(|diagnostic| enum_string(diagnostic.stage))
                .transpose()?,
        )
        .bind(&diagnostic)
        .bind(status)
        .bind(evidence.node_was_healthy)
        .bind(evidence.cluster_suppressed)
        .bind(format_time(evidence.occurred_at))
        .bind(format_time(now))
        .execute(&mut *tx)
        .await?;

        if inserted.rows_affected() != 0 {
            append_audit_tx(
                &mut tx,
                "run",
                &evidence.run_id.to_string(),
                "health.evidence_recorded",
                serde_json::json!({
                    "attempt_id": evidence.attempt_id,
                    "agent_id": evidence.agent_id,
                    "classifier_version": evidence.classification.version,
                    "evidence_class": evidence.classification.class,
                    "failure_family": evidence.classification.family,
                    "diagnostic": diagnostic,
                    "node_was_healthy": evidence.node_was_healthy,
                    "cluster_suppressed": evidence.cluster_suppressed,
                }),
            )
            .await?;
        }

        let input_rows = sqlx::query(
            "SELECT agent_id,occurred_at,evidence_class,failure_family,node_was_healthy \
             FROM health_evidence WHERE blueprint_digest=? AND input_fingerprint=? \
             ORDER BY occurred_at,id",
        )
        .bind(&evidence.blueprint_digest)
        .bind(&evidence.input_fingerprint)
        .fetch_all(&mut *tx)
        .await?;
        let input_observations = input_rows
            .into_iter()
            .map(|row| {
                Ok(InputHealthObservation {
                    agent_id: row.get("agent_id"),
                    occurred_at: parse_time(row.get("occurred_at"))?,
                    classification: HealthClassification {
                        version: evidence.classification.version,
                        class: parse_enum(&row.get::<String, _>("evidence_class"))?,
                        family: parse_enum(&row.get::<String, _>("failure_family"))?,
                    },
                    node_was_healthy: row.get("node_was_healthy"),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let input_evaluation =
            evaluate_input_health(&input_observations, poison_distinct_nodes as usize);
        let previous_input_state = sqlx::query(
            "SELECT state FROM input_health WHERE blueprint_digest=? AND input_fingerprint=?",
        )
        .bind(&evidence.blueprint_digest)
        .bind(&evidence.input_fingerprint)
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| parse_enum::<InputHealthState>(&row.get::<String, _>("state")))
        .transpose()?;
        sqlx::query(
            "INSERT INTO input_health(blueprint_digest,input_fingerprint,state,failure_family,\
             distinct_healthy_nodes,probe_available,revision,updated_at) VALUES (?,?,?,?,?,0,1,?) \
             ON CONFLICT(blueprint_digest,input_fingerprint) DO UPDATE SET \
             state=excluded.state,failure_family=excluded.failure_family,\
             distinct_healthy_nodes=excluded.distinct_healthy_nodes,\
             probe_available=CASE WHEN excluded.state='confirmed' THEN input_health.probe_available ELSE 0 END,\
             revision=input_health.revision+1,updated_at=excluded.updated_at",
        )
        .bind(&evidence.blueprint_digest)
        .bind(&evidence.input_fingerprint)
        .bind(enum_string(input_evaluation.state)?)
        .bind(input_evaluation.family.map(enum_string).transpose()?)
        .bind(input_evaluation.distinct_healthy_nodes as i64)
        .bind(format_time(now))
        .execute(&mut *tx)
        .await?;

        let mut agents_to_rescore = HashSet::from([evidence.agent_id.clone()]);
        if input_evaluation.state == InputHealthState::Confirmed {
            let affected = sqlx::query(
                "SELECT DISTINCT agent_id FROM health_evidence \
                 WHERE blueprint_digest=? AND input_fingerprint=? AND evidence_class='ambiguous'",
            )
            .bind(&evidence.blueprint_digest)
            .bind(&evidence.input_fingerprint)
            .fetch_all(&mut *tx)
            .await?;
            agents_to_rescore.extend(
                affected
                    .into_iter()
                    .map(|row| row.get::<String, _>("agent_id")),
            );
            sqlx::query(
                "UPDATE health_evidence SET retracted=1 \
                 WHERE blueprint_digest=? AND input_fingerprint=? AND evidence_class='ambiguous'",
            )
            .bind(&evidence.blueprint_digest)
            .bind(&evidence.input_fingerprint)
            .execute(&mut *tx)
            .await?;
            reconcile_confirmed_collection_items_tx(
                &mut tx,
                &evidence.blueprint_digest,
                &evidence.input_fingerprint,
                now,
            )
            .await?;
        }
        if previous_input_state != Some(input_evaluation.state) {
            append_audit_tx(
                &mut tx,
                "input_health",
                &evidence.input_fingerprint,
                "input_health.transitioned",
                serde_json::json!({
                    "from": previous_input_state,
                    "to": input_evaluation.state,
                    "failure_family": input_evaluation.family,
                    "distinct_healthy_nodes": input_evaluation.distinct_healthy_nodes,
                    "blueprint_digest": evidence.blueprint_digest,
                }),
            )
            .await?;
        }

        let mut node_views = Vec::new();
        for agent_id in agents_to_rescore {
            node_views.push(rescore_node_tx(&mut tx, &agent_id, now).await?);
        }
        let input = input_health_tx(
            &mut tx,
            &evidence.blueprint_digest,
            &evidence.input_fingerprint,
        )
        .await?
        .context("input health upsert disappeared")?;
        tx.commit().await?;
        Ok((input, node_views))
    }

    pub async fn input_health(
        &self,
        blueprint_digest: &str,
        input_fingerprint: &str,
    ) -> Result<Option<InputHealthView>> {
        let mut tx = self.pool().begin().await?;
        let view = input_health_tx(&mut tx, blueprint_digest, input_fingerprint).await?;
        tx.commit().await?;
        Ok(view)
    }

    pub async fn grant_input_probe(
        &self,
        blueprint_digest: &str,
        input_fingerprint: &str,
    ) -> Result<InputHealthView> {
        let now = Utc::now();
        let mut tx = self.pool().begin().await?;
        let updated = sqlx::query(
            "UPDATE input_health SET probe_available=1,revision=revision+1,updated_at=? \
             WHERE blueprint_digest=? AND input_fingerprint=? AND state='confirmed'",
        )
        .bind(format_time(now))
        .bind(blueprint_digest)
        .bind(input_fingerprint)
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() == 0 {
            bail!("confirmed input health record not found");
        }
        append_audit_tx(
            &mut tx,
            "input_health",
            input_fingerprint,
            "input_health.probe_granted",
            serde_json::json!({"blueprint_digest": blueprint_digest}),
        )
        .await?;
        let view = input_health_tx(&mut tx, blueprint_digest, input_fingerprint)
            .await?
            .context("input health record disappeared")?;
        tx.commit().await?;
        Ok(view)
    }

    /// Atomically decides whether a fingerprint is held. A released probe is
    /// single-use and is consumed by the first caller that reaches placement.
    pub async fn consume_probe_or_is_held(
        &self,
        blueprint_digest: &str,
        input_fingerprint: &str,
    ) -> Result<bool> {
        let now = Utc::now();
        let mut tx = self.pool().begin().await?;
        let row = sqlx::query(
            "SELECT state,probe_available FROM input_health \
             WHERE blueprint_digest=? AND input_fingerprint=?",
        )
        .bind(blueprint_digest)
        .bind(input_fingerprint)
        .fetch_optional(&mut *tx)
        .await?;
        let held = match row {
            None => false,
            Some(row) if row.get::<String, _>("state") != "confirmed" => false,
            Some(row) if row.get::<bool, _>("probe_available") => {
                sqlx::query(
                    "UPDATE input_health SET probe_available=0,revision=revision+1,updated_at=? \
                     WHERE blueprint_digest=? AND input_fingerprint=? AND probe_available=1",
                )
                .bind(format_time(now))
                .bind(blueprint_digest)
                .bind(input_fingerprint)
                .execute(&mut *tx)
                .await?;
                append_audit_tx(
                    &mut tx,
                    "input_health",
                    input_fingerprint,
                    "input_health.probe_consumed",
                    serde_json::json!({"blueprint_digest": blueprint_digest}),
                )
                .await?;
                false
            }
            Some(_) => true,
        };
        tx.commit().await?;
        Ok(held)
    }

    pub async fn set_node_manual_quarantine(
        &self,
        agent_id: &str,
        quarantined: bool,
    ) -> Result<NodeHealthView> {
        let now = Utc::now();
        let target = if quarantined {
            NodeHealthState::ManualQuarantined
        } else {
            NodeHealthState::Probation
        };
        let mut tx = self.pool().begin().await?;
        let current = ensure_node_health_tx(&mut tx, agent_id, now).await?;
        if !quarantined
            && !matches!(
                current.state,
                NodeHealthState::ManualQuarantined | NodeHealthState::AutoQuarantined
            )
        {
            bail!("only a quarantined node can be reset to probation");
        }
        sqlx::query(
            "UPDATE node_health SET state=?,reason_code=?,revision=revision+1,\
             transitioned_at=?,updated_at=? WHERE agent_id=?",
        )
        .bind(enum_string(target)?)
        .bind(if quarantined {
            "administrator_quarantine"
        } else {
            "administrator_reset"
        })
        .bind(format_time(now))
        .bind(format_time(now))
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        append_audit_tx(
            &mut tx,
            "node",
            agent_id,
            if quarantined {
                "node_health.manually_quarantined"
            } else {
                "node_health.reset_to_probation"
            },
            serde_json::json!({"from": current.state, "to": target}),
        )
        .await?;
        let result = node_health_tx(&mut tx, agent_id)
            .await?
            .context("node health record disappeared")?;
        tx.commit().await?;
        Ok(result)
    }

    pub async fn node_health(&self, agent_id: &str) -> Result<Option<NodeHealthView>> {
        let mut tx = self.pool().begin().await?;
        let view = node_health_tx(&mut tx, agent_id).await?;
        tx.commit().await?;
        Ok(view)
    }

    pub async fn list_health_evidence(
        &self,
        agent_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<HealthEvidenceView>> {
        let limit = limit.clamp(1, 200);
        let rows = if let Some(agent_id) = agent_id {
            sqlx::query(
                "SELECT * FROM health_evidence WHERE agent_id=? \
                 ORDER BY occurred_at DESC,id DESC LIMIT ?",
            )
            .bind(agent_id)
            .bind(i64::from(limit))
            .fetch_all(self.pool())
            .await?
        } else {
            sqlx::query("SELECT * FROM health_evidence ORDER BY occurred_at DESC,id DESC LIMIT ?")
                .bind(i64::from(limit))
                .fetch_all(self.pool())
                .await?
        };
        rows.into_iter().map(map_health_evidence).collect()
    }

    pub async fn list_health_evidence_page(
        &self,
        agent_id: Option<&str>,
        cursor_occurred_at: Option<&str>,
        cursor_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<HealthEvidenceView>> {
        let limit = i64::from(limit.clamp(1, 201));
        let rows = if let Some(agent_id) = agent_id {
            sqlx::query(
                "SELECT * FROM health_evidence WHERE agent_id=? \
                 AND (? IS NULL OR occurred_at<? OR (occurred_at=? AND id<?)) \
                 ORDER BY occurred_at DESC,id DESC LIMIT ?",
            )
            .bind(agent_id)
            .bind(cursor_occurred_at)
            .bind(cursor_occurred_at)
            .bind(cursor_occurred_at)
            .bind(cursor_id)
            .bind(limit)
            .fetch_all(self.pool())
            .await?
        } else {
            sqlx::query(
                "SELECT * FROM health_evidence \
                 WHERE (? IS NULL OR occurred_at<? OR (occurred_at=? AND id<?)) \
                 ORDER BY occurred_at DESC,id DESC LIMIT ?",
            )
            .bind(cursor_occurred_at)
            .bind(cursor_occurred_at)
            .bind(cursor_occurred_at)
            .bind(cursor_id)
            .bind(limit)
            .fetch_all(self.pool())
            .await?
        };
        rows.into_iter().map(map_health_evidence).collect()
    }
}

async fn reconcile_confirmed_collection_items_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    blueprint_digest: &str,
    input_fingerprint: &str,
    now: DateTime<Utc>,
) -> Result<()> {
    let now_text = format_time(now);
    // A result is stored before health attribution. Confirmation must fence a
    // retry that became queued in that gap, otherwise the poisoned input can
    // be immediately offered again.
    sqlx::query(
        "UPDATE runs SET state='failed',updated_at=? WHERE state='queued' AND id IN (\
         SELECT run_id FROM health_evidence WHERE blueprint_digest=? AND input_fingerprint=?\
         )",
    )
    .bind(&now_text)
    .bind(blueprint_digest)
    .bind(input_fingerprint)
    .execute(&mut **tx)
    .await?;

    let rows = sqlx::query(
        "UPDATE batch_items SET state='poisoned',failure_code='collection_input_poisoned',updated_at=? \
         WHERE run_id IN (SELECT run_id FROM health_evidence \
           WHERE blueprint_digest=? AND input_fingerprint=?) \
         AND state IN ('queued','failed','suspected_poison') \
         RETURNING batch_id,id,run_id",
    )
    .bind(&now_text)
    .bind(blueprint_digest)
    .bind(input_fingerprint)
    .fetch_all(&mut **tx)
    .await?;
    let mut batches = HashMap::<String, i64>::new();
    for row in rows {
        let batch_id: String = row.get("batch_id");
        *batches.entry(batch_id.clone()).or_default() += 1;
        append_audit_tx(
            tx,
            "batch_item",
            row.get::<String, _>("id").as_str(),
            "batch_item.poison_confirmed",
            serde_json::json!({
                "batch_id": batch_id,
                "run_id": row.get::<String, _>("run_id"),
                "failure_code": "collection_input_poisoned",
            }),
        )
        .await?;
    }
    for (batch_id, confirmed) in batches {
        sqlx::query(
            "UPDATE batches SET poisoned_item_count=poisoned_item_count+?,updated_at=? WHERE id=?",
        )
        .bind(confirmed)
        .bind(&now_text)
        .bind(&batch_id)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "UPDATE batches SET state='completed_with_errors',updated_at=? WHERE id=? \
             AND state='running' AND NOT EXISTS (\
               SELECT 1 FROM runs WHERE batch_id=? AND state IN ('queued','running')\
             )",
        )
        .bind(&now_text)
        .bind(&batch_id)
        .bind(&batch_id)
        .execute(&mut **tx)
        .await?;
        append_audit_tx(
            tx,
            "batch",
            &batch_id,
            "batch.poison_confirmed",
            serde_json::json!({"item_count": confirmed}),
        )
        .await?;
    }
    Ok(())
}

async fn rescore_node_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    now: DateTime<Utc>,
) -> Result<NodeHealthView> {
    let current = ensure_node_health_tx(tx, agent_id, now).await?;
    let since = if current.state == NodeHealthState::Probation {
        current.transitioned_at
    } else {
        now - Duration::minutes(15)
    };
    let rows = sqlx::query(
        "SELECT schedule_id,input_fingerprint,occurred_at,evidence_class,failure_family,\
         retracted,cluster_suppressed FROM health_evidence \
         WHERE agent_id=? AND occurred_at>=? ORDER BY occurred_at,id",
    )
    .bind(agent_id)
    .bind(format_time(since))
    .fetch_all(&mut **tx)
    .await?;
    let observations = rows
        .into_iter()
        .map(|row| {
            Ok(NodeHealthObservation {
                schedule_id: row.get("schedule_id"),
                input_fingerprint: row.get("input_fingerprint"),
                occurred_at: parse_time(row.get("occurred_at"))?,
                classification: HealthClassification {
                    version: 1,
                    class: parse_enum(&row.get::<String, _>("evidence_class"))?,
                    family: parse_enum(&row.get::<String, _>("failure_family"))?,
                },
                retracted: row.get("retracted"),
                cluster_suppressed: row.get("cluster_suppressed"),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let evaluation = evaluate_node_health(&observations, now);
    let next = apply_node_health_evaluation(current.state, &evaluation, &observations);
    let reason = match next {
        NodeHealthState::Healthy => None,
        NodeHealthState::Suspect => Some("diverse_infrastructure_failures"),
        NodeHealthState::AutoQuarantined => Some("automatic_health_threshold"),
        NodeHealthState::ManualQuarantined => Some("administrator_quarantine"),
        NodeHealthState::Probation => Some("administrator_reset"),
    };
    sqlx::query(
        "UPDATE node_health SET state=?,reason_code=?,distinct_failed_inputs=?,\
         distinct_schedules=?,considered_observations=?,failure_rate=?,revision=revision+1,\
         transitioned_at=CASE WHEN state<>? THEN ? ELSE transitioned_at END,updated_at=? \
         WHERE agent_id=?",
    )
    .bind(enum_string(next)?)
    .bind(reason)
    .bind(evaluation.distinct_failed_inputs as i64)
    .bind(evaluation.distinct_schedules as i64)
    .bind(evaluation.considered_observations as i64)
    .bind(evaluation.failure_rate)
    .bind(enum_string(next)?)
    .bind(format_time(now))
    .bind(format_time(now))
    .bind(agent_id)
    .execute(&mut **tx)
    .await?;
    if current.state != next {
        append_audit_tx(
            tx,
            "node",
            agent_id,
            "node_health.transitioned",
            serde_json::json!({
                "from": current.state,
                "to": next,
                "reason_code": reason,
                "distinct_failed_inputs": evaluation.distinct_failed_inputs,
                "distinct_schedules": evaluation.distinct_schedules,
                "considered_observations": evaluation.considered_observations,
                "failure_rate": evaluation.failure_rate,
            }),
        )
        .await?;
    }
    node_health_tx(tx, agent_id)
        .await?
        .context("node health record disappeared after update")
}

async fn ensure_node_health_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    now: DateTime<Utc>,
) -> Result<NodeHealthView> {
    sqlx::query(
        "INSERT OR IGNORE INTO node_health(agent_id,state,transitioned_at,updated_at) \
         VALUES (?,'healthy',?,?)",
    )
    .bind(agent_id)
    .bind(format_time(now))
    .bind(format_time(now))
    .execute(&mut **tx)
    .await?;
    node_health_tx(tx, agent_id)
        .await?
        .context("node health record unavailable")
}

async fn input_health_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    blueprint_digest: &str,
    input_fingerprint: &str,
) -> Result<Option<InputHealthView>> {
    let row =
        sqlx::query("SELECT * FROM input_health WHERE blueprint_digest=? AND input_fingerprint=?")
            .bind(blueprint_digest)
            .bind(input_fingerprint)
            .fetch_optional(&mut **tx)
            .await?;
    row.map(|row| {
        Ok(InputHealthView {
            blueprint_digest: row.get("blueprint_digest"),
            input_fingerprint: row.get("input_fingerprint"),
            state: parse_enum(&row.get::<String, _>("state"))?,
            failure_family: row
                .get::<Option<String>, _>("failure_family")
                .map(|value| parse_enum(&value))
                .transpose()?,
            distinct_healthy_nodes: row.get::<i64, _>("distinct_healthy_nodes") as u32,
            probe_available: row.get("probe_available"),
            revision: row.get("revision"),
            updated_at: parse_time(row.get("updated_at"))?,
        })
    })
    .transpose()
}

async fn node_health_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
) -> Result<Option<NodeHealthView>> {
    let row = sqlx::query("SELECT * FROM node_health WHERE agent_id=?")
        .bind(agent_id)
        .fetch_optional(&mut **tx)
        .await?;
    row.map(|row| {
        Ok(NodeHealthView {
            agent_id: row.get("agent_id"),
            state: parse_enum(&row.get::<String, _>("state"))?,
            reason_code: row.get("reason_code"),
            evaluation: NodeHealthEvaluation {
                distinct_failed_inputs: row.get::<i64, _>("distinct_failed_inputs") as usize,
                distinct_schedules: row.get::<i64, _>("distinct_schedules") as usize,
                considered_observations: row.get::<i64, _>("considered_observations") as usize,
                failure_rate: row.get("failure_rate"),
                decision: match parse_enum::<NodeHealthState>(&row.get::<String, _>("state"))? {
                    NodeHealthState::Healthy => scheduler_core::health::NodeHealthDecision::Healthy,
                    NodeHealthState::Suspect => scheduler_core::health::NodeHealthDecision::Suspect,
                    NodeHealthState::AutoQuarantined
                    | NodeHealthState::ManualQuarantined
                    | NodeHealthState::Probation => {
                        scheduler_core::health::NodeHealthDecision::AutoQuarantine
                    }
                },
            },
            revision: row.get("revision"),
            transitioned_at: parse_time(row.get("transitioned_at"))?,
            updated_at: parse_time(row.get("updated_at"))?,
        })
    })
    .transpose()
}

fn map_health_evidence(row: sqlx::sqlite::SqliteRow) -> Result<HealthEvidenceView> {
    Ok(HealthEvidenceView {
        id: Uuid::parse_str(&row.get::<String, _>("id"))?,
        attempt_id: row
            .get::<Option<String>, _>("attempt_id")
            .map(|value| Uuid::parse_str(&value))
            .transpose()?,
        run_id: Uuid::parse_str(&row.get::<String, _>("run_id"))?,
        schedule_id: Uuid::parse_str(&row.get::<String, _>("schedule_id"))?,
        agent_id: row.get("agent_id"),
        blueprint_digest: row.get("blueprint_digest"),
        input_fingerprint: row.get("input_fingerprint"),
        classifier_version: row.get::<i64, _>("classifier_version") as u32,
        evidence_class: parse_enum(&row.get::<String, _>("evidence_class"))?,
        failure_family: parse_enum(&row.get::<String, _>("failure_family"))?,
        diagnostic: row
            .get::<Option<String>, _>("diagnostic_json")
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
        node_was_healthy: row.get("node_was_healthy"),
        cluster_suppressed: row.get("cluster_suppressed"),
        retracted: row.get("retracted"),
        occurred_at: parse_time(row.get("occurred_at"))?,
    })
}

fn enum_string<T: Serialize>(value: T) -> Result<String> {
    serde_json::to_value(value)?
        .as_str()
        .map(ToOwned::to_owned)
        .context("enum did not serialize as a string")
}

fn parse_enum<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .with_context(|| format!("invalid persisted health enum {value}"))
}

#[cfg(test)]
mod tests {
    use scheduler_core::{
        FailureCode, FailureOrigin, FailureStage,
        health::{HealthEvidenceClass, classify_failure_code},
    };
    use tempfile::TempDir;

    use super::*;

    async fn database() -> (TempDir, Store) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let url = format!("sqlite://{}", directory.path().join("health.db").display());
        let store = Store::connect(&url, None).await.expect("store");
        (directory, store)
    }

    async fn seed_node(store: &Store, agent_id: &str) {
        store
            .upsert_agent(agent_id, agent_id, &Default::default(), 2, 0)
            .await
            .expect("seed agent");
    }

    async fn seed_run(store: &Store, schedule_id: Uuid, run_id: Uuid) {
        let now = format_time(Utc::now());
        sqlx::query(
            "INSERT OR IGNORE INTO schedules(id,name,spec_json,encrypted_snapshot,snapshot_digest,\
             key_id,revision,enabled,webhook_enabled,created_at,updated_at) \
             VALUES (?,'health-test','{}',X'00','blueprint','test',1,1,0,?,?)",
        )
        .bind(schedule_id.to_string())
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .expect("seed schedule");
        sqlx::query(
            "INSERT INTO runs(id,schedule_id,state,trigger_kind,scheduled_at,not_before,\
             encrypted_snapshot,key_id,max_attempts,initial_backoff_seconds,backoff_cap_seconds,\
             attempt_count,created_at,updated_at) \
             VALUES (?,?,'running','manual',?,?,X'00','test',3,1,60,1,?,?)",
        )
        .bind(run_id.to_string())
        .bind(schedule_id.to_string())
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .expect("seed run");
    }

    async fn attach_collection_item(store: &Store, schedule_id: Uuid, run_id: Uuid) -> Uuid {
        let batch_id = Uuid::new_v4();
        let item_id = Uuid::new_v4();
        let trigger_id = Uuid::new_v4();
        let now = format_time(Utc::now());
        sqlx::query(
            "INSERT INTO trigger_identities(id,schedule_id,trigger_kind,scheduled_at,target_kind,target_id,created_at) \
             VALUES (?,?,'manual',?,'batch',?,?)",
        )
        .bind(trigger_id.to_string())
        .bind(schedule_id.to_string())
        .bind(&now)
        .bind(batch_id.to_string())
        .bind(&now)
        .execute(store.pool())
        .await
        .expect("trigger identity");
        sqlx::query(
            "INSERT INTO batches(id,trigger_identity_id,schedule_id,schedule_revision,state,trigger_kind,scheduled_at,\
             encrypted_snapshot,snapshot_digest,key_id,page_size,max_items,max_active_runs,poison_distinct_nodes,\
             ingestion_complete,item_count,valid_item_count,finalized_at,created_at,updated_at) \
             VALUES (?,?,?,1,'running','manual',?,X'00','snapshot','test',500,10000,32,2,1,1,1,?,?,?)",
        )
        .bind(batch_id.to_string())
        .bind(trigger_id.to_string())
        .bind(schedule_id.to_string())
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .expect("batch");
        sqlx::query(
            "INSERT INTO batch_items(id,batch_id,item_index,provider_key_encrypted,provider_key_hmac,\
             encrypted_parameters,encrypted_snapshot,key_id,parameters_digest,state,run_id,created_at,updated_at) \
             VALUES (?,?,0,X'00','provider',X'00',X'00','test','parameters','queued',?,?,?)",
        )
        .bind(item_id.to_string())
        .bind(batch_id.to_string())
        .bind(run_id.to_string())
        .bind(&now)
        .bind(&now)
        .execute(store.pool())
        .await
        .expect("batch item");
        sqlx::query("UPDATE runs SET state='queued',batch_id=?,batch_item_id=? WHERE id=?")
            .bind(batch_id.to_string())
            .bind(item_id.to_string())
            .bind(run_id.to_string())
            .execute(store.pool())
            .await
            .expect("link collection run");
        batch_id
    }

    fn evidence(
        run_id: Uuid,
        schedule_id: Uuid,
        agent_id: &str,
        input: &str,
        code: FailureCode,
    ) -> NewHealthEvidence {
        NewHealthEvidence {
            attempt_id: None,
            run_id,
            schedule_id,
            agent_id: agent_id.into(),
            blueprint_digest: "blueprint-a".into(),
            input_fingerprint: input.into(),
            classification: classify_failure_code(code),
            diagnostic: Some(FailureDiagnostic::new(
                code,
                FailureOrigin::TaskExecutor,
                FailureStage::Execution,
                "safe test diagnostic",
                true,
            )),
            node_was_healthy: true,
            cluster_suppressed: false,
            occurred_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn ordinary_business_failure_never_suspects_input_or_node() {
        let (_directory, store) = database().await;
        seed_node(&store, "node-a").await;
        let schedule = Uuid::new_v4();
        let run = Uuid::new_v4();
        seed_run(&store, schedule, run).await;
        let (input, nodes) = store
            .record_health_evidence(
                &evidence(
                    run,
                    schedule,
                    "node-a",
                    "input-a",
                    FailureCode::ProcessExitedNonZero,
                ),
                2,
            )
            .await
            .expect("record evidence");
        assert_eq!(input.state, InputHealthState::Clear);
        assert_eq!(nodes[0].state, NodeHealthState::Healthy);
    }

    #[tokio::test]
    async fn preacceptance_rejection_requires_another_node_without_hot_loop_fallback() {
        let (_directory, store) = database().await;
        seed_node(&store, "node-a").await;
        let schedule = Uuid::new_v4();
        let run = Uuid::new_v4();
        seed_run(&store, schedule, run).await;
        store
            .record_health_evidence(
                &evidence(
                    run,
                    schedule,
                    "node-a",
                    "input-a",
                    FailureCode::ParameterBindingFailed,
                ),
                2,
            )
            .await
            .expect("record rejection");

        assert_eq!(
            store.retry_avoided_agents(run).await.expect("avoidance"),
            HashSet::from(["node-a".to_owned()])
        );
        assert!(
            store
                .retry_requires_alternative_agent(run)
                .await
                .expect("alternative requirement")
        );

        let crash_run = Uuid::new_v4();
        seed_run(&store, schedule, crash_run).await;
        store
            .record_health_evidence(
                &evidence(
                    crash_run,
                    schedule,
                    "node-a",
                    "input-b",
                    FailureCode::ProcessCrashed,
                ),
                2,
            )
            .await
            .expect("record crash");
        assert!(
            !store
                .retry_requires_alternative_agent(crash_run)
                .await
                .expect("ordinary crash may fall back when no alternative exists")
        );
    }

    #[tokio::test]
    async fn two_healthy_nodes_confirm_poison_and_retract_node_evidence() {
        let (_directory, store) = database().await;
        let schedule = Uuid::new_v4();
        for node in ["node-a", "node-b"] {
            seed_node(&store, node).await;
            let run = Uuid::new_v4();
            seed_run(&store, schedule, run).await;
            store
                .record_health_evidence(
                    &evidence(
                        run,
                        schedule,
                        node,
                        "same-input",
                        FailureCode::ExcelProcessCrashed,
                    ),
                    2,
                )
                .await
                .expect("record evidence");
        }
        let input = store
            .input_health("blueprint-a", "same-input")
            .await
            .expect("input query")
            .expect("input health");
        assert_eq!(input.state, InputHealthState::Confirmed);
        let rows = store
            .list_health_evidence(None, 10)
            .await
            .expect("evidence");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.retracted));
        assert!(
            rows.iter()
                .all(|row| row.evidence_class == HealthEvidenceClass::Ambiguous)
        );
    }

    #[tokio::test]
    async fn confirmed_poison_fences_retries_and_reconciles_batch_items() {
        let (_directory, store) = database().await;
        let schedule = Uuid::new_v4();
        let mut batches = Vec::new();
        for node in ["node-a", "node-b"] {
            seed_node(&store, node).await;
            let run = Uuid::new_v4();
            seed_run(&store, schedule, run).await;
            batches.push(attach_collection_item(&store, schedule, run).await);
            store
                .record_health_evidence(
                    &evidence(
                        run,
                        schedule,
                        node,
                        "same-collection-input",
                        FailureCode::ExcelProcessCrashed,
                    ),
                    2,
                )
                .await
                .expect("record evidence");
        }

        let poisoned: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM batch_items WHERE state='poisoned' AND failure_code='collection_input_poisoned'",
        )
        .fetch_one(store.pool())
        .await
        .expect("poisoned items");
        assert_eq!(poisoned, 2);
        let queued_runs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM runs WHERE state='queued' AND batch_id IS NOT NULL",
        )
        .fetch_one(store.pool())
        .await
        .expect("queued runs");
        assert_eq!(queued_runs, 0);
        for batch_id in batches {
            let row = sqlx::query("SELECT state,poisoned_item_count FROM batches WHERE id=?")
                .bind(batch_id.to_string())
                .fetch_one(store.pool())
                .await
                .expect("batch state");
            assert_eq!(row.get::<String, _>("state"), "completed_with_errors");
            assert_eq!(row.get::<i64, _>("poisoned_item_count"), 1);
        }
    }

    #[tokio::test]
    async fn diverse_failures_quarantine_and_administrator_reset_enters_probation() {
        let (_directory, store) = database().await;
        seed_node(&store, "node-a").await;
        let schedules = [Uuid::new_v4(), Uuid::new_v4()];
        for index in 0..5 {
            let schedule = schedules[index % schedules.len()];
            let run = Uuid::new_v4();
            seed_run(&store, schedule, run).await;
            store
                .record_health_evidence(
                    &evidence(
                        run,
                        schedule,
                        "node-a",
                        &format!("input-{index}"),
                        FailureCode::InfrastructureError,
                    ),
                    2,
                )
                .await
                .expect("record evidence");
        }
        assert_eq!(
            store
                .node_health("node-a")
                .await
                .expect("node query")
                .expect("node health")
                .state,
            NodeHealthState::AutoQuarantined
        );
        assert_eq!(
            store
                .set_node_manual_quarantine("node-a", false)
                .await
                .expect("reset")
                .state,
            NodeHealthState::Probation
        );
    }

    #[tokio::test]
    async fn confirmed_input_probe_is_single_use() {
        let (_directory, store) = database().await;
        let schedule = Uuid::new_v4();
        for node in ["node-a", "node-b"] {
            seed_node(&store, node).await;
            let run = Uuid::new_v4();
            seed_run(&store, schedule, run).await;
            store
                .record_health_evidence(
                    &evidence(
                        run,
                        schedule,
                        node,
                        "probe-input",
                        FailureCode::ProcessCrashed,
                    ),
                    2,
                )
                .await
                .expect("record evidence");
        }
        assert!(
            store
                .consume_probe_or_is_held("blueprint-a", "probe-input")
                .await
                .expect("held")
        );
        store
            .grant_input_probe("blueprint-a", "probe-input")
            .await
            .expect("grant probe");
        assert!(
            !store
                .consume_probe_or_is_held("blueprint-a", "probe-input")
                .await
                .expect("probe consumed")
        );
        assert!(
            store
                .consume_probe_or_is_held("blueprint-a", "probe-input")
                .await
                .expect("held again")
        );
    }
}
