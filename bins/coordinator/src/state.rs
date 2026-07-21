use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use anyhow::{Result, bail};
use scheduler_core::{
    AdapterRegistry, ExecutionSnapshot, ParameterBindingSource, SnapshotCipher,
    health::NodeHealthState,
};
use scheduler_protocol::control::{CoordinatorMessage, SettingsUpdate, coordinator_message};
use scheduler_store::Store;
use tokio::sync::{RwLock, mpsc};

use crate::auth::AuthManager;
use crate::collection_runtime::CollectionSourceRegistry;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub cipher: SnapshotCipher,
    pub adapters: AdapterRegistry,
    pub auth: AuthManager,
    pub sessions: Arc<RwLock<HashMap<String, AgentSession>>>,
    pub internal_rest_url: String,
    pub internal_admin_token: String,
    pub collection_sources: CollectionSourceRegistry,
    pub collection_worker_id: String,
}

#[derive(Clone)]
pub struct AgentSession {
    pub connection_id: uuid::Uuid,
    pub tx: mpsc::Sender<Result<CoordinatorMessage, tonic::Status>>,
    pub enabled: bool,
    pub labels: BTreeMap<String, String>,
    pub capacity: u32,
    pub applied_settings_revision: i64,
    pub running: u32,
    pub health: NodeHealthState,
    pub last_assigned: Instant,
    pub environment_bindings: BTreeSet<String>,
    pub secret_file_bindings: BTreeSet<String>,
}

impl AppState {
    pub async fn reserve_agent(
        &self,
        snapshot: &ExecutionSnapshot,
    ) -> Option<(
        String,
        mpsc::Sender<Result<CoordinatorMessage, tonic::Status>>,
    )> {
        self.reserve_agent_avoiding(snapshot, &HashSet::new()).await
    }

    pub async fn reserve_agent_avoiding(
        &self,
        snapshot: &ExecutionSnapshot,
        excluded_agents: &HashSet<String>,
    ) -> Option<(
        String,
        mpsc::Sender<Result<CoordinatorMessage, tonic::Status>>,
    )> {
        let mut sessions = self.sessions.write().await;
        let selected = sessions
            .iter()
            .filter(|(agent_id, session)| {
                let capacity = session
                    .health
                    .capacity_limit()
                    .map_or(session.capacity, |limit| session.capacity.min(limit));
                session.enabled
                    && !excluded_agents.contains(*agent_id)
                    && session.health.accepts_new_work()
                    && session.running < capacity
                    && snapshot
                        .required_labels
                        .iter()
                        .all(|(key, value)| session.labels.get(key) == Some(value))
                    && supports_parameter_bindings(session, snapshot)
            })
            .min_by(|(left_id, left), (right_id, right)| {
                let left_util = left.running as f64 / left.capacity.max(1) as f64;
                let right_util = right.running as f64 / right.capacity.max(1) as f64;
                left_util
                    .total_cmp(&right_util)
                    .then(left.last_assigned.cmp(&right.last_assigned))
                    .then(left_id.cmp(right_id))
            })
            .map(|(id, _)| id.clone())?;
        let session = sessions.get_mut(&selected)?;
        session.running = session.running.saturating_add(1);
        session.last_assigned = Instant::now();
        Some((selected, session.tx.clone()))
    }

    pub async fn release_agent_slot(&self, agent_id: &str) {
        if let Some(session) = self.sessions.write().await.get_mut(agent_id) {
            session.running = session.running.saturating_sub(1);
        }
    }

    pub async fn update_agent_health(&self, agent_id: &str, health: NodeHealthState) {
        if let Some(session) = self.sessions.write().await.get_mut(agent_id) {
            session.health = health;
        }
    }

    /// Applies a durable node-health view to the live placement cache and
    /// emits a transition counter only when the store moved the state. Node
    /// identifiers stay in traces/logs and are deliberately not metric labels.
    pub async fn apply_agent_health_view(&self, view: &scheduler_store::NodeHealthView) {
        self.update_agent_health(&view.agent_id, view.state).await;
        if view.transitioned_at == view.updated_at {
            let state = match view.state {
                NodeHealthState::Healthy => "healthy",
                NodeHealthState::Suspect => "suspect",
                NodeHealthState::AutoQuarantined => "auto_quarantined",
                NodeHealthState::ManualQuarantined => "manual_quarantined",
                NodeHealthState::Probation => "probation",
            };
            opentelemetry::global::meter("scheduler-coordinator")
                .u64_counter("scheduler.node_health.transitions")
                .build()
                .add(1, &[opentelemetry::KeyValue::new("state", state)]);
        }
    }

    pub async fn send_to_agent(&self, agent_id: &str, message: CoordinatorMessage) -> bool {
        let tx = self
            .sessions
            .read()
            .await
            .get(agent_id)
            .map(|session| session.tx.clone());
        match tx {
            Some(tx) => tx.send(Ok(message)).await.is_ok(),
            None => false,
        }
    }

    pub async fn push_node_settings(&self, agent_id: &str) -> Result<bool> {
        let settings = self
            .store
            .get_node_settings(agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("node settings not found"))?;
        let heartbeat_seconds = self.store.get_global_settings().await?.heartbeat_seconds;
        Ok(self
            .send_to_agent(
                agent_id,
                CoordinatorMessage {
                    payload: Some(coordinator_message::Payload::Settings(SettingsUpdate {
                        revision: settings.revision,
                        settings_json: serde_json::to_string(&settings)?,
                        heartbeat_seconds,
                    })),
                },
            )
            .await)
    }

    pub async fn push_all_node_settings(&self) -> Result<()> {
        let agent_ids = self
            .sessions
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for agent_id in agent_ids {
            if self.store.get_node_settings(&agent_id).await?.is_none() {
                bail!("node settings not found for connected agent {agent_id}");
            }
            self.push_node_settings(&agent_id).await?;
        }
        Ok(())
    }
}

fn supports_parameter_bindings(session: &AgentSession, snapshot: &ExecutionSnapshot) -> bool {
    snapshot.late_bindings.as_ref().is_none_or(|late| {
        late.bindings.values().all(|binding| match binding.source {
            ParameterBindingSource::Environment => {
                session.environment_bindings.contains(&binding.name)
            }
            ParameterBindingSource::SecretFile => {
                session.secret_file_bindings.contains(&binding.name)
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use scheduler_core::{
        CommandSpec, ExecutionPolicy, ExecutorSpec, LateBindingSnapshot, ParameterBinding,
        ParameterBindingValueType,
    };
    use tokio::sync::mpsc;

    use super::*;

    fn snapshot(source: ParameterBindingSource, name: &str) -> ExecutionSnapshot {
        let executor = ExecutorSpec::Command(CommandSpec {
            program: "runner".into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            working_directory: None,
        });
        ExecutionSnapshot {
            executor: executor.clone(),
            policy: ExecutionPolicy::default(),
            required_labels: BTreeMap::new(),
            blueprint_digest: "test-blueprint".into(),
            parameters_digest: "safe".into(),
            late_bindings: Some(LateBindingSnapshot {
                executor_template: executor,
                parameters_schema: serde_json::json!({"type": "object"}),
                parameters: serde_json::json!({}),
                bindings: BTreeMap::from([(
                    "credential".into(),
                    ParameterBinding {
                        source,
                        name: name.into(),
                        value_type: ParameterBindingValueType::String,
                        sensitive: true,
                    },
                )]),
            }),
        }
    }

    fn session() -> AgentSession {
        let (tx, _rx) = mpsc::channel(1);
        AgentSession {
            connection_id: uuid::Uuid::new_v4(),
            tx,
            enabled: true,
            labels: BTreeMap::new(),
            capacity: 1,
            applied_settings_revision: 1,
            running: 0,
            health: NodeHealthState::Healthy,
            last_assigned: Instant::now(),
            environment_bindings: BTreeSet::from(["TASK_TOKEN".into()]),
            secret_file_bindings: BTreeSet::from(["reporting-password".into()]),
        }
    }

    #[test]
    fn placement_requires_the_binding_name_from_the_matching_source() {
        let session = session();
        assert!(supports_parameter_bindings(
            &session,
            &snapshot(ParameterBindingSource::Environment, "TASK_TOKEN")
        ));
        assert!(supports_parameter_bindings(
            &session,
            &snapshot(ParameterBindingSource::SecretFile, "reporting-password")
        ));
        assert!(!supports_parameter_bindings(
            &session,
            &snapshot(ParameterBindingSource::Environment, "reporting-password")
        ));
        assert!(!supports_parameter_bindings(
            &session,
            &snapshot(ParameterBindingSource::SecretFile, "missing")
        ));
    }
}
