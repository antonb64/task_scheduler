use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant,
};

use scheduler_core::{AdapterRegistry, SnapshotCipher};
use scheduler_protocol::control::CoordinatorMessage;
use scheduler_store::Store;
use tokio::sync::{RwLock, mpsc};

use crate::auth::AuthManager;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub cipher: SnapshotCipher,
    pub adapters: AdapterRegistry,
    pub auth: AuthManager,
    pub sessions: Arc<RwLock<HashMap<String, AgentSession>>>,
    pub internal_rest_url: String,
    pub internal_admin_token: String,
}

#[derive(Clone)]
pub struct AgentSession {
    pub tx: mpsc::Sender<Result<CoordinatorMessage, tonic::Status>>,
    pub labels: BTreeMap<String, String>,
    pub capacity: u32,
    pub running: u32,
    pub last_assigned: Instant,
}

impl AppState {
    pub async fn reserve_agent(
        &self,
        required: &BTreeMap<String, String>,
    ) -> Option<(
        String,
        mpsc::Sender<Result<CoordinatorMessage, tonic::Status>>,
    )> {
        let mut sessions = self.sessions.write().await;
        let selected = sessions
            .iter()
            .filter(|(_, session)| {
                session.running < session.capacity
                    && required
                        .iter()
                        .all(|(key, value)| session.labels.get(key) == Some(value))
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
}
