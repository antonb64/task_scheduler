use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use scheduler_core::DashboardConfig;
use sqlx::Row;

use crate::{Store, parse_time};

#[derive(Debug, Clone, serde::Serialize)]
pub struct DashboardDocument {
    pub config: DashboardConfig,
    pub revision: i64,
    pub updated_at: DateTime<Utc>,
}

impl Store {
    pub async fn get_dashboard(&self) -> Result<DashboardDocument> {
        let row = sqlx::query(
            "SELECT document_json,revision,updated_at FROM settings_documents \
             WHERE document_key='dashboard'",
        )
        .fetch_one(self.pool())
        .await
        .context("dashboard settings document is missing")?;
        let config: DashboardConfig = serde_json::from_str(row.get("document_json"))?;
        config.validate()?;
        Ok(DashboardDocument {
            config,
            revision: row.get("revision"),
            updated_at: parse_time(row.get("updated_at"))?,
        })
    }

    pub async fn update_dashboard(
        &self,
        config: &DashboardConfig,
        expected_revision: i64,
        lock_token: &str,
    ) -> Result<i64> {
        config.validate()?;
        self.update_settings(
            "dashboard",
            expected_revision,
            &serde_json::to_string(config)?,
            lock_token,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    async fn database() -> (TempDir, Store) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let url = format!(
            "sqlite://{}",
            directory.path().join("dashboard.db").display()
        );
        let store = Store::connect(&url, None).await.expect("store");
        (directory, store)
    }

    #[tokio::test]
    async fn dashboard_uses_settings_lock_and_revision_fencing() {
        let (_directory, store) = database().await;
        let original = store.get_dashboard().await.expect("dashboard");
        let lock = store
            .acquire_lock("dashboard", "dashboard-test")
            .await
            .expect("lock");
        let revision = store
            .update_dashboard(&original.config, original.revision, &lock.lock_token)
            .await
            .expect("update");
        assert_eq!(revision, original.revision + 1);
        assert!(
            store
                .update_dashboard(&original.config, original.revision, &lock.lock_token)
                .await
                .is_err()
        );
    }
}
