use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row;

use crate::{Store, now_string, parse_time};

#[derive(Debug, Clone)]
pub struct NewBlueprintRevision {
    pub digest: String,
    pub resolved_snapshot_digest: String,
    pub source_ref: String,
    pub source_version: Option<String>,
    pub executor_kind: String,
    pub required_labels: Value,
    pub execution_policy: Value,
    pub parameter_schema: Value,
    pub binding_declarations: Value,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct BlueprintRevisionView {
    pub digest: String,
    pub source_ref: String,
    pub source_version: Option<String>,
    pub loaded_at: DateTime<Utc>,
    pub executor_kind: String,
    pub required_labels: Value,
    pub execution_policy: Value,
    pub parameter_schema: Value,
    pub binding_declarations: Value,
    pub current_schedule_count: u32,
    pub retained_schedule_revision_count: u32,
}

impl Store {
    pub async fn register_blueprint_revision(&self, revision: &NewBlueprintRevision) -> Result<()> {
        let now = now_string();
        let mut tx = self.pool().begin().await?;
        sqlx::query(
            "INSERT OR IGNORE INTO blueprint_revisions(digest,source_ref,source_version,loaded_at,\
             executor_kind,required_labels_json,execution_policy_json,parameter_schema_json,\
             binding_declarations_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&revision.digest)
        .bind(&revision.source_ref)
        .bind(&revision.source_version)
        .bind(&now)
        .bind(&revision.executor_kind)
        .bind(serde_json::to_string(&revision.required_labels)?)
        .bind(serde_json::to_string(&revision.execution_policy)?)
        .bind(serde_json::to_string(&revision.parameter_schema)?)
        .bind(serde_json::to_string(&revision.binding_declarations)?)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO resolved_snapshot_blueprints(snapshot_digest,blueprint_digest,created_at) \
             VALUES (?,?,?)",
        )
        .bind(&revision.resolved_snapshot_digest)
        .bind(&revision.digest)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_blueprint_revisions(&self, limit: u32) -> Result<Vec<BlueprintRevisionView>> {
        let rows = sqlx::query(
            "SELECT b.*,\
             (SELECT COUNT(DISTINCT current.schedule_id) FROM schedule_blueprint_revisions current \
              WHERE current.blueprint_digest=b.digest AND current.is_current=1) AS current_count,\
             (SELECT COUNT(*) FROM schedule_blueprint_revisions retained \
              WHERE retained.blueprint_digest=b.digest) AS retained_count \
             FROM blueprint_revisions b \
             ORDER BY b.loaded_at DESC,b.digest DESC LIMIT ?",
        )
        .bind(i64::from(limit.clamp(1, 200)))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(blueprint_revision_from_row).collect()
    }

    pub async fn list_blueprint_revisions_page(
        &self,
        cursor_loaded_at: Option<&str>,
        cursor_digest: Option<&str>,
        limit: u32,
    ) -> Result<Vec<BlueprintRevisionView>> {
        let rows = sqlx::query(
            "SELECT b.*,\
             (SELECT COUNT(DISTINCT current.schedule_id) FROM schedule_blueprint_revisions current \
              WHERE current.blueprint_digest=b.digest AND current.is_current=1) AS current_count,\
             (SELECT COUNT(*) FROM schedule_blueprint_revisions retained \
              WHERE retained.blueprint_digest=b.digest) AS retained_count \
             FROM blueprint_revisions b \
             WHERE (? IS NULL OR b.loaded_at<? OR (b.loaded_at=? AND b.digest<?)) \
             ORDER BY b.loaded_at DESC,b.digest DESC LIMIT ?",
        )
        .bind(cursor_loaded_at)
        .bind(cursor_loaded_at)
        .bind(cursor_loaded_at)
        .bind(cursor_digest)
        .bind(i64::from(limit.clamp(1, 201)))
        .fetch_all(self.pool())
        .await?;
        rows.into_iter().map(blueprint_revision_from_row).collect()
    }
}

fn blueprint_revision_from_row(row: sqlx::sqlite::SqliteRow) -> Result<BlueprintRevisionView> {
    Ok(BlueprintRevisionView {
        digest: row.get("digest"),
        source_ref: row.get("source_ref"),
        source_version: row.get("source_version"),
        loaded_at: parse_time(row.get("loaded_at"))?,
        executor_kind: row.get("executor_kind"),
        required_labels: serde_json::from_str(&row.get::<String, _>("required_labels_json"))?,
        execution_policy: serde_json::from_str(&row.get::<String, _>("execution_policy_json"))?,
        parameter_schema: serde_json::from_str(&row.get::<String, _>("parameter_schema_json"))?,
        binding_declarations: serde_json::from_str(
            &row.get::<String, _>("binding_declarations_json"),
        )?,
        current_schedule_count: row.get::<i64, _>("current_count") as u32,
        retained_schedule_revision_count: row.get::<i64, _>("retained_count") as u32,
    })
}
