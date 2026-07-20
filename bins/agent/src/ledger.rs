use std::str::FromStr;

use anyhow::Result;
use chrono::Utc;
use scheduler_core::ExecutionAssignment;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

#[derive(Clone)]
pub struct Ledger {
    pool: SqlitePool,
}

#[derive(Debug, Clone)]
pub struct PendingResult {
    pub attempt_id: String,
    pub lease_token: String,
    pub result_json: String,
}

impl Ledger {
    pub async fn connect(url: &str) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS assignments(attempt_id TEXT PRIMARY KEY,lease_token TEXT NOT NULL,assignment_json TEXT NOT NULL,state TEXT NOT NULL,result_json TEXT,updated_at TEXT NOT NULL)")
            .execute(&pool).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS local_settings(id INTEGER PRIMARY KEY CHECK(id=1),revision INTEGER NOT NULL,settings_json TEXT NOT NULL,updated_at TEXT NOT NULL)")
            .execute(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn accept(&self, assignment: &ExecutionAssignment, json: &str) -> Result<bool> {
        let result = sqlx::query("INSERT OR IGNORE INTO assignments(attempt_id,lease_token,assignment_json,state,updated_at) VALUES (?,?,?,'accepted',?)")
            .bind(assignment.attempt_id.to_string()).bind(&assignment.lease_token).bind(json).bind(Utc::now().to_rfc3339()).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn save_result(&self, attempt_id: &str, result_json: &str) -> Result<()> {
        sqlx::query(
            "UPDATE assignments SET state='finished',result_json=?,updated_at=? WHERE attempt_id=?",
        )
        .bind(result_json)
        .bind(Utc::now().to_rfc3339())
        .bind(attempt_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn acknowledge(&self, attempt_id: &str) -> Result<()> {
        sqlx::query("UPDATE assignments SET state='acknowledged',updated_at=? WHERE attempt_id=?")
            .bind(Utc::now().to_rfc3339())
            .bind(attempt_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn pending_results(&self) -> Result<Vec<PendingResult>> {
        let rows = sqlx::query("SELECT attempt_id,lease_token,result_json FROM assignments WHERE state='finished' AND result_json IS NOT NULL")
            .fetch_all(&self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| PendingResult {
                attempt_id: row.get("attempt_id"),
                lease_token: row.get("lease_token"),
                result_json: row.get("result_json"),
            })
            .collect())
    }

    pub async fn result(&self, attempt_id: &str) -> Result<Option<PendingResult>> {
        let row = sqlx::query("SELECT attempt_id,lease_token,result_json FROM assignments WHERE attempt_id=? AND state='finished'")
            .bind(attempt_id).fetch_optional(&self.pool).await?;
        Ok(row.map(|row| PendingResult {
            attempt_id: row.get("attempt_id"),
            lease_token: row.get("lease_token"),
            result_json: row.get("result_json"),
        }))
    }

    pub async fn save_settings(&self, revision: i64, json: &str) -> Result<()> {
        sqlx::query("INSERT INTO local_settings(id,revision,settings_json,updated_at) VALUES (1,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,settings_json=excluded.settings_json,updated_at=excluded.updated_at")
            .bind(revision).bind(json).bind(Utc::now().to_rfc3339()).execute(&self.pool).await?;
        Ok(())
    }
}
