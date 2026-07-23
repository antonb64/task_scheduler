use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DailyWindow {
    Current,
    Previous,
}

impl DailyWindow {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Previous => "previous",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DailyVerdict {
    Idle,
    Green,
    Pending,
    Degraded,
    Red,
    Unknown,
}

impl DailyVerdict {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Green => "green",
            Self::Pending => "pending",
            Self::Degraded => "degraded",
            Self::Red => "red",
            Self::Unknown => "unknown",
        }
    }

    pub const fn severity(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Green => 1,
            Self::Pending => 2,
            Self::Degraded => 3,
            Self::Red => 4,
            Self::Unknown => 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyScheduleStatus {
    pub schedule_id: Uuid,
    pub schedule_name: String,
    pub operations_day: String,
    pub operations_timezone: String,
    pub completion_deadline_seconds: u64,
    pub expected_triggers: u64,
    pub materialized_triggers: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub pending: u64,
    pub overdue: u64,
    pub missing_due: u64,
    pub retries: u64,
    pub attempt_anomalies: u64,
    pub ready_items: u64,
    pub queued_items: u64,
    pub running_items: u64,
    pub succeeded_items: u64,
    pub failed_items: u64,
    pub cancelled_items: u64,
    pub invalid_items: u64,
    pub suspected_poison_items: u64,
    pub poisoned_items: u64,
    pub held_items: u64,
    pub coverage_complete: bool,
    pub verdict: DailyVerdict,
    pub reasons: Vec<String>,
    pub last_success_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyObservabilitySnapshot {
    pub generated_at: DateTime<Utc>,
    pub window: DailyWindow,
    pub cluster_verdict: DailyVerdict,
    pub coverage_gap: bool,
    pub schedules: Vec<DailyScheduleStatus>,
}
