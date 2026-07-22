use std::collections::BTreeMap;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    pub uri: String,
}

pub fn validate_agent_id(id: &str) -> Result<()> {
    let bytes = id.as_bytes();
    let valid = (1..=64).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'_' | b'-'));
    if !valid {
        bail!(
            "agent_id must be 1-64 ASCII letters, digits, dots, underscores, or hyphens and must start and end with a letter or digit"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleSpec {
    pub name: String,
    pub blueprint_ref: ArtifactRef,
    pub parameters_ref: ArtifactRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameter_collection: Option<crate::ParameterCollectionSpec>,
    #[serde(default)]
    pub required_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub cron: Option<CronSpec>,
    #[serde(default)]
    pub webhook_enabled: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronSpec {
    pub expression: String,
    pub timezone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Blueprint {
    pub api_version: String,
    pub executor: ExecutorSpec,
    #[serde(default = "empty_schema")]
    pub parameters_schema: Value,
    #[serde(default)]
    pub parameter_bindings: BTreeMap<String, crate::ParameterBinding>,
    #[serde(default)]
    pub required_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub policy: ExecutionPolicy,
}

fn empty_schema() -> Value {
    serde_json::json!({"type": "object"})
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutorSpec {
    Command(CommandSpec),
    ExcelMacro(ExcelMacroSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandSpec {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub working_directory: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExcelMacroSpec {
    pub workbook_path: String,
    #[serde(default)]
    pub module_name: Option<String>,
    pub macro_name: String,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default = "default_true")]
    pub read_only: bool,
    #[serde(default)]
    pub save_changes: bool,
    #[serde(default)]
    pub visible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPolicy {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_backoff_seconds")]
    pub initial_backoff_seconds: u64,
    #[serde(default = "default_backoff_cap_seconds")]
    pub backoff_cap_seconds: u64,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            max_attempts: default_max_attempts(),
            timeout_seconds: default_timeout_seconds(),
            initial_backoff_seconds: default_backoff_seconds(),
            backoff_cap_seconds: default_backoff_cap_seconds(),
        }
    }
}

fn default_max_attempts() -> u32 {
    3
}

fn default_timeout_seconds() -> u64 {
    3_600
}

fn default_backoff_seconds() -> u64 {
    5
}

fn default_backoff_cap_seconds() -> u64 {
    300
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedScheduleSnapshot {
    pub blueprint: Blueprint,
    pub base_parameters: Value,
    pub blueprint_source_version: Option<String>,
    pub parameters_source_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSnapshot {
    pub executor: ExecutorSpec,
    pub policy: ExecutionPolicy,
    pub required_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub blueprint_digest: String,
    pub parameters_digest: String,
    /// Exact coordinator-resolved parameters used to create this execution.
    ///
    /// Agent-local bindings are deliberately absent because their values never
    /// leave the selected agent. `Option` keeps snapshots written by older
    /// coordinators readable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    /// JSON Pointer paths which the management UI must redact unless its
    /// authenticated debug view was explicitly enabled.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sensitive_parameter_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub late_bindings: Option<crate::LateBindingSnapshot>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl RunState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionAssignment {
    pub schedule_id: Uuid,
    pub run_id: Uuid,
    pub attempt_id: Uuid,
    pub attempt_number: u32,
    pub lease_token: String,
    pub lease_seconds: u64,
    pub snapshot: ExecutionSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub outcome: ExecutionOutcome,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
    pub stdout: String,
    pub stderr: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub error: Option<String>,
    #[serde(default)]
    pub output: OutputMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<FailureDiagnostic>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputMetadata {
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureDiagnostic {
    /// Stable machine-readable code suitable for alerting and automation.
    pub code: FailureCode,
    /// The scheduler component in which the failure originated.
    pub origin: FailureOrigin,
    /// The lifecycle stage that failed.
    pub stage: FailureStage,
    /// Safe, parameter-free operator summary. Detailed output remains encrypted.
    pub summary: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<FailureStatus>,
}

impl FailureDiagnostic {
    pub fn new(
        code: FailureCode,
        origin: FailureOrigin,
        stage: FailureStage,
        summary: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            code,
            origin,
            stage,
            summary: summary.into(),
            retryable,
            status: None,
        }
    }

    pub fn with_status(mut self, status: FailureStatus) -> Self {
        self.status = Some(status);
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureStatus {
    pub process_id: Option<u32>,
    pub status_code: Option<i64>,
    pub status_code_hex: Option<String>,
    pub signal: Option<String>,
    pub hresult: Option<i64>,
    pub hresult_hex: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    AssignmentRejected,
    ParameterBindingFailed,
    ExecutorStartFailed,
    ExecutorProcessCrashed,
    ExecutorProtocolError,
    ProcessSpawnFailed,
    ProcessIsolationFailed,
    ProcessExitedNonZero,
    ProcessCrashed,
    ProcessTimedOut,
    Cancelled,
    AgentLeaseExpired,
    ExcelUnsupported,
    ExcelStartupFailed,
    ExcelWorkbookOpenFailed,
    ExcelCorrelationSetupFailed,
    ExcelMacroFailed,
    ExcelMacroReturnedFailure,
    ExcelInvalidReturn,
    ExcelProcessCrashed,
    ExcelCleanupFailed,
    InfrastructureError,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureOrigin {
    Coordinator,
    Agent,
    TaskExecutor,
    CommandProcess,
    ExcelHostProcess,
    ExcelAutomation,
    ExcelMacro,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureStage {
    Placement,
    Validation,
    ParameterBinding,
    ExecutorStart,
    ProcessStart,
    Isolation,
    Execution,
    ExcelStartup,
    WorkbookOpen,
    CorrelationSetup,
    MacroInvoke,
    MacroResult,
    Cleanup,
    ResultDecode,
    Lease,
    Cancellation,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOutcome {
    Succeeded,
    Failed,
    InfrastructureError,
    TimedOut,
    Cancelled,
    LeaseExpired,
}

impl ExecutionOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::InfrastructureError => "infrastructure_error",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::LeaseExpired => "lease_expired",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalSettings {
    pub revision: i64,
    pub default_timezone: String,
    pub default_max_attempts: u32,
    pub default_timeout_seconds: u64,
    pub lease_seconds: u64,
    pub heartbeat_seconds: u64,
    pub audit_retention_days: u32,
    pub otlp_endpoint: Option<String>,
}

impl Default for GlobalSettings {
    fn default() -> Self {
        Self {
            revision: 1,
            default_timezone: "UTC".into(),
            default_max_attempts: 3,
            default_timeout_seconds: 3_600,
            lease_seconds: 60,
            heartbeat_seconds: 10,
            audit_retention_days: 90,
            otlp_endpoint: None,
        }
    }
}

impl GlobalSettings {
    pub fn validate(&self) -> Result<()> {
        if self.default_timezone.parse::<chrono_tz::Tz>().is_err() {
            bail!("default_timezone must be a valid IANA timezone");
        }
        if self.default_max_attempts == 0 {
            bail!("default_max_attempts must be at least one");
        }
        if self.default_timeout_seconds == 0 {
            bail!("default_timeout_seconds must be at least one");
        }
        if self.heartbeat_seconds < 5 {
            bail!("heartbeat_seconds must be at least five");
        }
        if self.lease_seconds < self.heartbeat_seconds.saturating_mul(3) {
            bail!("lease_seconds must be at least three times heartbeat_seconds");
        }
        if self.audit_retention_days == 0 {
            bail!("audit_retention_days must be at least one");
        }
        if let Some(endpoint) = &self.otlp_endpoint {
            let endpoint = url::Url::parse(endpoint)
                .map_err(|_| anyhow::anyhow!("otlp_endpoint must be a valid URL"))?;
            if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host().is_none() {
                bail!("otlp_endpoint must be an absolute http(s) URL");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSettings {
    pub revision: i64,
    pub enabled: bool,
    pub labels: BTreeMap<String, String>,
    pub max_parallel: u32,
    pub excel_max_parallel: u32,
    pub allowed_command_roots: Vec<String>,
    pub allowed_workbook_roots: Vec<String>,
    pub otlp_endpoint: Option<String>,
}

impl Default for NodeSettings {
    fn default() -> Self {
        Self {
            revision: 1,
            enabled: true,
            labels: BTreeMap::new(),
            max_parallel: 2,
            excel_max_parallel: 1,
            allowed_command_roots: Vec::new(),
            allowed_workbook_roots: Vec::new(),
            otlp_endpoint: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleView {
    pub id: Uuid,
    pub spec: ScheduleSpec,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub webhook_public_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunView {
    pub id: Uuid,
    pub schedule_id: Uuid,
    pub state: RunState,
    pub trigger_kind: String,
    pub scheduled_at: DateTime<Utc>,
    pub attempt_count: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentView {
    pub id: String,
    pub hostname: String,
    pub labels: BTreeMap<String, String>,
    pub capacity: u32,
    pub running: u32,
    pub connected: bool,
    pub desired_settings_revision: i64,
    pub applied_settings_revision: i64,
    pub settings_error: Option<String>,
    pub last_seen_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_ids_are_safe_for_management_path_segments() {
        for valid in ["node1", "excel-01", "site_a.node-42"] {
            validate_agent_id(valid).expect("valid agent ID");
        }
        for invalid in [
            "",
            "-node",
            "node-",
            "node/west",
            "node?west",
            "node#west",
            "node west",
            "nöde",
        ] {
            validate_agent_id(invalid).expect_err("unsafe agent ID must fail");
        }
        validate_agent_id(&"a".repeat(65)).expect_err("agent ID length must be bounded");
    }

    #[test]
    fn global_settings_reject_unsafe_lease_and_invalid_endpoints() {
        let mut settings = GlobalSettings::default();
        assert!(settings.validate().is_ok());

        settings.lease_seconds = settings.heartbeat_seconds * 2;
        assert!(settings.validate().is_err());

        settings = GlobalSettings::default();
        settings.otlp_endpoint = Some("file:///tmp/collector".into());
        assert!(settings.validate().is_err());
    }

    #[test]
    fn execution_snapshots_from_older_coordinators_remain_readable() {
        let snapshot: ExecutionSnapshot = serde_json::from_value(serde_json::json!({
            "executor": {"kind": "command", "program": "runner"},
            "policy": {
                "max_attempts": 1,
                "timeout_seconds": 60,
                "initial_backoff_seconds": 1,
                "backoff_cap_seconds": 30
            },
            "required_labels": {},
            "blueprint_digest": "blueprint",
            "parameters_digest": "parameters"
        }))
        .expect("legacy snapshot");

        assert!(snapshot.parameters.is_none());
        assert!(snapshot.sensitive_parameter_paths.is_empty());
    }
}
