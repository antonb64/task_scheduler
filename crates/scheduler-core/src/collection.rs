use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::ArtifactRef;

pub const DEFAULT_COLLECTION_PAGE_SIZE: u32 = 500;
pub const MAX_COLLECTION_PAGE_SIZE: u32 = 1_000;
pub const DEFAULT_COLLECTION_MAX_ITEMS: u32 = 10_000;
pub const MAX_COLLECTION_ITEMS: u32 = 10_000;
pub const DEFAULT_COLLECTION_MAX_ACTIVE_RUNS: u32 = 32;
pub const MAX_COLLECTION_ACTIVE_RUNS: u32 = 1_000;
pub const DEFAULT_POISON_DISTINCT_NODES: u32 = 2;

/// Describes a collection which is resolved once for each schedule trigger.
///
/// This lives on `ScheduleSpec` rather than `Blueprint`: a schedule still has
/// exactly one blueprint and can independently choose whether each occurrence
/// executes one parameter object or a collection of parameter objects.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ParameterCollectionSpec {
    pub source_ref: ArtifactRef,
    #[serde(default = "default_page_size")]
    pub page_size: u32,
    #[serde(default = "default_max_items")]
    pub max_items: u32,
    #[serde(default = "default_max_active_runs")]
    pub max_active_runs: u32,
    #[serde(default = "default_poison_distinct_nodes")]
    pub poison_distinct_nodes: u32,
}

impl ParameterCollectionSpec {
    pub fn validate(&self) -> Result<()> {
        let source = self.source_ref.uri.trim();
        if source.is_empty() {
            bail!("parameter collection source_ref.uri must not be empty");
        }
        let parsed = Url::parse(source)
            .map_err(|error| anyhow::anyhow!("invalid parameter collection source URI: {error}"))?;
        match parsed.scheme() {
            "file" | "http" | "https" | "connector" => {}
            scheme => bail!("unsupported parameter collection source scheme {scheme}"),
        }
        if !(1..=MAX_COLLECTION_PAGE_SIZE).contains(&self.page_size) {
            bail!(
                "parameter collection page_size must be between 1 and {MAX_COLLECTION_PAGE_SIZE}"
            );
        }
        if !(1..=MAX_COLLECTION_ITEMS).contains(&self.max_items) {
            bail!("parameter collection max_items must be between 1 and {MAX_COLLECTION_ITEMS}");
        }
        if !(1..=MAX_COLLECTION_ACTIVE_RUNS).contains(&self.max_active_runs) {
            bail!(
                "parameter collection max_active_runs must be between 1 and {MAX_COLLECTION_ACTIVE_RUNS}"
            );
        }
        if !(2..=32).contains(&self.poison_distinct_nodes) {
            bail!("parameter collection poison_distinct_nodes must be between 2 and 32");
        }
        Ok(())
    }
}

fn default_page_size() -> u32 {
    DEFAULT_COLLECTION_PAGE_SIZE
}

fn default_max_items() -> u32 {
    DEFAULT_COLLECTION_MAX_ITEMS
}

fn default_max_active_runs() -> u32 {
    DEFAULT_COLLECTION_MAX_ACTIVE_RUNS
}

fn default_poison_distinct_nodes() -> u32 {
    DEFAULT_POISON_DISTINCT_NODES
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BatchState {
    Scheduled,
    Collecting,
    Running,
    Succeeded,
    CompletedWithErrors,
    Failed,
    Cancelled,
}

impl BatchState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Collecting => "collecting",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::CompletedWithErrors => "completed_with_errors",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "scheduled" => Ok(Self::Scheduled),
            "collecting" => Ok(Self::Collecting),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "completed_with_errors" => Ok(Self::CompletedWithErrors),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => bail!("unknown batch state {value}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BatchItemState {
    Ready,
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Invalid,
    SuspectedPoison,
    Poisoned,
    Held,
}

impl BatchItemState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Invalid => "invalid",
            Self::SuspectedPoison => "suspected_poison",
            Self::Poisoned => "poisoned",
            Self::Held => "held",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "ready" => Ok(Self::Ready),
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "invalid" => Ok(Self::Invalid),
            "suspected_poison" => Ok(Self::SuspectedPoison),
            "poisoned" => Ok(Self::Poisoned),
            "held" => Ok(Self::Held),
            _ => bail!("unknown batch item state {value}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_bounded_and_backward_compatible() {
        let spec: ParameterCollectionSpec = serde_json::from_value(serde_json::json!({
            "source_ref": {"uri": "file:///var/lib/scheduler/reports.ndjson"}
        }))
        .expect("collection spec");
        assert_eq!(spec.page_size, DEFAULT_COLLECTION_PAGE_SIZE);
        assert_eq!(spec.max_items, DEFAULT_COLLECTION_MAX_ITEMS);
        assert_eq!(spec.max_active_runs, DEFAULT_COLLECTION_MAX_ACTIVE_RUNS);
        assert_eq!(spec.poison_distinct_nodes, DEFAULT_POISON_DISTINCT_NODES);
        spec.validate().expect("valid defaults");
    }

    #[test]
    fn limits_and_source_schemes_are_validated() {
        let mut spec = ParameterCollectionSpec {
            source_ref: ArtifactRef {
                uri: "connector://reporting/monthly".into(),
            },
            page_size: 1,
            max_items: MAX_COLLECTION_ITEMS,
            max_active_runs: 1,
            poison_distinct_nodes: 2,
        };
        spec.validate().expect("valid boundary values");
        spec.max_items += 1;
        assert!(spec.validate().is_err());
        spec.max_items = 1;
        spec.source_ref.uri = "ftp://example.test/items".into();
        assert!(spec.validate().is_err());
    }

    #[test]
    fn legacy_schedule_json_round_trips_without_a_collection_field() {
        let legacy = r#"{"name":"legacy","blueprint_ref":{"uri":"file:///blueprint"},"parameters_ref":{"uri":"file:///parameters"},"required_labels":{},"cron":null,"webhook_enabled":false,"enabled":true}"#;
        let schedule: crate::ScheduleSpec = serde_json::from_str(legacy).expect("legacy schedule");
        assert!(schedule.parameter_collection.is_none());
        assert_eq!(
            serde_json::to_string(&schedule).expect("serialize legacy schedule"),
            legacy
        );
    }
}
