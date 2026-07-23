use std::collections::HashSet;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const MAX_DASHBOARD_SCHEDULES: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DashboardWidget {
    ClusterCapacity,
    ActiveBatches,
    RecentFailures,
    QuarantinedNodes,
    ConnectorHealth,
    TelemetryHealth,
    SelectedSchedules,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DashboardConfig {
    #[serde(default)]
    pub schedule_ids: Vec<Uuid>,
    #[serde(default = "default_widgets")]
    pub widgets: Vec<DashboardWidget>,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            schedule_ids: Vec::new(),
            widgets: default_widgets(),
        }
    }
}

impl DashboardConfig {
    pub fn validate(&self) -> Result<()> {
        if self.schedule_ids.len() > MAX_DASHBOARD_SCHEDULES {
            bail!("dashboard can contain at most {MAX_DASHBOARD_SCHEDULES} schedules");
        }
        if self.schedule_ids.iter().collect::<HashSet<_>>().len() != self.schedule_ids.len() {
            bail!("dashboard schedule IDs must be unique");
        }
        if self.widgets.iter().collect::<HashSet<_>>().len() != self.widgets.len() {
            bail!("dashboard widgets must be unique");
        }
        Ok(())
    }
}

fn default_widgets() -> Vec<DashboardWidget> {
    vec![
        DashboardWidget::ClusterCapacity,
        DashboardWidget::ActiveBatches,
        DashboardWidget::RecentFailures,
        DashboardWidget::QuarantinedNodes,
        DashboardWidget::ConnectorHealth,
        DashboardWidget::TelemetryHealth,
        DashboardWidget::SelectedSchedules,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_defaults_are_complete_and_duplicates_are_rejected() {
        let mut config = DashboardConfig::default();
        config.validate().expect("defaults");
        let schedule = Uuid::new_v4();
        config.schedule_ids = vec![schedule, schedule];
        assert!(config.validate().is_err());
        config.schedule_ids.clear();
        config.widgets.push(DashboardWidget::ClusterCapacity);
        assert!(config.validate().is_err());
    }

    #[test]
    fn maximum_dashboard_schedule_set_is_bounded() {
        let mut config = DashboardConfig {
            schedule_ids: (0..MAX_DASHBOARD_SCHEDULES)
                .map(|_| Uuid::new_v4())
                .collect(),
            ..DashboardConfig::default()
        };
        config.validate().expect("maximum schedule set");
        config.schedule_ids.push(Uuid::new_v4());
        assert!(config.validate().is_err());
    }
}
