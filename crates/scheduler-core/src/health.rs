use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{ExecutionOutcome, FailureCode, FailureDiagnostic};

/// Version of the stable failure-attribution rules persisted with health evidence.
pub const HEALTH_CLASSIFIER_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureFamily {
    Functional,
    Cancellation,
    Lease,
    ExecutorAvailability,
    BindingAvailability,
    Isolation,
    ProcessLaunch,
    Crash,
    Timeout,
    ExcelHost,
    ExcelInvocation,
    ExcelContract,
    Cleanup,
    Infrastructure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthEvidenceClass {
    Functional,
    Ignore,
    ClusterSensitive,
    Ambiguous,
    StrongNodeLocal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthClassification {
    pub version: u32,
    pub class: HealthEvidenceClass,
    pub family: FailureFamily,
}

const fn classification(class: HealthEvidenceClass, family: FailureFamily) -> HealthClassification {
    HealthClassification {
        version: HEALTH_CLASSIFIER_VERSION,
        class,
        family,
    }
}

pub fn classify_result(
    outcome: ExecutionOutcome,
    diagnostic: Option<&FailureDiagnostic>,
) -> HealthClassification {
    if outcome == ExecutionOutcome::Succeeded {
        return classification(HealthEvidenceClass::Functional, FailureFamily::Functional);
    }
    if let Some(diagnostic) = diagnostic {
        return classify_failure_code(diagnostic.code);
    }
    match outcome {
        ExecutionOutcome::Succeeded => {
            classification(HealthEvidenceClass::Functional, FailureFamily::Functional)
        }
        ExecutionOutcome::Cancelled => {
            classification(HealthEvidenceClass::Ignore, FailureFamily::Cancellation)
        }
        ExecutionOutcome::LeaseExpired => {
            classification(HealthEvidenceClass::ClusterSensitive, FailureFamily::Lease)
        }
        ExecutionOutcome::TimedOut => {
            classification(HealthEvidenceClass::Ambiguous, FailureFamily::Timeout)
        }
        ExecutionOutcome::Failed | ExecutionOutcome::InfrastructureError => classification(
            HealthEvidenceClass::Ambiguous,
            FailureFamily::Infrastructure,
        ),
    }
}

pub const fn classify_failure_code(code: FailureCode) -> HealthClassification {
    use FailureCode as Code;
    match code {
        Code::ProcessExitedNonZero | Code::ExcelMacroReturnedFailure => {
            classification(HealthEvidenceClass::Functional, FailureFamily::Functional)
        }
        Code::Cancelled => classification(HealthEvidenceClass::Ignore, FailureFamily::Cancellation),
        Code::AgentLeaseExpired => {
            classification(HealthEvidenceClass::ClusterSensitive, FailureFamily::Lease)
        }
        Code::ExecutorStartFailed | Code::ExecutorProtocolError | Code::ExcelUnsupported => {
            classification(
                HealthEvidenceClass::StrongNodeLocal,
                FailureFamily::ExecutorAvailability,
            )
        }
        Code::ParameterBindingFailed => classification(
            HealthEvidenceClass::StrongNodeLocal,
            FailureFamily::BindingAvailability,
        ),
        Code::ProcessIsolationFailed => classification(
            HealthEvidenceClass::StrongNodeLocal,
            FailureFamily::Isolation,
        ),
        Code::ExcelStartupFailed => classification(
            HealthEvidenceClass::StrongNodeLocal,
            FailureFamily::ExcelHost,
        ),
        Code::AssignmentRejected | Code::ProcessSpawnFailed => {
            classification(HealthEvidenceClass::Ambiguous, FailureFamily::ProcessLaunch)
        }
        Code::ExecutorProcessCrashed | Code::ProcessCrashed | Code::ExcelProcessCrashed => {
            classification(HealthEvidenceClass::Ambiguous, FailureFamily::Crash)
        }
        Code::ProcessTimedOut => {
            classification(HealthEvidenceClass::Ambiguous, FailureFamily::Timeout)
        }
        Code::ExcelWorkbookOpenFailed
        | Code::ExcelCorrelationSetupFailed
        | Code::ExcelMacroFailed => classification(
            HealthEvidenceClass::Ambiguous,
            FailureFamily::ExcelInvocation,
        ),
        Code::ExcelInvalidReturn => {
            classification(HealthEvidenceClass::Ambiguous, FailureFamily::ExcelContract)
        }
        Code::ExcelCleanupFailed => {
            classification(HealthEvidenceClass::Ambiguous, FailureFamily::Cleanup)
        }
        Code::InfrastructureError => classification(
            HealthEvidenceClass::Ambiguous,
            FailureFamily::Infrastructure,
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputHealthState {
    Clear,
    Suspected,
    Confirmed,
}

#[derive(Debug, Clone)]
pub struct InputHealthObservation {
    pub agent_id: String,
    pub occurred_at: DateTime<Utc>,
    pub classification: HealthClassification,
    pub node_was_healthy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputHealthEvaluation {
    pub state: InputHealthState,
    pub family: Option<FailureFamily>,
    pub distinct_healthy_nodes: usize,
}

pub fn evaluate_input_health(
    observations: &[InputHealthObservation],
    required_distinct_nodes: usize,
) -> InputHealthEvaluation {
    let threshold = required_distinct_nodes.max(2);
    let last_functional = observations
        .iter()
        .filter(|observation| observation.classification.class == HealthEvidenceClass::Functional)
        .map(|observation| observation.occurred_at)
        .max();
    let mut by_family: HashMap<FailureFamily, HashSet<&str>> = HashMap::new();
    for observation in observations {
        if last_functional.is_some_and(|success| observation.occurred_at <= success)
            || !observation.node_was_healthy
            || observation.classification.class != HealthEvidenceClass::Ambiguous
        {
            continue;
        }
        by_family
            .entry(observation.classification.family)
            .or_default()
            .insert(observation.agent_id.as_str());
    }
    let strongest = by_family
        .into_iter()
        .max_by_key(|(family, nodes)| (nodes.len(), *family as u8));
    match strongest {
        Some((family, nodes)) if nodes.len() >= threshold => InputHealthEvaluation {
            state: InputHealthState::Confirmed,
            family: Some(family),
            distinct_healthy_nodes: nodes.len(),
        },
        Some((family, nodes)) => InputHealthEvaluation {
            state: InputHealthState::Suspected,
            family: Some(family),
            distinct_healthy_nodes: nodes.len(),
        },
        None => InputHealthEvaluation {
            state: InputHealthState::Clear,
            family: None,
            distinct_healthy_nodes: 0,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealthDecision {
    Healthy,
    Suspect,
    AutoQuarantine,
}

/// Persisted placement state. Manual quarantine is intentionally never
/// changed by automatic scoring, while probation has stricter recovery rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealthState {
    Healthy,
    Suspect,
    AutoQuarantined,
    ManualQuarantined,
    Probation,
}

impl NodeHealthState {
    pub const fn accepts_new_work(self) -> bool {
        matches!(self, Self::Healthy | Self::Suspect | Self::Probation)
    }

    pub const fn capacity_limit(self) -> Option<u32> {
        if matches!(self, Self::Probation) {
            Some(1)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeHealthObservation {
    pub schedule_id: String,
    pub input_fingerprint: String,
    pub occurred_at: DateTime<Utc>,
    pub classification: HealthClassification,
    pub retracted: bool,
    pub cluster_suppressed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeHealthEvaluation {
    pub decision: NodeHealthDecision,
    pub distinct_failed_inputs: usize,
    pub distinct_schedules: usize,
    pub considered_observations: usize,
    pub failure_rate: f64,
}

pub fn evaluate_node_health(
    observations: &[NodeHealthObservation],
    now: DateTime<Utc>,
) -> NodeHealthEvaluation {
    let cutoff = now - Duration::minutes(15);
    // One input may contribute at most one current observation to node scoring.
    let mut latest_by_input: HashMap<&str, &NodeHealthObservation> = HashMap::new();
    for observation in observations.iter().filter(|observation| {
        observation.occurred_at >= cutoff
            && !observation.retracted
            && !observation.cluster_suppressed
            && matches!(
                observation.classification.class,
                HealthEvidenceClass::Functional
                    | HealthEvidenceClass::Ambiguous
                    | HealthEvidenceClass::StrongNodeLocal
            )
    }) {
        latest_by_input
            .entry(observation.input_fingerprint.as_str())
            .and_modify(|current| {
                if observation.occurred_at > current.occurred_at {
                    *current = observation;
                }
            })
            .or_insert(observation);
    }
    let mut considered = latest_by_input.into_values().collect::<Vec<_>>();
    considered.sort_by_key(|observation| observation.occurred_at);
    let recent = considered
        .iter()
        .rev()
        .take(10)
        .copied()
        .collect::<Vec<_>>();
    let failures = recent
        .iter()
        .copied()
        .filter(|observation| {
            matches!(
                observation.classification.class,
                HealthEvidenceClass::Ambiguous | HealthEvidenceClass::StrongNodeLocal
            )
        })
        .collect::<Vec<_>>();
    let distinct_inputs = failures
        .iter()
        .map(|observation| observation.input_fingerprint.as_str())
        .collect::<HashSet<_>>()
        .len();
    let distinct_schedules = failures
        .iter()
        .map(|observation| observation.schedule_id.as_str())
        .collect::<HashSet<_>>()
        .len();
    let failure_rate = if recent.is_empty() {
        0.0
    } else {
        failures.len() as f64 / recent.len() as f64
    };
    let strong_since = now - Duration::minutes(5);
    let strong_inputs = failures
        .iter()
        .filter(|observation| {
            observation.occurred_at >= strong_since
                && observation.classification.class == HealthEvidenceClass::StrongNodeLocal
        })
        .map(|observation| observation.input_fingerprint.as_str())
        .collect::<HashSet<_>>()
        .len();
    let strong_schedules = failures
        .iter()
        .filter(|observation| {
            observation.occurred_at >= strong_since
                && observation.classification.class == HealthEvidenceClass::StrongNodeLocal
        })
        .map(|observation| observation.schedule_id.as_str())
        .collect::<HashSet<_>>()
        .len();

    let normal_quarantine = failures.len() >= 5
        && distinct_inputs >= if distinct_schedules >= 2 { 4 } else { 6 }
        && failure_rate >= 0.60;
    let fast_quarantine = strong_inputs >= 3 && strong_schedules >= 2;
    let decision = if normal_quarantine || fast_quarantine {
        NodeHealthDecision::AutoQuarantine
    } else if failures.len() >= 3 && distinct_inputs >= 3 && failure_rate >= 0.50 {
        NodeHealthDecision::Suspect
    } else {
        NodeHealthDecision::Healthy
    };
    NodeHealthEvaluation {
        decision,
        distinct_failed_inputs: distinct_inputs,
        distinct_schedules,
        considered_observations: recent.len(),
        failure_rate,
    }
}

/// Applies automatic scoring without allowing it to release a manual
/// quarantine. A reset is represented explicitly by changing the persisted
/// state to `Probation` before this function is called.
pub fn apply_node_health_evaluation(
    current: NodeHealthState,
    evaluation: &NodeHealthEvaluation,
    observations: &[NodeHealthObservation],
) -> NodeHealthState {
    if current == NodeHealthState::ManualQuarantined {
        return current;
    }
    if current == NodeHealthState::AutoQuarantined {
        return current;
    }
    if current == NodeHealthState::Probation {
        let usable = observations
            .iter()
            .filter(|observation| !observation.retracted && !observation.cluster_suppressed)
            .collect::<Vec<_>>();
        if usable.iter().any(|observation| {
            matches!(
                observation.classification.class,
                HealthEvidenceClass::Ambiguous | HealthEvidenceClass::StrongNodeLocal
            )
        }) {
            return NodeHealthState::AutoQuarantined;
        }
        let functional = usable
            .iter()
            .filter(|observation| {
                observation.classification.class == HealthEvidenceClass::Functional
            })
            .collect::<Vec<_>>();
        let distinct_inputs = functional
            .iter()
            .map(|observation| observation.input_fingerprint.as_str())
            .collect::<HashSet<_>>()
            .len();
        return if functional.len() >= 5 && distinct_inputs >= 3 {
            NodeHealthState::Healthy
        } else {
            NodeHealthState::Probation
        };
    }
    match evaluation.decision {
        NodeHealthDecision::Healthy => NodeHealthState::Healthy,
        NodeHealthDecision::Suspect => NodeHealthState::Suspect,
        NodeHealthDecision::AutoQuarantine => NodeHealthState::AutoQuarantined,
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::{FailureOrigin, FailureStage};

    fn at(minute: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 21, 12, 0, 0).unwrap() + Duration::minutes(minute)
    }

    fn diagnosis(code: FailureCode) -> FailureDiagnostic {
        FailureDiagnostic::new(
            code,
            FailureOrigin::TaskExecutor,
            FailureStage::Execution,
            "safe summary",
            true,
        )
    }

    #[test]
    fn ordinary_task_failures_are_functional_health_observations() {
        for code in [
            FailureCode::ProcessExitedNonZero,
            FailureCode::ExcelMacroReturnedFailure,
        ] {
            let classified = classify_result(ExecutionOutcome::Failed, Some(&diagnosis(code)));
            assert_eq!(classified.class, HealthEvidenceClass::Functional);
            assert_eq!(classified.family, FailureFamily::Functional);
        }
    }

    #[test]
    fn poison_requires_the_same_family_on_two_distinct_healthy_nodes() {
        let crash = classify_failure_code(FailureCode::ExcelProcessCrashed);
        let one_node = vec![InputHealthObservation {
            agent_id: "excel-a".into(),
            occurred_at: at(0),
            classification: crash,
            node_was_healthy: true,
        }];
        assert_eq!(
            evaluate_input_health(&one_node, 2).state,
            InputHealthState::Suspected
        );
        let mut two_nodes = one_node;
        two_nodes.push(InputHealthObservation {
            agent_id: "excel-b".into(),
            occurred_at: at(1),
            classification: crash,
            node_was_healthy: true,
        });
        assert_eq!(
            evaluate_input_health(&two_nodes, 2).state,
            InputHealthState::Confirmed
        );
        two_nodes.push(InputHealthObservation {
            agent_id: "excel-c".into(),
            occurred_at: at(2),
            classification: classification(
                HealthEvidenceClass::Functional,
                FailureFamily::Functional,
            ),
            node_was_healthy: true,
        });
        assert_eq!(
            evaluate_input_health(&two_nodes, 2).state,
            InputHealthState::Clear
        );
    }

    fn node_observation(
        schedule: &str,
        input: &str,
        minute: i64,
        class: HealthEvidenceClass,
    ) -> NodeHealthObservation {
        NodeHealthObservation {
            schedule_id: schedule.into(),
            input_fingerprint: input.into(),
            occurred_at: at(minute),
            classification: classification(class, FailureFamily::Infrastructure),
            retracted: false,
            cluster_suppressed: false,
        }
    }

    #[test]
    fn one_bad_input_never_quarantines_a_node() {
        let observations = (0..20)
            .map(|minute| {
                node_observation(
                    "schedule-a",
                    "same-input",
                    minute,
                    HealthEvidenceClass::StrongNodeLocal,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            evaluate_node_health(&observations, at(20)).decision,
            NodeHealthDecision::Healthy
        );
    }

    #[test]
    fn diverse_failures_quarantine_but_retracted_and_cluster_events_do_not() {
        let mut observations = (0..5)
            .map(|index| {
                node_observation(
                    if index % 2 == 0 { "one" } else { "two" },
                    &format!("input-{index}"),
                    index,
                    HealthEvidenceClass::Ambiguous,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            evaluate_node_health(&observations, at(5)).decision,
            NodeHealthDecision::AutoQuarantine
        );
        observations[0].retracted = true;
        observations[1].cluster_suppressed = true;
        assert_ne!(
            evaluate_node_health(&observations, at(5)).decision,
            NodeHealthDecision::AutoQuarantine
        );
    }

    #[test]
    fn functional_observations_keep_failure_rate_below_quarantine_threshold() {
        let mut observations = (0..5)
            .map(|index| {
                node_observation(
                    if index % 2 == 0 { "one" } else { "two" },
                    &format!("bad-{index}"),
                    index,
                    HealthEvidenceClass::Ambiguous,
                )
            })
            .collect::<Vec<_>>();
        observations.extend((5..10).map(|index| {
            node_observation(
                "healthy",
                &format!("good-{index}"),
                index,
                HealthEvidenceClass::Functional,
            )
        }));
        let evaluation = evaluate_node_health(&observations, at(10));
        assert_eq!(evaluation.failure_rate, 0.5);
        assert_eq!(evaluation.decision, NodeHealthDecision::Suspect);
    }

    #[test]
    fn manual_quarantine_is_never_released_by_automatic_scoring() {
        let evaluation = evaluate_node_health(&[], at(0));
        assert_eq!(
            apply_node_health_evaluation(NodeHealthState::ManualQuarantined, &evaluation, &[]),
            NodeHealthState::ManualQuarantined
        );
        assert_eq!(
            apply_node_health_evaluation(NodeHealthState::AutoQuarantined, &evaluation, &[]),
            NodeHealthState::AutoQuarantined
        );
    }

    #[test]
    fn probation_needs_five_functional_observations_across_three_inputs() {
        let observations = (0..5)
            .map(|index| {
                node_observation(
                    "probation",
                    &format!("input-{}", index % 3),
                    index,
                    HealthEvidenceClass::Functional,
                )
            })
            .collect::<Vec<_>>();
        let evaluation = evaluate_node_health(&observations, at(5));
        assert_eq!(
            apply_node_health_evaluation(NodeHealthState::Probation, &evaluation, &observations),
            NodeHealthState::Healthy
        );
        assert_eq!(NodeHealthState::Probation.capacity_limit(), Some(1));
    }

    #[test]
    fn infrastructure_failure_during_probation_requarantines() {
        let observations = vec![node_observation(
            "probation",
            "input-a",
            0,
            HealthEvidenceClass::Ambiguous,
        )];
        let evaluation = evaluate_node_health(&observations, at(1));
        assert_eq!(
            apply_node_health_evaluation(NodeHealthState::Probation, &evaluation, &observations),
            NodeHealthState::AutoQuarantined
        );
    }
}
