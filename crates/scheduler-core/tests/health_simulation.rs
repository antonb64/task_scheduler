//! Replayable model simulation for poison correlation and node quarantine.
//!
//! Controls: `SCHEDULER_HEALTH_SIM_SEED_START`, `SCHEDULER_HEALTH_SIM_SEEDS`,
//! and `SCHEDULER_HEALTH_SIM_STEPS`.

use std::collections::{HashMap, HashSet};

use chrono::{Duration, TimeZone, Utc};
use scheduler_core::health::{
    FailureFamily, HealthClassification, HealthEvidenceClass, InputHealthObservation,
    InputHealthState, NodeHealthDecision, NodeHealthObservation, evaluate_input_health,
    evaluate_node_health,
};

const DEFAULT_SEEDS: u64 = 512;
const DEFAULT_STEPS: usize = 240;

#[derive(Clone, Copy)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn index(&mut self, bound: usize) -> usize {
        (self.next() as usize) % bound
    }
}

fn configured(name: &str, default: u64, maximum: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .clamp(1, maximum)
}

fn classification(class: HealthEvidenceClass, family: FailureFamily) -> HealthClassification {
    HealthClassification {
        version: 1,
        class,
        family,
    }
}

fn base_time() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 21, 12, 0, 0)
        .single()
        .expect("fixed time")
}

#[test]
fn seeded_health_correlation_preserves_safety_invariants() {
    let seed_start = configured("SCHEDULER_HEALTH_SIM_SEED_START", 0, u32::MAX as u64);
    let seeds = configured("SCHEDULER_HEALTH_SIM_SEEDS", DEFAULT_SEEDS, 16_384);
    let steps = configured("SCHEDULER_HEALTH_SIM_STEPS", DEFAULT_STEPS as u64, 10_000) as usize;

    for seed in seed_start..seed_start + seeds {
        let mut rng = Rng(seed ^ 0x9e37_79b9_7f4a_7c15);
        let now = base_time();

        // A single pathological input may fail indefinitely but must never
        // quarantine a node, independent of family or event count.
        let same_input = (0..steps)
            .map(|step| NodeHealthObservation {
                schedule_id: format!("schedule-{}", step % 3),
                input_fingerprint: "one-input".into(),
                occurred_at: now - Duration::seconds((steps - step) as i64),
                classification: classification(
                    HealthEvidenceClass::Ambiguous,
                    FailureFamily::Crash,
                ),
                retracted: false,
                cluster_suppressed: false,
            })
            .collect::<Vec<_>>();
        assert_ne!(
            evaluate_node_health(&same_input, now).decision,
            NodeHealthDecision::AutoQuarantine,
            "seed={seed}: one input quarantined a node"
        );

        // Ordinary business failures and fleet-sensitive lease events never
        // contribute to node quarantine, even across diverse work.
        let non_attributable = (0..steps)
            .map(|step| NodeHealthObservation {
                schedule_id: format!("schedule-{}", step % 5),
                input_fingerprint: format!("input-{step}"),
                occurred_at: now - Duration::seconds((steps - step) as i64),
                classification: if step % 2 == 0 {
                    classification(HealthEvidenceClass::Functional, FailureFamily::Functional)
                } else {
                    classification(HealthEvidenceClass::ClusterSensitive, FailureFamily::Lease)
                },
                retracted: false,
                cluster_suppressed: step % 2 == 1,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            evaluate_node_health(&non_attributable, now).decision,
            NodeHealthDecision::Healthy,
            "seed={seed}: functional/cluster evidence harmed a node"
        );

        let families = [
            FailureFamily::Crash,
            FailureFamily::Timeout,
            FailureFamily::ExcelInvocation,
            FailureFamily::ExcelContract,
        ];
        let mut input_observations = Vec::new();
        for step in 0..steps {
            let class = match rng.index(8) {
                0 => HealthEvidenceClass::Functional,
                1 => HealthEvidenceClass::StrongNodeLocal,
                2 => HealthEvidenceClass::ClusterSensitive,
                _ => HealthEvidenceClass::Ambiguous,
            };
            input_observations.push(InputHealthObservation {
                agent_id: format!("node-{}", rng.index(8)),
                occurred_at: now + Duration::milliseconds(step as i64),
                classification: classification(class, families[rng.index(families.len())]),
                node_was_healthy: rng.index(5) != 0,
            });
        }
        let evaluation = evaluate_input_health(&input_observations, 2);
        let last_success = input_observations
            .iter()
            .filter(|event| event.classification.class == HealthEvidenceClass::Functional)
            .map(|event| event.occurred_at)
            .max();
        let mut oracle: HashMap<FailureFamily, HashSet<&str>> = HashMap::new();
        for event in &input_observations {
            if last_success.is_some_and(|success| event.occurred_at <= success)
                || !event.node_was_healthy
                || event.classification.class != HealthEvidenceClass::Ambiguous
            {
                continue;
            }
            oracle
                .entry(event.classification.family)
                .or_default()
                .insert(&event.agent_id);
        }
        let confirmed = oracle.values().any(|nodes| nodes.len() >= 2);
        assert_eq!(
            evaluation.state == InputHealthState::Confirmed,
            confirmed,
            "seed={seed}: poison correlation diverged"
        );
        if evaluation.state == InputHealthState::Confirmed {
            assert!(evaluation.distinct_healthy_nodes >= 2);
        }
    }
}
