//! Deterministic, model-based simulation tests for the scheduler's safety properties.
//!
//! This deliberately uses a tiny, dependency-free PRNG so every failure is replayable with
//! the seed printed by the test. The model is not an implementation of the coordinator. It is
//! an executable specification of the invariants the coordinator, store, and agent protocol
//! must preserve while events are reordered, duplicated, rejected, or delivered late.
//!
//! Replay/scale controls:
//! - `SCHEDULER_SIM_SEED_START`, `SCHEDULER_SIM_SEEDS`, `SCHEDULER_SIM_STEPS`
//! - `SCHEDULER_CRON_SIM_SEED_START`, `SCHEDULER_CRON_SIM_SEEDS`

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use chrono::{TimeZone, Utc};
use scheduler_core::{CronSpec, ExecutionOutcome, ExecutionPolicy, RunState};

const DEFAULT_SEED_COUNT: u64 = 1_024;
const DEFAULT_STEPS_PER_SEED: usize = 400;
const DEFAULT_CRON_SEED_COUNT: u64 = 512;
const LEASE_TICKS: u64 = 10;
const TRACE_LENGTH: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AttemptState {
    TransportFailed,
    Active,
    LeaseExpired,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug)]
struct Attempt {
    id: u64,
    run_id: u64,
    accepted: bool,
    node: Option<usize>,
    lease_until: u64,
    state: AttemptState,
}

#[derive(Clone, Debug)]
struct Run {
    id: u64,
    state: RunState,
    required_labels: BTreeMap<&'static str, &'static str>,
    policy: ExecutionPolicy,
    attempts: Vec<u64>,
    accepted_attempts: u32,
    next_eligible_at: u64,
    retry_expectation: Option<(u64, u64)>,
    successful_attempt: Option<u64>,
}

#[derive(Clone, Debug)]
struct Node {
    enabled: bool,
    capacity: u32,
    labels: BTreeMap<&'static str, &'static str>,
}

#[derive(Clone, Copy, Debug)]
enum Delivery {
    Accepted,
    FailedBeforeAcceptance,
}

#[derive(Default)]
struct Coverage {
    cron_duplicate: u64,
    webhook_replay: u64,
    accepted: u64,
    transport_failure: u64,
    placement_rejection: u64,
    capacity_rejection: u64,
    redelivery: u64,
    heartbeat: u64,
    lease_expiry: u64,
    late_result: u64,
    retry: u64,
    max_attempts: u64,
    cancellation: u64,
    success: u64,
}

struct Simulator {
    now: u64,
    next_run_id: u64,
    next_attempt_id: u64,
    runs: BTreeMap<u64, Run>,
    attempts: BTreeMap<u64, Attempt>,
    cron_occurrences: BTreeMap<(usize, u64), u64>,
    webhook_idempotency: BTreeMap<(usize, u64), u64>,
    nodes: Vec<Node>,
    trace: VecDeque<String>,
    coverage: Coverage,
}

impl Simulator {
    fn new() -> Self {
        Self {
            now: 0,
            next_run_id: 1,
            next_attempt_id: 1,
            runs: BTreeMap::new(),
            attempts: BTreeMap::new(),
            cron_occurrences: BTreeMap::new(),
            webhook_idempotency: BTreeMap::new(),
            nodes: vec![
                Node {
                    enabled: true,
                    capacity: 2,
                    labels: BTreeMap::from([("os", "linux"), ("pool", "batch")]),
                },
                Node {
                    enabled: true,
                    capacity: 1,
                    labels: BTreeMap::from([("os", "windows"), ("capability", "excel")]),
                },
                Node {
                    enabled: true,
                    capacity: 3,
                    labels: BTreeMap::from([("os", "linux"), ("pool", "general")]),
                },
            ],
            trace: VecDeque::with_capacity(TRACE_LENGTH),
            coverage: Coverage::default(),
        }
    }

    fn record(&mut self, event: impl Into<String>) {
        if self.trace.len() == TRACE_LENGTH {
            self.trace.pop_front();
        }
        self.trace.push_back(event.into());
    }

    fn labels_for_schedule(schedule: usize) -> BTreeMap<&'static str, &'static str> {
        match schedule % 3 {
            0 => BTreeMap::new(),
            1 => BTreeMap::from([("os", "windows"), ("capability", "excel")]),
            _ => BTreeMap::from([("pool", "batch")]),
        }
    }

    fn policy_for_schedule(schedule: usize) -> ExecutionPolicy {
        ExecutionPolicy {
            max_attempts: 2 + (schedule % 3) as u32,
            timeout_seconds: 60,
            initial_backoff_seconds: 5,
            backoff_cap_seconds: 20,
        }
    }

    fn create_run(&mut self, schedule: usize) -> u64 {
        let id = self.next_run_id;
        self.next_run_id += 1;
        let old = self.runs.insert(
            id,
            Run {
                id,
                state: RunState::Queued,
                required_labels: Self::labels_for_schedule(schedule),
                policy: Self::policy_for_schedule(schedule),
                attempts: Vec::new(),
                accepted_attempts: 0,
                next_eligible_at: self.now,
                retry_expectation: None,
                successful_attempt: None,
            },
        );
        assert!(old.is_none());
        id
    }

    fn manual_trigger(&mut self, schedule: usize) {
        let run = self.create_run(schedule);
        self.record(format!(
            "t={} manual schedule={schedule} -> run={run}",
            self.now
        ));
    }

    fn cron_trigger(&mut self, schedule: usize, scheduled_at: u64) {
        if let Some(run) = self
            .cron_occurrences
            .get(&(schedule, scheduled_at))
            .copied()
        {
            self.coverage.cron_duplicate += 1;
            self.record(format!(
                "t={} cron duplicate schedule={schedule} at={scheduled_at} -> run={run}",
                self.now
            ));
            return;
        }
        let run = self.create_run(schedule);
        self.cron_occurrences.insert((schedule, scheduled_at), run);
        self.record(format!(
            "t={} cron schedule={schedule} at={scheduled_at} -> run={run}",
            self.now
        ));
    }

    fn webhook_trigger(&mut self, schedule: usize, key: u64) {
        if let Some(run) = self.webhook_idempotency.get(&(schedule, key)).copied() {
            self.coverage.webhook_replay += 1;
            self.record(format!(
                "t={} webhook replay schedule={schedule} key={key} -> run={run}",
                self.now
            ));
            return;
        }
        let run = self.create_run(schedule);
        self.webhook_idempotency.insert((schedule, key), run);
        self.record(format!(
            "t={} webhook schedule={schedule} key={key} -> run={run}",
            self.now
        ));
    }

    fn active_on_node(&self, node: usize) -> u32 {
        self.attempts
            .values()
            .filter(|attempt| attempt.node == Some(node) && attempt.state == AttemptState::Active)
            .count() as u32
    }

    fn dispatch(&mut self, run_id: u64, node_id: usize, delivery: Delivery) {
        let Some(run) = self.runs.get(&run_id) else {
            return;
        };
        if run.state != RunState::Queued
            || run.next_eligible_at > self.now
            || run.accepted_attempts >= run.policy.max_attempts
        {
            return;
        }

        let node = &self.nodes[node_id];
        let labels_match = run
            .required_labels
            .iter()
            .all(|(key, value)| node.labels.get(key) == Some(value));
        if !node.enabled || !labels_match {
            self.coverage.placement_rejection += 1;
            self.record(format!(
                "t={} dispatch rejected run={run_id} node={node_id} placement",
                self.now
            ));
            return;
        }
        if self.active_on_node(node_id) >= node.capacity {
            self.coverage.capacity_rejection += 1;
            self.record(format!(
                "t={} dispatch rejected run={run_id} node={node_id} capacity",
                self.now
            ));
            return;
        }

        // The coordinator persists this record before attempting transport.
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id += 1;
        let accepted = matches!(delivery, Delivery::Accepted);
        self.attempts.insert(
            attempt_id,
            Attempt {
                id: attempt_id,
                run_id,
                accepted,
                node: accepted.then_some(node_id),
                lease_until: self.now + LEASE_TICKS,
                state: if accepted {
                    AttemptState::Active
                } else {
                    AttemptState::TransportFailed
                },
            },
        );
        let run = self.runs.get_mut(&run_id).expect("run still exists");
        run.attempts.push(attempt_id);
        if accepted {
            run.accepted_attempts += 1;
            run.state = RunState::Running;
            run.retry_expectation = None;
            self.coverage.accepted += 1;
        } else {
            // A failure known to happen before acceptance does not consume the retry budget.
            self.coverage.transport_failure += 1;
        }
        self.record(format!(
            "t={} dispatch run={run_id} node={node_id} attempt={attempt_id} {delivery:?}",
            self.now
        ));
    }

    fn redeliver(&mut self, attempt_id: u64) {
        let Some(attempt) = self.attempts.get(&attempt_id) else {
            return;
        };
        if !attempt.accepted {
            return;
        }
        // The agent's ledger makes delivery of the same assignment ID idempotent. The task may
        // still be executed again after lease expiry, but that receives a distinct attempt ID.
        self.coverage.redelivery += 1;
        self.record(format!(
            "t={} redeliver attempt={attempt_id} run={} -> ledger replay",
            self.now, attempt.run_id
        ));
    }

    fn heartbeat(&mut self, attempt_id: u64) {
        let Some(attempt) = self.attempts.get_mut(&attempt_id) else {
            return;
        };
        if attempt.state != AttemptState::Active {
            return;
        }
        attempt.lease_until = self.now + LEASE_TICKS;
        let lease_until = attempt.lease_until;
        self.coverage.heartbeat += 1;
        self.record(format!(
            "t={} heartbeat attempt={attempt_id} lease_until={}",
            self.now, lease_until
        ));
    }

    fn backoff(run: &Run) -> u64 {
        let shift = run.accepted_attempts.saturating_sub(1).min(63);
        run.policy
            .initial_backoff_seconds
            .saturating_mul(1_u64 << shift)
            .min(run.policy.backoff_cap_seconds)
    }

    fn retry_or_fail(&mut self, run_id: u64) {
        let run = self.runs.get_mut(&run_id).expect("attempt references run");
        if matches!(
            run.state,
            RunState::Succeeded | RunState::Cancelled | RunState::Failed
        ) {
            return;
        }
        if run.accepted_attempts >= run.policy.max_attempts {
            run.state = RunState::Failed;
            run.retry_expectation = None;
            self.coverage.max_attempts += 1;
        } else {
            let delay = Self::backoff(run);
            run.state = RunState::Queued;
            run.next_eligible_at = self.now + delay;
            run.retry_expectation = Some((self.now, delay));
            self.coverage.retry += 1;
        }
    }

    fn result(&mut self, attempt_id: u64, outcome: ExecutionOutcome) {
        let Some(attempt) = self.attempts.get_mut(&attempt_id) else {
            return;
        };
        if !attempt.accepted
            || !matches!(
                attempt.state,
                AttemptState::Active | AttemptState::LeaseExpired
            )
        {
            return;
        }
        let was_active = attempt.state == AttemptState::Active;
        if !was_active {
            self.coverage.late_result += 1;
        }
        let run_id = attempt.run_id;
        let success = outcome == ExecutionOutcome::Succeeded;
        attempt.state = if success {
            AttemptState::Succeeded
        } else {
            AttemptState::Failed
        };

        let terminal = matches!(
            self.runs[&run_id].state,
            RunState::Succeeded | RunState::Cancelled | RunState::Failed
        );
        if terminal {
            self.record(format!(
                "t={} late terminal result attempt={attempt_id} {outcome:?} ignored",
                self.now
            ));
            return;
        }

        if success {
            let run = self.runs.get_mut(&run_id).expect("run exists");
            run.state = RunState::Succeeded;
            run.successful_attempt = Some(attempt_id);
            run.retry_expectation = None;
            for other in &run.attempts {
                if let Some(other) = self.attempts.get_mut(other)
                    && other.state == AttemptState::Active
                {
                    other.state = AttemptState::Cancelled;
                }
            }
            self.coverage.success += 1;
        } else if was_active {
            self.retry_or_fail(run_id);
        }
        // A failing result received after expiry cannot enqueue a second retry: expiry already did.
        self.record(format!(
            "t={} result run={run_id} attempt={attempt_id} {outcome:?}",
            self.now
        ));
    }

    fn cancel(&mut self, run_id: u64) {
        let Some(run) = self.runs.get_mut(&run_id) else {
            return;
        };
        if matches!(
            run.state,
            RunState::Succeeded | RunState::Failed | RunState::Cancelled
        ) {
            return;
        }
        run.state = RunState::Cancelled;
        run.retry_expectation = None;
        for attempt in &run.attempts {
            if let Some(attempt) = self.attempts.get_mut(attempt)
                && attempt.state == AttemptState::Active
            {
                attempt.state = AttemptState::Cancelled;
            }
        }
        self.coverage.cancellation += 1;
        self.record(format!("t={} cancel run={run_id}", self.now));
    }

    fn advance(&mut self, ticks: u64) {
        self.now += ticks;
        self.record(format!("advance +{ticks} -> t={}", self.now));
        let expired = self
            .attempts
            .values()
            .filter(|attempt| {
                attempt.state == AttemptState::Active && attempt.lease_until <= self.now
            })
            .map(|attempt| attempt.id)
            .collect::<Vec<_>>();
        for attempt_id in expired {
            let attempt = self
                .attempts
                .get_mut(&attempt_id)
                .expect("selected attempt exists");
            attempt.state = AttemptState::LeaseExpired;
            let run_id = attempt.run_id;
            self.coverage.lease_expiry += 1;
            self.retry_or_fail(run_id);
        }
    }

    fn toggle_node(&mut self, node: usize) {
        self.nodes[node].enabled = !self.nodes[node].enabled;
        self.record(format!(
            "t={} node={node} enabled={}",
            self.now, self.nodes[node].enabled
        ));
    }

    fn check_invariants(&self) -> Result<(), String> {
        let unique_attempts = self.attempts.keys().copied().collect::<BTreeSet<_>>();
        if unique_attempts.len() != self.attempts.len() {
            return Err("attempt IDs are not globally unique".into());
        }

        for (identity, run_id) in &self.cron_occurrences {
            if !self.runs.contains_key(run_id) {
                return Err(format!(
                    "cron identity {identity:?} points to missing run {run_id}"
                ));
            }
        }
        for (identity, run_id) in &self.webhook_idempotency {
            if !self.runs.contains_key(run_id) {
                return Err(format!(
                    "webhook identity {identity:?} points to missing run {run_id}"
                ));
            }
        }

        for (node_id, node) in self.nodes.iter().enumerate() {
            let active = self.active_on_node(node_id);
            if active > node.capacity {
                return Err(format!(
                    "node {node_id} exceeds capacity: {active} > {}",
                    node.capacity
                ));
            }
        }

        for run in self.runs.values() {
            if run.id == 0 {
                return Err("run ID zero is reserved".into());
            }
            let mut accepted = 0_u32;
            let mut active = 0_u32;
            let mut attempt_ids = BTreeSet::new();
            for attempt_id in &run.attempts {
                if !attempt_ids.insert(*attempt_id) {
                    return Err(format!(
                        "run {} contains attempt {attempt_id} twice",
                        run.id
                    ));
                }
                let Some(attempt) = self.attempts.get(attempt_id) else {
                    return Err(format!(
                        "run {} points to missing attempt {attempt_id}",
                        run.id
                    ));
                };
                if attempt.run_id != run.id {
                    return Err(format!(
                        "attempt {attempt_id} changed TASK_RUN_ID {} -> {}",
                        attempt.run_id, run.id
                    ));
                }
                accepted += u32::from(attempt.accepted);
                active += u32::from(attempt.state == AttemptState::Active);
                if !attempt.accepted && attempt.state != AttemptState::TransportFailed {
                    return Err(format!(
                        "unaccepted attempt {attempt_id} escaped transport-failed state"
                    ));
                }
            }
            if accepted != run.accepted_attempts {
                return Err(format!(
                    "run {} accepted count {} != model count {accepted}",
                    run.id, run.accepted_attempts
                ));
            }
            if run.accepted_attempts > run.policy.max_attempts {
                return Err(format!(
                    "run {} exceeded retry budget {} > {}",
                    run.id, run.accepted_attempts, run.policy.max_attempts
                ));
            }
            match run.state {
                RunState::Queued if active != 0 => {
                    return Err(format!(
                        "queued run {} still has {active} active attempts",
                        run.id
                    ));
                }
                RunState::Running if active == 0 => {
                    return Err(format!("running run {} has no active delivery", run.id));
                }
                RunState::Succeeded => {
                    let Some(attempt_id) = run.successful_attempt else {
                        return Err(format!(
                            "succeeded run {} has no successful attempt",
                            run.id
                        ));
                    };
                    if self.attempts[&attempt_id].state != AttemptState::Succeeded {
                        return Err(format!(
                            "run {} success points to non-successful attempt {attempt_id}",
                            run.id
                        ));
                    }
                    if active != 0 {
                        return Err(format!("succeeded run {} still has active work", run.id));
                    }
                }
                RunState::Failed => {
                    if run.accepted_attempts < run.policy.max_attempts || active != 0 {
                        return Err(format!(
                            "failed run {} did not exhaust attempts or has active work",
                            run.id
                        ));
                    }
                }
                RunState::Cancelled if active != 0 => {
                    return Err(format!("cancelled run {} still has active work", run.id));
                }
                _ => {}
            }
            if let Some((failure_at, delay)) = run.retry_expectation {
                if run.state != RunState::Queued || run.next_eligible_at != failure_at + delay {
                    return Err(format!("run {} violated retry backoff", run.id));
                }
                if delay > run.policy.backoff_cap_seconds
                    || delay < run.policy.initial_backoff_seconds
                {
                    return Err(format!(
                        "run {} retry delay {delay} is out of bounds",
                        run.id
                    ));
                }
            }

            // At-least-once safety: a created, non-cancelled run is always either durably queued,
            // backed by an accepted live attempt, or represented by a durable terminal result.
            if !matches!(run.state, RunState::Queued | RunState::Running)
                && !matches!(
                    run.state,
                    RunState::Succeeded | RunState::Failed | RunState::Cancelled
                )
            {
                return Err(format!("run {} was lost from durable state", run.id));
            }
        }
        Ok(())
    }

    fn recent_trace(&self) -> String {
        self.trace.iter().cloned().collect::<Vec<_>>().join("\n")
    }
}

#[derive(Clone, Copy)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // SplitMix's nonzero increment also keeps seed zero useful.
        Self(seed.wrapping_add(0x9e37_79b9_7f4a_7c15))
    }

    fn next(&mut self) -> u64 {
        // SplitMix64: small, fast, deterministic on every Rust target.
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn below(&mut self, upper: usize) -> usize {
        (self.next() % upper as u64) as usize
    }
}

fn nth_key<T>(map: &BTreeMap<u64, T>, index: usize) -> Option<u64> {
    if map.is_empty() {
        None
    } else {
        map.keys().nth(index % map.len()).copied()
    }
}

fn env_u64(name: &str, default: u64, maximum: u64) -> u64 {
    let value = std::env::var(name)
        .ok()
        .map(|raw| {
            raw.parse::<u64>()
                .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
        })
        .unwrap_or(default);
    assert!(value > 0, "{name} must be greater than zero");
    assert!(
        value <= maximum,
        "{name}={value} exceeds the safety limit {maximum}"
    );
    value
}

#[test]
fn seeded_scheduler_state_machine_preserves_delivery_invariants() {
    let seed_start = std::env::var("SCHEDULER_SIM_SEED_START")
        .ok()
        .map(|raw| {
            raw.parse::<u64>().unwrap_or_else(|error| {
                panic!("SCHEDULER_SIM_SEED_START must be an unsigned integer: {error}")
            })
        })
        .unwrap_or(0);
    let seed_count = env_u64("SCHEDULER_SIM_SEEDS", DEFAULT_SEED_COUNT, 1_000_000);
    let steps_per_seed = env_u64(
        "SCHEDULER_SIM_STEPS",
        DEFAULT_STEPS_PER_SEED as u64,
        100_000,
    ) as usize;
    let seed_end = seed_start
        .checked_add(seed_count)
        .expect("configured simulation seed range overflows u64");
    let mut total = Coverage::default();
    for seed in seed_start..seed_end {
        let mut rng = Rng::new(seed);
        let mut sim = Simulator::new();
        for step in 0..steps_per_seed {
            match rng.below(100) {
                0..=9 => sim.manual_trigger(rng.below(3)),
                10..=21 => sim.webhook_trigger(rng.below(3), rng.next() % 8),
                22..=32 => {
                    let bucket = (sim.now / 5).saturating_sub((rng.next() % 4) * 5);
                    sim.cron_trigger(rng.below(3), bucket);
                }
                33..=53 => {
                    if let Some(run) = nth_key(&sim.runs, rng.below(sim.runs.len().max(1))) {
                        let delivery = if rng.next().is_multiple_of(4) {
                            Delivery::FailedBeforeAcceptance
                        } else {
                            Delivery::Accepted
                        };
                        sim.dispatch(run, rng.below(sim.nodes.len()), delivery);
                    }
                }
                54..=63 => sim.advance(1 + rng.next() % 15),
                64..=76 => {
                    if let Some(attempt) =
                        nth_key(&sim.attempts, rng.below(sim.attempts.len().max(1)))
                    {
                        let outcome = match rng.below(4) {
                            0 => ExecutionOutcome::Succeeded,
                            1 => ExecutionOutcome::Failed,
                            2 => ExecutionOutcome::InfrastructureError,
                            _ => ExecutionOutcome::TimedOut,
                        };
                        sim.result(attempt, outcome);
                    }
                }
                77..=83 => {
                    if let Some(run) = nth_key(&sim.runs, rng.below(sim.runs.len().max(1))) {
                        sim.cancel(run);
                    }
                }
                84..=88 => {
                    if let Some(attempt) =
                        nth_key(&sim.attempts, rng.below(sim.attempts.len().max(1)))
                    {
                        sim.redeliver(attempt);
                    }
                }
                89..=93 => {
                    if let Some(attempt) =
                        nth_key(&sim.attempts, rng.below(sim.attempts.len().max(1)))
                    {
                        sim.heartbeat(attempt);
                    }
                }
                94..=97 => sim.toggle_node(rng.below(sim.nodes.len())),
                _ => sim.advance(20 + rng.next() % 20),
            }
            if let Err(problem) = sim.check_invariants() {
                panic!(
                    "scheduler simulation failed: seed={seed}, step={step}\n{problem}\nrecent trace:\n{}",
                    sim.recent_trace()
                );
            }
        }
        total.cron_duplicate += sim.coverage.cron_duplicate;
        total.webhook_replay += sim.coverage.webhook_replay;
        total.accepted += sim.coverage.accepted;
        total.transport_failure += sim.coverage.transport_failure;
        total.placement_rejection += sim.coverage.placement_rejection;
        total.capacity_rejection += sim.coverage.capacity_rejection;
        total.redelivery += sim.coverage.redelivery;
        total.heartbeat += sim.coverage.heartbeat;
        total.lease_expiry += sim.coverage.lease_expiry;
        total.late_result += sim.coverage.late_result;
        total.retry += sim.coverage.retry;
        total.max_attempts += sim.coverage.max_attempts;
        total.cancellation += sim.coverage.cancellation;
        total.success += sim.coverage.success;
    }

    // Guard against a generator change silently removing an important fault scenario.
    let covered = [
        ("duplicate cron", total.cron_duplicate),
        ("idempotent webhook replay", total.webhook_replay),
        ("accepted delivery", total.accepted),
        ("pre-accept transport failure", total.transport_failure),
        ("placement rejection", total.placement_rejection),
        ("capacity rejection", total.capacity_rejection),
        ("assignment redelivery", total.redelivery),
        ("lease heartbeat", total.heartbeat),
        ("lease expiry", total.lease_expiry),
        ("late result", total.late_result),
        ("retry", total.retry),
        ("retry exhaustion", total.max_attempts),
        ("cancellation", total.cancellation),
        ("success", total.success),
    ];
    if seed_count >= DEFAULT_SEED_COUNT && steps_per_seed >= DEFAULT_STEPS_PER_SEED {
        for (scenario, observations) in covered {
            assert!(
                observations > 0,
                "simulation never exercised {scenario}; seed range {seed_start}..{seed_end}"
            );
        }
    }
}

#[test]
fn seeded_cron_queries_are_unique_monotonic_and_replayable() {
    let expressions = [
        "0/7 * * * * *",
        "0 */5 * * * *",
        "0 30 2 * * *",
        "0 0 9 * * MON-FRI",
    ];
    let timezones = ["UTC", "Europe/Vienna", "America/New_York", "Asia/Tokyo"];

    let seed_start = std::env::var("SCHEDULER_CRON_SIM_SEED_START")
        .ok()
        .map(|raw| {
            raw.parse::<u64>().unwrap_or_else(|error| {
                panic!("SCHEDULER_CRON_SIM_SEED_START must be an unsigned integer: {error}")
            })
        })
        .unwrap_or(0);
    let seed_count = env_u64(
        "SCHEDULER_CRON_SIM_SEEDS",
        DEFAULT_CRON_SEED_COUNT,
        1_000_000,
    );
    let seed_end = seed_start
        .checked_add(seed_count)
        .expect("configured cron simulation seed range overflows u64");

    for seed in seed_start..seed_end {
        let mut rng = Rng::new(seed ^ 0xc0de_cafe);
        let spec = CronSpec {
            expression: expressions[rng.below(expressions.len())].into(),
            timezone: timezones[rng.below(timezones.len())].into(),
        };
        let seconds = 1_735_689_600 + (rng.next() % (3 * 365 * 24 * 60 * 60));
        let after = Utc
            .timestamp_opt(seconds as i64, 0)
            .single()
            .expect("generated timestamp is valid");
        let count = 1 + rng.below(64);
        let first = scheduler_core::schedule::next_occurrences(&spec, after, count).unwrap_or_else(
            |error| panic!("cron query failed: seed={seed}, spec={spec:?}: {error}"),
        );
        let replay = scheduler_core::schedule::next_occurrences(&spec, after, count)
            .unwrap_or_else(|error| panic!("cron replay failed: seed={seed}: {error}"));
        let extended = scheduler_core::schedule::next_occurrences(&spec, after, count + 5)
            .unwrap_or_else(|error| panic!("cron extension failed: seed={seed}: {error}"));

        assert_eq!(
            first.len(),
            count,
            "wrong count: seed={seed}, spec={spec:?}"
        );
        assert_eq!(
            first, replay,
            "cron replay diverged: seed={seed}, spec={spec:?}"
        );
        assert_eq!(
            first.as_slice(),
            &extended[..count],
            "cron prefix changed: seed={seed}, spec={spec:?}"
        );
        assert!(
            first.windows(2).all(|pair| pair[0] < pair[1]),
            "cron occurrences not strictly monotonic: seed={seed}, spec={spec:?}, occurrences={first:?}"
        );
        let unique = first.iter().collect::<BTreeSet<_>>();
        assert_eq!(
            unique.len(),
            first.len(),
            "cron emitted a duplicate: seed={seed}, spec={spec:?}, occurrences={first:?}"
        );

        // Model the store's unique `(schedule_id, scheduled_at_utc)` key and repeated scans.
        let mut persisted = BTreeSet::new();
        for occurrence in first.iter().chain(replay.iter()) {
            persisted.insert((seed % 7, *occurrence));
        }
        assert_eq!(
            persisted.len(),
            count,
            "repeated cron scan created duplicate logical occurrences: seed={seed}"
        );
    }
}
