# Testing and deterministic simulation

The scheduler combines ordinary unit and integration tests with deterministic,
seeded simulations inspired by TigerBeetle's testing approach. The simulators
generate long event sequences, inject duplicate delivery and restart boundaries,
and check safety invariants after each transition. Given the same seed, counts,
and step limit, a run follows the same path.

This is not formal verification or an operating-system fault injector. Process
isolation, signals, timeouts, and the Windows Excel/COM boundary still have their
own integration tests. The simulations concentrate on the state transitions
where at-least-once delivery commonly fails.

## What is simulated

- The core model mixes manual, webhook, and cron triggers with placement
  failures, acceptance, redelivery, heartbeats, lease expiry, late results,
  cancellation, retry exhaustion, and node availability changes.
- The cron model samples expressions, timezones, and dates across several years,
  then checks determinism, strict ordering, uniqueness, and prefix stability.
- A separate UTC-second oracle verifies that cron results are the first matching
  absolute instants, including both fallback folds in Vienna and New York,
  Lord Howe's half-hour change, Casey's three-hour change, Vienna's historical
  midnight date crossing, spring gaps, dense/sparse expressions, and bounded years.
- The store model runs real SQLite migrations and transactions while creating
  schedules/runs/attempts, replaying idempotency keys, expiring leases, completing
  work, and repeatedly closing and reopening the database. It checks SQLite
  integrity, foreign keys, uniqueness, and run/attempt state invariants.
- Focused store regressions verify cron cursor reset/preservation and prove that
  stale revisions or changed cron identities cannot insert or advance an occurrence.
  A dense every-second fallback test guards the coordinator's single batch scan.
- The coordinator protocol model uses the real coordinator store and agent
  ledger. It explores duplicate offers and acknowledgements, lost result
  acknowledgements, restarts on either side, and command, executor, Excel, and
  successful outcomes. Every accepted result and structured diagnosis must
  survive and eventually converge.

Recovery regression tests use the production ordering of durable record,
authoritative resume validation, fenced claim, and start. They also cover two
live ledger owners, cancellation before spawn, corrupt-row quarantine,
persisted disabled settings, stale/expired/cancelled/finished resume rejection,
capacity-one recovery queuing, inherited output pipes, and the Windows Excel
identity/Job Object contract.

## Running the suites

The regular workspace suite includes small default simulations:

```sh
cargo test --workspace
```

The runner gives them explicit, larger bounds and prints all replay inputs:

```sh
./scripts/test-simulations.sh
SCHEDULER_SIM_PROFILE=soak ./scripts/test-simulations.sh
```

The runner accepts zero-based deterministic sharding. Shards use disjoint model,
cron, and store seed ranges, while the protocol simulation gets one distinct seed
per shard:

```sh
SCHEDULER_SIM_PROFILE=soak \
SCHEDULER_SIM_SHARD_INDEX=3 \
SCHEDULER_SIM_SHARD_COUNT=8 \
./scripts/test-simulations.sh
```

`SCHEDULER_SIM_BASE_SEED` changes the initial seed. The individual variables
listed below override profile defaults and computed starts, which is useful both
for custom soaks and exact replay.

| Suite | Seed variables | Volume variables |
| --- | --- | --- |
| Core state machine | `SCHEDULER_SIM_SEED_START` | `SCHEDULER_SIM_SEEDS`, `SCHEDULER_SIM_STEPS` |
| Cron | `SCHEDULER_CRON_SIM_SEED_START` | `SCHEDULER_CRON_SIM_SEEDS` |
| SQLite store | `SCHEDULER_STORE_SIM_SEED_START` | `SCHEDULER_STORE_SIM_SEEDS`, `SCHEDULER_STORE_SIM_STEPS` |
| Coordinator protocol | `SCHEDULER_SIM_SEED` | `SCHEDULER_SIM_CASES` |

The simulators enforce upper bounds on configurable volume so an accidental
environment value cannot create an unbounded CI run.

## Replaying a failure

Failures include the seed and transition/case position. Use a single seed and a
step or case count that reaches the reported position. `--test-threads=1` keeps
failure output easy to follow.

For a core failure at seed `1042`, step `317`:

```sh
SCHEDULER_SIM_SEED_START=1042 \
SCHEDULER_SIM_SEEDS=1 \
SCHEDULER_SIM_STEPS=318 \
cargo test -p scheduler-core --test model_simulation \
  seeded_scheduler_state_machine_preserves_delivery_invariants -- \
  --nocapture --test-threads=1
```

For a cron failure, set `SCHEDULER_CRON_SIM_SEED_START` to the reported seed and
`SCHEDULER_CRON_SIM_SEEDS=1`. For a store failure, set
`SCHEDULER_STORE_SIM_SEED_START` and `SCHEDULER_STORE_SIM_SEEDS=1`, and make
`SCHEDULER_STORE_SIM_STEPS` at least the reported step plus one.

The protocol model prints its seed when it starts. Its generator is sequential,
so replay through the failing case index plus one:

```sh
SCHEDULER_SIM_SEED=1592614637 \
SCHEDULER_SIM_CASES=37 \
cargo test -p coordinator --test protocol_simulation -- \
  --nocapture --test-threads=1
```

Keep the recent trace from a failure in the bug report. It is part of the replay
contract and shows the last operations that led to the invariant violation.

## Continuous integration

Every push and pull request runs formatting, Clippy, and the workspace tests on
Linux, macOS, and Windows, plus the fast simulator profile on Linux. A weekly
schedule and manual workflow dispatch run the soak profile across eight
deterministic shards. The matrix does not stop the other shards after one fails,
so a single run can reveal more than one reproducible seed.

Excel automation tests that require desktop Excel remain on a separately managed,
interactive Windows runner with a licensed Excel installation. The portable CI
tests compile the Windows implementation and test the fake Excel boundary, but
cannot validate Office installation or Trust Center configuration.
