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
- The store model also replays parameter-collection page commits across crash/reopen
  boundaries and checks cursor generation, page/provider-key uniqueness, batch
  finalization, child-run uniqueness, cancellation rollups, and per-batch
  concurrency/fair interleaving.
- A dedicated health model generates diverse functional, ambiguous, strong-local,
  and cluster-sensitive evidence. It proves that one input and ordinary business
  failures cannot quarantine a node, poison needs distinct healthy nodes in one
  failure family, cluster/retracted evidence is excluded, and threshold decisions
  remain deterministic.
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

Focused collection tests cover JSON/NDJSON parsing, bounded paging, snapshot drift,
bad/cyclic cursors, connector request/response shape, valid-item continuation,
invalid item quarantine, duplicate rollback, restart replay, idempotent cron batch
creation, and batch cancellation. Binding tests cover environment allowlists,
logical secret names, traversal and symlink escape, regular-file/size/type parsing,
schema validation, placement capability matching, redacted failures, sensitive
command-field restrictions, and pre-accept rejection without retry-budget or stream
loss. Management tests cover stable cursor bounds, wildcard-safe ID search,
dashboard lock/revision fencing, request-ID panic containment, proxy panic
containment, and bounded proxy bodies. Published-schema tests also enforce strict
unknown-property rejection throughout typed schedule and blueprint objects.

Security/management regressions also bind the peer leaf-certificate SHA-256 to the
claimed agent ID, reject unregistered/mismatched/missing certificates and duplicate
configuration entries, verify contended settings/dashboard pages are read-only and
do not expose lock tokens, and prove force-releasing an existing lock appends the
safe audit event. Queue-depth store tests separately prove exact ready/delayed counts.

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
| Health correlation | `SCHEDULER_HEALTH_SIM_SEED_START` | `SCHEDULER_HEALTH_SIM_SEEDS`, `SCHEDULER_HEALTH_SIM_STEPS` |

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

For a health-correlation failure, replay exactly one seed and enough steps to
include the reported position:

```sh
SCHEDULER_HEALTH_SIM_SEED_START=1000123 \
SCHEDULER_HEALTH_SIM_SEEDS=1 \
SCHEDULER_HEALTH_SIM_STEPS=241 \
cargo test -p scheduler-core --test health_simulation -- --nocapture
```

The health defaults are 512 seeds × 240 steps in `fast` and 4,096 × 2,000 in
`soak`; the test caps them at 16,384 seeds and 10,000 steps.

Keep the recent trace from a failure in the bug report. It is part of the replay
contract and shows the last operations that led to the invariant violation.

There is not yet a standalone 10,000-item wall-clock load-test binary or a real
OTLP HTTP/protobuf/mTLS integration suite. Collection limits and crash/replay
invariants are tested in the current unit/store simulations; licensed Excel remains
the only test of real Office COM behavior.

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

### Licensed Excel workbook fixture

Set `SCHEDULER_TEST_XLSM` to an absolute path to the trusted or signed `.xlsm`
fixture on an interactive Windows runner. The workbook must contain a public
standard module named `TestModule`. In addition to the return, VBA-error, crash,
and hang macros used by the other ignored tests, it must expose
`TestModule.ValidateProcessIdArguments` with this signature:

```vb
Public Function ValidateProcessIdArguments( _
    ByVal id As Long, ByVal workbookName As String, _
    ByVal recipients As String, ByVal selectionVariant As String, _
    ByVal responsible As String, ByVal subject As String, _
    ByVal body As String, ByVal pdf As Boolean, _
    ByVal mailfilter As Boolean, Optional ByVal query1 As String = "", _
    Optional ByVal query2 As String = "", Optional ByVal query3 As String = "", _
    Optional ByVal query4 As String = "", Optional ByVal query5 As String = "", _
    Optional ByVal info As Boolean = False, Optional ByVal bwpUser As String = "", _
    Optional ByVal bwpPassword As String = "") As Integer
```

The macro must compare all 17 typed VBA parameter values against this contract,
return `0` only when every check passes, and return `1` for any mismatch. This
proves positional ordering and compatibility with the declared VBA parameter
types; a typed VBA parameter does not expose the original incoming COM Variant
subtype after VBA has performed argument coercion.

| Position | VBA parameter | Expected value |
| ---: | --- | --- |
| 1 | `id As Long` | `2147483647` |
| 2 | `workbookName As String` | `Monthly Processing.xlsm` |
| 3 | `recipients As String` | `operations@example.com;finance@example.com` |
| 4 | `selectionVariant As String` | `CURRENT_AND_ARCHIVED` |
| 5 | `responsible As String` | `Ada Lovelace` |
| 6 | `subject As String` | `Processing result – July` |
| 7 | `body As String` | Two lines: `Line 1`, then `Line 2 with 'quotes' and {{literal braces}}` |
| 8 | `pdf As Boolean` | `True` |
| 9 | `mailfilter As Boolean` | `False` |
| 10 | `query1 As String` | `SELECT * FROM CurrentData WHERE Status = 'Ready'` |
| 11–14 | `query2` through `query5 As String` | Empty string |
| 15 | `info As Boolean` | `False` |
| 16 | `bwpUser As String` | `example-user` |
| 17 | `bwpPassword As String` | `example-password` |

The fixture must also read `CStr(Evaluate("TASK_RUN_ID"))` and
`CStr(Evaluate("TASK_ATTEMPT_ID"))`, then require non-empty, distinct UUID
strings. This verifies that Excel macros receive stable run-level idempotency
and attempt-level diagnostic identifiers through temporary workbook-scoped
defined names. Portable source-contract tests separately verify that the
secret-bearing scheduler bootstrap variables are scrubbed before Excel starts.

The ignored test
`excel_process_id_signature_preserves_all_seventeen_values_and_order` invokes
this macro and requires scheduler success with exit code `0`. Returning `1`
proves that ordering, scalar conversion, or value preservation failed and is
reported by the scheduler as a macro task failure.
