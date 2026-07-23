# Authoritative observability contract

The scheduler is observable without querying SQLite or the management UI. Three
signals have different jobs:

- `scheduler.state` OTLP logs are the durable, ordered lifecycle history.
- Reconciled OTLP gauges are the current authoritative projection and the source
  of daily verdicts.
- Traces explain the causal path and latency across HTTP, cron, collection,
  dispatch, agent execution, result persistence, and acknowledgement.

Diagnostic logs and spans remain bounded and best effort. Their loss cannot make
a daily verdict green: completeness comes from durable state events, current
state snapshots, and explicit freshness/coverage signals.

## Resource and correlation contract

Every process exports `service.name`, `service.version`, and
`service.instance.id`. Agent resources also export `scheduler.agent.id`. HTTP
and the coordinator-agent protocol use W3C `traceparent` and `tracestate`.
Trigger context is persisted, so a restart does not break later dispatch
correlation. Each dispatch attempt starts a trace linked to its trigger. A
collection batch has its own linked trace, whose persisted context is used as
the link for every child-run attempt; the agent continues the attempt context.

Lifecycle logs use the `scheduler.state` target and carry:

| Attribute | Meaning |
| --- | --- |
| `event_id` / `event.id` | Globally stable deduplication ID. |
| `event_sequence` / `event.sequence` | Durable coordinator audit sequence or agent-ledger sequence. |
| `event_name` / `event.name` | Stable lifecycle event name. |
| `event_occurred_at` / `event.occurred_at` | Time of the state mutation, not replay time. |
| `entity.type`, `entity.id` | Entity changed by the transaction. |
| `state.from`, `state.to` | Previous and current state when applicable. |
| `schedule.id`, `trigger.id`, `batch.id`, `item.id`, `run.id`, `attempt.id`, `agent.id` | Applicable correlation IDs. |
| `operations.timezone`, `operations.day`, `completion.deadline_at` | Immutable trigger interpretation. |
| OTLP trace/span IDs | Link to the originating trace even when the event was replayed later. |

Some collectors flatten the safe JSON in `event_attributes_json`; others retain
it as a JSON string. Configure ingestion to parse that field, while retaining the
top-level event ID and sequence.

Authoritative events never include parameters, provider keys, collection
cursors, secret names or values, output content, or free-form errors and
diagnostics. Only allowlisted identifiers, states, bounded codes, booleans, and
counts are copied from the internal audit record. Metric attributes are bounded:
entity/state/window/kind, schedule ID, and agent ID. Batch, item, run, and attempt
IDs occur only in logs and traces.

## Durable delivery and coverage

Coordinator state mutation, audit insertion, and outbox insertion share one
SQLite transaction. Collection-item and agent-ledger transitions use local
SQLite triggers so bulk ingestion and crash recovery have the same guarantee.
The log exporter acknowledges an event only after a successful OTLP export
batch. Until then the outbox retries with the same `event.id`; collector storage
must deduplicate on that ID.

Scheduling and execution continue while the collector is unavailable. Pending
events replay after collector or process recovery. Delivered and undelivered
outbox rows are retained for the configured coordinator
`audit_retention_days`; the agent uses 90 days. If an undelivered event expires,
the process durably increments the expired count and sets a coverage gap.
Coverage cannot be inferred back into existence, so affected verdicts remain
`unknown`.

Pre-migration triggers are stamped with a conservative UTC/24-hour
interpretation but have incomplete coverage. A day containing them is
`unknown`.

## Operations day and completion policy

Each schedule may set:

```yaml
observability:
  completion_deadline_seconds: 86400
```

Omission inherits `GlobalSettings.default_completion_deadline_seconds`, whose
default is 86,400 seconds. The resolved value is stored with the schedule and
the effective timezone, local operations day, and absolute deadline are copied
onto every trigger.

Cron schedules use their cron timezone. Schedules without cron use the cluster
default timezone. Work belongs to the local calendar day containing
`scheduled_at`, including 23-hour and 25-hour DST days. Schedule edits never
reinterpret existing trigger records.

Cron work becomes expected when an occurrence is due. Manual and webhook work
becomes expected when its request reaches the scheduler. A request never
delivered by an upstream system is outside this boundary; monitor the upstream
with its own delivery SLI.

## Daily verdict

Ordinary work succeeds only when its run is `succeeded`. A collection trigger
succeeds only when the batch is `succeeded`, ingestion/finalization completed,
and every item succeeded. Cluster verdict is the worst schedule verdict, with
the order `unknown`, `red`, `degraded`, `pending`, `green`, `idle`.

| Verdict | What it proves |
| --- | --- |
| `green` | Work existed, every trigger and collection item succeeded before its captured deadline, and there was no retry or recovered anomaly. |
| `degraded` | All work eventually succeeded before deadline, but retry, rejected offer, lease recovery/expiry, late result, or another recovered anomaly occurred. |
| `red` | A due cron occurrence was not materialized; work is overdue, failed, or cancelled; an item is invalid/suspected-poison/poisoned/held; or collection finalization is overdue/incomplete. |
| `pending` | Materialized expected work is nonterminal and still within its deadline. Recovered anomalies do not turn incomplete work into `degraded`. |
| `idle` | No work was expected or observed for the operations day. |
| `unknown` | Lifecycle coverage is incomplete, cron backlog evaluation was truncated, or the authoritative snapshot is stale/missing. |

The evaluator is shared by the telemetry projection and built-in dashboard. A
dashboard must still apply the freshness gate below, because an old green sample
does not describe the present.

## Reconciled metrics

The coordinator refreshes authoritative gauges every 30 seconds. Gauges emit
explicit zeroes for known entity states, so restarts and counter loss converge
to SQLite state.

| Metric | Attributes / value |
| --- | --- |
| `scheduler.state.entities` | Current count by `schedule.id`, `entity`, `state`. |
| `scheduler.state.overdue` | Overdue count by `schedule.id`, `entity`, `window`. |
| `scheduler.schedule.daily.triggers` | Current/previous day counts by `schedule.id`, `window`, `status=expected|materialized|succeeded|failed|cancelled|pending|overdue|missing`. |
| `scheduler.schedule.daily.items` | Current/previous collection-item counts by `schedule.id`, `window`, and every item `state`. |
| `scheduler.schedule.daily.retries` | Retry count by schedule/window. |
| `scheduler.schedule.daily.attempt_anomalies` | Rejected/failed/expired/late attempt count by schedule/window. |
| `scheduler.schedule.daily.verdict` | One-hot value by schedule/window/verdict. Exactly one is 1 in a fresh complete snapshot. |
| `scheduler.schedule.daily.operations_day` | Local date as integer `YYYYMMDD`, with schedule/window and `operations.timezone`. |
| `scheduler.schedule.completion_deadline_seconds` | Effective deadline duration by schedule/window. |
| `scheduler.schedule.last_success_age_seconds` | Age of the last successful trigger by schedule. |
| `scheduler.cron.backlog` / `scheduler.cron.lag_ms` | Missing due materializations and cron loop lag. |
| `scheduler.cluster.agents` | One current row per `agent.id` with connection, health, and settings state. |
| `scheduler.cluster.agent.capacity` | Maximum/running slots by `agent.id`, `kind`. |
| `scheduler.agent.assignment_state` | Agent-ledger assignments by state. |
| `scheduler.agent.pending_results` | Results durably waiting for coordinator acknowledgement. |
| `scheduler.observability.snapshot.generated_at_unix_seconds` | Authoritative projection generation time. |
| `scheduler.observability.outbox.depth` | Undelivered state-event count. |
| `scheduler.observability.outbox.oldest_age_seconds` | Age of oldest undelivered state event. |
| `scheduler.observability.outbox.expired_events` | Cumulative retention expirations. |
| `scheduler.observability.coverage_gap` | 1 after any durable coverage loss. |
| `scheduler.observability.telemetry.export_failures` | Failed export items by signal, visible after recovery. |
| `scheduler.observability.telemetry.dropped_items` | Diagnostic telemetry items lost to export failure. |

Existing dispatch, queue, collection, execution, settings, node-health, and
management metrics retain their names for compatibility.

## Backend-neutral dashboard specification

Use a 90-second default freshness allowance: three times the 30-second
projection interval. Deployments may raise it, but must not remove it.

### Overall verdict

1. Read the newest
   `scheduler.observability.snapshot.generated_at_unix_seconds`.
2. Render `unknown` if it is missing, older than the freshness allowance, or
   `scheduler.observability.coverage_gap` is 1.
3. Otherwise select the one-hot current verdict for every active schedule and
   render the worst value. Never treat a missing schedule series as green.

### Per-schedule table

Show one current-window row per `schedule.id`:

- operations day and `operations.timezone`;
- effective completion deadline;
- expected, materialized, succeeded, pending, overdue, failed, and cancelled
  triggers;
- retries and attempt anomalies;
- invalid, suspected-poison, poisoned, and held item counts;
- last-success age;
- freshness-gated verdict.

Clicking a row filters `scheduler.state` logs by `schedule.id`. A run ID in a
log opens the run lifecycle; the log's OTLP trace ID opens the causal trace.
Collection logs additionally expose batch/item IDs.

### Required panels

- Ready/delayed queue, current entity states, and overdue work.
- Cron backlog and lag.
- Collection ingestion, page commits, item states, and finalization.
- Retry, rejected-offer, lease-expiry/recovery, and late-result anomalies.
- Agent connectivity, capacity, health/quarantine, and desired/applied settings
  divergence.
- Agent pending results and result persistence retries.
- Per-signal export freshness, durable outbox depth/age, expired events, and
  coverage gap.

No Grafana-specific dashboard is shipped. The metric/log/trace contract above is
the portable dashboard API.

## Alert recipes

Alerts must use the same freshness gate as the dashboard:

| Condition | Suggested behavior |
| --- | --- |
| Previous-day verdict `red` or `unknown` | Page the schedule owner; cluster page if multiple schedules or coverage is unknown. |
| Current verdict `pending` near/prolonged beyond deadline | Warn before deadline; page when it becomes overdue/red. |
| `scheduler.state.overdue > 0` | Page with schedule/entity labels. |
| `scheduler.cron.backlog > 0` or sustained cron lag | Page; a missing due materialization is already red. |
| Disconnected or quarantined agent, or no free capacity | Alert by required pool/site policy. |
| Settings state rejected or pending for a sustained interval | Alert with agent ID and desired/applied revisions from lifecycle logs. |
| Agent pending results or result-persist retries growing | Page before the agent ledger fills. |
| Snapshot absent/stale, coverage gap 1, or expired events increasing | Page as observability failure; verdict is unknown. |
| Durable event outbox age approaching retention | Page before coverage becomes irrecoverable. |
| Per-signal failures or no signal success | Alert separately for logs, metrics, and traces. Logs are critical for lifecycle replay. |

## Operator runbook

For a non-green verdict:

1. Confirm snapshot freshness and coverage. If either fails, treat the result as
   `unknown`; inspect collector reachability and outbox depth/age first.
2. Open the schedule row and compare expected versus materialized. Missing means
   cron materialization; materialized pending/overdue means processing.
3. Inspect bad item and anomaly counts. Filter lifecycle logs on `schedule.id`,
   then narrow with trigger/batch/item/run/attempt IDs.
4. Open the correlated trace to find the slow or failed boundary: request,
   collection, dispatch, agent execution, result persistence, or acknowledgement.
5. Check agent connectivity, health, settings state, capacity, and pending
   results before retrying or resetting work.
6. For webhook/manual absence, prove whether the scheduler received the request.
   If it did not, continue in the upstream system's delivery SLI; scheduler
   telemetry cannot prove an event that never crossed its boundary.

`green` proves scheduler-observed completion only. It does not prove downstream
business side effects were correct; tasks that need that guarantee must publish
their own domain success SLI keyed by the scheduler run ID.
