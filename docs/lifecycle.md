# Recurring schedules, history limits and retention

## Recurring schedules

`materializeWorkflowSchedules(runtime)` seeds the next future occurrence for each registered definition. Call it from deployment/configuration code. Workers advance deadlines without a periodic materialization process. Definition input functions run when materialized, not once per tick.

```ts
const runtime = defineWorkflowRuntime({ store, workflows: {
  report: {
    load: async () => reportWorkflow,
    schedules: [{ id: 'daily-report',
      schedule: { kind: 'cron', expression: '0 9 * * *', timezone: 'America/Los_Angeles' },
      overlapPolicy: 'skip', missedTickPolicy: 'run-once', input: {},
    }],
  },
} })
await materializeWorkflowSchedules(runtime)
```

Intervals are aligned to UTC epoch multiples and require positive integer milliseconds. Cron uses deterministic five-field expressions, defaults to UTC, and accepts IANA timezones. The pinned `cron-parser` dependency handles daylight-saving transitions; interval durations do not change with daylight saving. Randomized `H` expressions are rejected. Deadlines describe eligibility, not exact delivery time: SQS and Scheduler add delivery latency.

| Missed-tick policy | Behavior when multiple occurrences are due |
| --- | --- |
| `skip` | Advance to the first future occurrence without running overdue buckets |
| `run-once` (default) | Run one overdue bucket, then advance to the first future occurrence |
| `catch-up` | Run at most `maxCatchUp` overdue buckets (default 10, maximum 100), then skip the rest |

A slightly late delivery is not skipped if the following occurrence is still in the future. Overlap `skip` consumes a tick without starting a run while the schedule's active run is nonterminal; `allow` permits overlapping runs. Other overlap policies are rejected.

Claims persist the accepted bucket and lease on the same metadata item. Finalization clears that bucket and advances `nextFireAt` in one conditional write. A crash leaves either a reclaimable bucket or a persisted next deadline. Bucket IDs include schedule generation and fire time. Editing workflow identity, input, timing or policy creates a new generation; identical registrations are idempotent. Older configuration timestamps cannot overwrite newer definitions. Disabling a definition stops future buckets but allows an already-accepted bucket to finish. Its completion cannot overwrite a newer generation's next deadline.

## Committed effects and continue as new

`publishWorkflowEvent(ctx, stepName, event)` records a publication intent in the step's checkpoint. The log head and pending-outbox boundary commit together on the run metadata item. The worker walks only the reachable committed chain, publishes with a stable ID, and advances a conditional cursor only after the publisher accepts it. A crash between publication and cursor acknowledgement causes an idempotent retry. Concurrent regional drainers use the same IDs.

A published event describes that checkpoint; it does not imply that later workflow steps completed. Use immutable event payloads. Direct `events.publish()` is available for producers, but calling it as an uncoordinated external effect inside a workflow does not provide this outbox guarantee. The `$workflowEffect` result field is reserved for these helpers.

Return `continueAsNew(ctx, nextInput)` from the workflow handler. The terminal log record commits a deterministic successor intent. Outbox processing creates that successor with the same workflow identity, and its stream record triggers normal execution. Each successor starts with fresh history and signal receipts; callers must follow the returned successor ID when signalling the next run. External effects still require their own idempotency keys. Do not interpret the continuation marker as a final business result.

## Limits

`createDynamoWorkflowExecutionStore({ tableName, limits })` accepts:

| Option | Default | Purpose |
| --- | --- | --- |
| `maxHistoryEvents` | 256 | Bound replay round trips; continue as new proactively |
| `maxHistoryBytes` | 1 MiB | Bound aggregate serialized replay data |
| `maxItemBytes` | 300 KiB | Leave headroom below DynamoDB's item limit; maximum configurable value 350 KiB |
| `maxSignalIds` | 1,000 | Bound accepted signal/approval receipts on one run |
| `terminalRetentionMs` | 7 days | Preserve terminal state/history before cleanup |
| `tombstoneRetentionMs` | 30 days | Keep compact ID tombstones after payload cleanup |

Limits are positive safe integers. Oversized inputs or event batches fail before acceptance. History exhaustion raises `WorkflowLimitError`; one terminal error event has reserved space so a failure can be recorded. Interrupted bounded failures encountered during recovery become terminal rather than retrying forever. No history is truncated during replay. Large payloads belong in external storage; put immutable references in workflow state/events.

Application event publishers independently accept `retentionMs`, default seven days. Keep retention longer than your operational recovery/redrive window. A duplicate publish to a retained event returns the original envelope; a tombstone prevents reusing its ID. After tombstone expiry, IDs can be reused, so applications must not replay older requests without reconciliation.

## Cleanup

Terminal state and application event writes persist `cleanupAt`. The router schedules one-time keyed cleanup. A run with pending outbox effects is deferred, preserving its committed intent history. Aborting a run stops execution but still delivers intents that already committed. Cleanup fences terminal runs against writes, deletes committed and unreachable event segments in bounded pages, then replaces metadata with a compact tombstone. A later keyed deadline deletes the tombstone. Cleanup is retryable after partial deletion and does not use TTL or a table-wide periodic scan.

Tombstones remove payloads and signal-ID collections but retain enough identity to reject late requests during the retry window. Indefinitely paused workflows retain their history until they finish or are explicitly aborted. External object storage retention, application consumer receipts, backups and audit exports remain application responsibilities. Benchmark your selected replay bounds against MRSC latency and Lambda budgets before production use.
