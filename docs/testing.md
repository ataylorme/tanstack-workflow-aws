# Validation and AWS acceptance

## Local checks

```sh
npm ci
npm run typecheck
npm run build
DYNAMODB_LOCAL_JAR=/path/to/DynamoDBLocal.jar npm run test:integration
npm run test:package
npx --no-install esbuild examples/handler.ts examples/router.ts examples/worker.ts examples/application-consumer.ts examples/ordered-subscriber.ts \
  --bundle --platform=node --target=node22 --format=cjs --outdir=build/ci
cfn-lint cloudformation/*.yaml
```

The integration runner accepts only localhost DynamoDB endpoints. Tests create/delete isolated tables and require no AWS credentials. `npm test` without a local endpoint skips database suites. Package checks install the tarball into a separate project, exercise public exports and verify TypeScript declarations.

Coverage includes regional claim races, accepted signal/approval recovery, persisted timer waits, schedule lease fencing, generation changes, all missed-tick policies, zoned cron, overlap, duplicate publication, ambiguous writes, uncommitted segments, successor creation, history/payload/receipt limits, effect-aware cleanup and tombstone expiry. Targeted runtime tests reject broad queries and scans. Router tests cover old/new image comparison, heartbeat suppression, key coalescing, failure checkpoints and downstream queue isolation. Transport tests cover deterministic deadlines, conflicting schedules, late creation and durable fallback failure.

CI runs typechecking, builds, DynamoDB Local tests, package tests and handler bundling on Node 20, 22 and 24. Node 22 also validates CloudFormation.

## Controlled AWS acceptance

Deploy an isolated MRSC table and worker stacks with bounded workloads. Keep immutable build artifacts and all needed workflow definitions available in both regions. Local tests cannot establish AWS permissions, timing, quorum behavior or regional availability.

| Scenario | Required evidence |
| --- | --- |
| Topology | Exactly one stream mapping per replica and two independent processing queues |
| Ordered subscribers | Reversed/duplicate regional notifications produce ascending callbacks; uncertain effects block later positions until resolved |
| Concurrent claims | One conditional owner for each run, timer or bucket; duplicate external delivery handled idempotently |
| Interrupted execution | Other-region recovery after queued creation, accepted signal/approval, persisted timer wait and schedule claim |
| Self-advancing schedules | Several consecutive ticks without another materialization call; missed ticks obey each policy |
| Edits/cancellation | Accepted buckets retain input; new generations control future ticks; disabled schedules stop new buckets |
| Publication crash | Kill after event publication but before cursor acknowledgement; recover the same event ID |
| Staging crash | Unreachable log segments never publish application events |
| Continue as new | Retry successor creation and observe one successor with empty initial history |
| Cleanup | Pending effects block history deletion; partial deletion resumes; late IDs are rejected during tombstone retention |
| Bounds | Payload and history limits fail explicitly; normal runs continue as new before exhaustion |
| Regional outage | Surviving region progresses when quorum exists; quorum loss fails writes safely |
| Queue/archive failure | Failed handoff is retried or retained; partial failures and deferred diagnostics produce alarms |
| Idle behavior | No periodic worker invocations once immediate work drains; future retention deadlines remain scheduled |
| Amplification | Measure input records, unique keys, ignored heartbeat/checkpoint changes and no-op messages per completed run |

Test POST retry routing separately: the example edge router provides placement, not automatic mutation failover. DSQL transactional outbox and downstream effect idempotency are application-owned boundaries. Record observed run IDs, timings and outcomes privately. No live AWS validation is implied by emulator or CI success.
