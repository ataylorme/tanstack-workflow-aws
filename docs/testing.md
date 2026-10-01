# Review evidence and controlled AWS testing

Readiness here means a package suitable for a disposable, monitored AWS integration environment. It does not mean production qualification. Local tests cannot establish MRSC quorum behavior, regional replication, IAM correctness in a deployed stack, CloudFront propagation, or Lambda timing under load.

## Reproduce the checks

```sh
npm ci
npm run typecheck
npm run build
npm test
npm run test:package
DYNAMODB_LOCAL_JAR=/absolute/path/to/DynamoDBLocal.jar npm run test:integration
cfn-lint cloudformation/*.yaml
```

Install Java 17+ and download DynamoDB Local following [AWS's instructions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html). Alternatively, set `DYNAMODB_ENDPOINT` to an already-running local emulator. The integration runner creates isolated tables and deletes them afterwards. Its endpoint guard deliberately prevents using it against real AWS. A plain `npm test` without the emulator skips the Local suites; those skips are not integration passes.

The package check builds a tarball, installs it in a clean consumer project, verifies the public imports and typechecks the included store/runtime contract. It needs registry access or a populated npm cache. The upstream contract tests preserve their source commit and license provenance in the test file.

## Original workflow-runtime review result

All **61 tests across 8 suites** passed with DynamoDB Local enabled. Typecheck, build, clean packed-consumer installation, all CloudFormation lint checks and both example Lambda bundles also passed. Three review agents completed two review passes, with an independent final read of the recovery fixes. No blocker for controlled AWS integration testing remained in those reviews. This is an engineering assessment, not a measured reliability percentage.

## What the review changed

Three independent review agents examined storage/concurrency, upstream runtime compatibility, and delivery/templates. A second review followed the fixes and added further targeted regressions.

| Finding | Correction / regression evidence |
| --- | --- |
| DynamoDB expressions carried unused names | Execute the real SDK expressions against DynamoDB Local |
| SDK retry after a successful staging or publication write | Reconcile the unique immutable segment; inject ambiguous failures |
| Queued starts and released running leases could disappear from sweep discovery | Index both states and recover through the matching runtime |
| Accepted signal/approval payload could be lost on process death | Persist acceptance payload; publish resolution and clear pending data atomically |
| Crash between persisting a timer wait and registering its timer | Discover timer waits directly from persisted run metadata |
| Materializing the next schedule tick hid an unfinished previous bucket | Keep the pending bucket and its lease in the schedule item |
| Overlap policies were advertised without enforcement | Implement `allow`/`skip`; reject unsupported policies |
| Published upstream engines lacked the recovery contract | Bundle a pinned MIT-licensed snapshot and test its public entry points together |
| Invalid input or missing registrations could repeatedly block sweep work | Persist terminal validation failures; isolate recoverable driver errors with backoff |
| Timer claim survived into a subsequent wait | Clear the timer lease when accepting its resolution |
| HTTP and sweep work shared execution limits | Separate targeted worker Lambda and bounded HTTP handler examples |
| Updating deployment artifacts could reuse stale versions | S3 object-version support and explicit edge router revision |

The source is still pre-release. Create a fresh table for AWS tests; do not mix workers or persisted schedules from the earlier prototype schema. Schedule workflow identity and overlap policy are immutable for an existing schedule ID; drain or disable the old schedule and create a new ID to change them. Input/configuration updates remain supported. Use unique lease owners for every invocation and bind every runtime call through `withLeaseOwner`.

## Deferred work diagnostics

Targeted and legacy sweep driver failures are isolated per claimed item. Inspect the returned `recovered`, `scheduled` and `timers` results for `RUN_ERRORED` diagnostics with `recovery_deferred`, `schedule_deferred` or `timer_deferred` codes; these diagnostics describe a retry, not necessarily a terminal stored run. Log and alarm on them in your worker wrapper. Missing workflow registrations remain recoverable; restore the matching loader rather than deleting work. Input-validation and workflow-version mismatches are terminal errors and require operator/application handling.

The DynamoDB host hook defers failed run recovery for 60 seconds. Pending-resolution failures also retain `recoveryError` on the run's base metadata for inspection; they are not discarded. A store-wide outage can still fail the entire invocation. Monitoring and remediation are deployment responsibilities.

## Live AWS acceptance plan

Use a disposable account or isolated stacks, small bounded workloads, cost limits and logs that include run ID, owner, Region and workflow version. Provision the MRSC table once with east/west replicas and the Ohio witness. Deploy compatible workflow registries and separate targeted worker Lambdas in both Regions. Keep workflow definitions available for every in-flight version.

1. **Deployment and package:** build from the reviewed commit, install the tarball in the actual application build, deploy templates, and confirm both local DynamoDB endpoints and IAM roles work. Verify each regional stream dispatcher and Scheduler-to-SQS-to-targeted-worker path is enabled, with no recurring sweep rule. Exercise the standalone API or your Start/Lambda Web Adapter integration.
2. **Concurrent claims:** submit the same run ID concurrently to both Regions. Repeat with the same signal ID and schedule tick. Verify a single committed event chain, one accepted delivery and one bucket ownership at a time. Measure conflict retries.
3. **Interrupted work:** terminate a worker after run creation, signal/approval acceptance, event staging, event publication, persisted timer wait, and schedule claim. Confirm the other regional worker resumes the same run and retains the original payload. Repeat with an intentionally expired lease and a stale worker attempting to write.
4. **Network uncertainty:** inject response loss/timeouts around writes. Verify that retries neither erase an acknowledged delivery nor publish partial event batches. Inspect unreachable event segments separately from committed history.
5. **Regional disruption:** block one application's DynamoDB access or stop its workers, then verify the other Region progresses while quorum remains available. Also test loss of quorum: writes must fail rather than allow divergent execution. Restore access and observe eventual recovery.
6. **Routing:** verify mutation retries reach a healthy regional endpoint with the same IDs. The edge example distributes traffic; it does not automatically fail over POST requests. Check your own health-routing mechanism separately.
7. **Scheduling and timing:** test both supported overlap policies, expired bucket leases, duplicate one-time wakeups and SQS deliveries, delayed GSI discovery, and several consecutive sleeps. Change schedule inputs while a previous bucket is pending and verify that the accepted bucket retains its original input.
8. **Application effects:** inject a crash after a DSQL/business transaction but before the workflow event commits. Verify per-step idempotency and transactional outbox reconciliation. These application mechanisms are not implemented by the package.
9. **Bounds and operations:** measure event-chain latency, GSI hot partitions, clock skew, state growth, the 400 KB item limit and targeted worker capacity (plus legacy sweep capacity during migration). Add alarms for failed/retried work and a retention/orphan cleanup procedure. Exercise deployment rollback with in-flight versions.

Record actual outcomes, timings and run IDs. A failing acceptance case is a release blocker, even if every local test passes. No live AWS deployment, MRSC failover test or production load test was performed during the code review.

## Application event readiness

The event API and optional bridges have a separate [AWS test handoff](event-testing.md).
Run `npm run test:package` to check the installable artifact and all optional exports,
and run the full suite with DynamoDB Local for duplicate publication/conflict coverage.
`node scripts/test-events-live.mjs` is an offline plan; `--execute` is explicitly required
for the bounded live publisher/optional SQS checks. No AWS delivery outcome is implied
by local or emulator passes. The application repository owns its own integration changes.

### Demand-driven wakeup acceptance

Test the [wakeup example](workflow-wakeups.md) beyond store unit tests: consecutive sleeps; abandonment after durable state writes; execution/timer-claim lease expiry; zero reliance on index visibility; many unrelated pending items; stale or duplicate messages; failure during schedule creation; and worker timeout before acknowledgement. Disable one regional processing path, verify the other recovers, restore it in `finally`, then repeat in the opposite direction. Exercise queue DLQ redrive and reconciliation after a stream retention gap.

After outstanding deadlines and obsolete wakeups drain, observe at least 15 idle minutes with zero dispatcher/worker invocations, empty processing queues, no failure alarms, and no recurring rules. Indefinite signal and approval waits should remain idle; intentionally recurring application schedules are not idle work. Verify application-event delivery independently: its bridge is not the workflow scheduler.

## Targeted worker acceptance

`test/targeted-runtime.test.ts` rejects every `Query`/`Scan` and broad claim while exercising direct recovery, leases, concurrent delivery, consecutive timers, schedule bucket recovery, overlap policies, standalone timer claims, exhausted execution budgets and loader backoff. The crash-recovery suite exercises queued starts, accepted signals/approvals, persisted waits without timer registration, and released running leases through both targeted and legacy entry points. Transport tests verify current-category routing, obsolete messages, keyed continuations, legacy drain compatibility and failures before acknowledgement.

On AWS, submit several unrelated runs and deliver one keyed message. Verify only its named work is claimed, apart from normal stream-driven invocations for the other records. Check `process_target` traces and DynamoDB calls: no normal-path `DueIndex` queries or scans. Induce a lease race and a state/category transition between worker reads; confirm the successor names the same key with the current deadline. Deny continuation delivery temporarily and verify SQS retries instead of acknowledging unresolved work. Check the old drain and explicit sweep paths separately; they intentionally retain discovery. These local tests do not prove live MRSC coordination, regional failure behavior, or AWS delivery timing.
