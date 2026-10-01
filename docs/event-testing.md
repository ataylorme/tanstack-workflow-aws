# Application event AWS acceptance testing

Build and install a tarball from the exact reviewed commit. Record the commit, tarball SHA-256, application commit and stack outputs alongside test evidence. The application and private workers must use the same package artifact.

```sh
npm ci
npm run typecheck
npm run build
npm run test:package
npm pack --pack-destination /path/to/shared-artifacts
```

## Isolated deployment

Provision the MRSC table and regional [workers stacks](workflow-wakeups.md). Verify `STRONG` consistency, active east/west replicas and the Ohio witness. Each regional stream must use `NEW_AND_OLD_IMAGES` and exactly one enabled router mapping. Confirm its filter, `ReportBatchItemFailures`, retry limits and S3 failure destination. Confirm both SQS mappings are enabled.

Create a dedicated standard SQS test queue and subscribe it to the application SNS topic with **raw message delivery enabled** and a queue policy allowing that topic. The smoke runner must read this observation queue, not the application work queue competing with its Lambda consumer. Cross-account or customer-managed encryption requires additional policies. Do not use a business queue for the test.

The application owns authenticated endpoints, payload schemas, stable operation IDs, state-to-event handoff and consumer idempotency. Publishing acknowledges durable DynamoDB storage; verify downstream delivery separately. The workflow outbox should be tested through a workflow using `publishWorkflowEvent`, as well as the direct publisher below.

## Bounded smoke runner

The runner prints an offline plan unless given `--execute`. It creates no infrastructure, deletes no table data, publishes three uniquely identified facts and exercises same-ID retry/conflict handling. Optional queue observation deletes only messages belonging to that run; unrelated messages can be temporarily hidden by visibility timeout. Evidence is stored in `.event-test-results/` with owner-only permissions.

```sh
node scripts/test-events-live.mjs
export AWS_PROFILE=your-sandbox-profile
export EVENT_TEST_TABLE=your-isolated-mrsc-table
export EVENT_TEST_QUEUE_REGION=us-east-1
export EVENT_TEST_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/ACCOUNT/TEST_QUEUE
npm run test:events:live -- --execute
```

The profile needs `DescribeTable`, `PutItem` and `GetItem` in both replica Regions, plus `ReceiveMessage` and `DeleteMessage` on the observation queue. The SQS optional peer is required. Credentials use the normal AWS SDK chain. Omitting the queue tests publication only, not delivery.

The runner verifies MRSC/stream settings, publishes from both Regions, races a shared ID, checks retained timestamps and conflicting content, strongly reads both replicas, and waits up to three minutes for full envelopes in SQS. Nonzero exit indicates failure. Retention later cleans up test events through the normal keyed worker.

## Recovery drills

Use dedicated resources and bounded fault injection. Record IDs, expected outcomes and CloudWatch evidence.

1. Lose a producer response, then retry the same ID and content from the other Region. Require the same envelope and timestamp; changed content must fail.
2. Deny the router's application queue send permission. Require partial stream failure, routing alarm and eventual full batch archive. Restore access and replay an inspected archived batch to the private router.
3. Deny the application worker's SNS permission. Require application delivery failure logs, retries and the application DLQ while independent workflow messages still progress. Restore access and redrive the inspected messages.
4. Crash after an outbox publication but before cursor acknowledgement. Retry and verify one event item with the same ID and eventual cursor progress. Repeat concurrent drains in both Regions.
5. Invoke the same consumer message twice and deliver events out of order. Verify the application's idempotency and domain-version checks.
6. Exercise a slow handler and unavailable failure archive. Verify timeout guards, `IteratorAge`, queue age and `DestinationDeliveryFailures`; a failed archive write is not successful recovery.
7. Exercise cleanup after the configured retention boundary and confirm payload deletion, tombstone rejection of late retries, and eventual tombstone expiry.

S3 archive objects contain the original Lambda batch in their payload. Inspect a specific object, parse it to a local JSON batch, correct the fault and invoke the private router. Check invocation errors and `batchItemFailures` before considering replay complete. Application queue redrive and stream archive replay are separate recovery operations.

These tests do not establish application transaction atomicity, edge routing correctness or automatic quorum recovery. Validate those in the application's AWS acceptance suite.
