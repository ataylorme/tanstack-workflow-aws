# Application event AWS test readiness

This guide is the package handoff for `tanstack-start-aws-high-availability` (its `test/tanstack-workflow-aws` branch). Application endpoints, UI, deployment orchestration and production side-effect logic belong to that repository. This PR prepares the package; it does not deploy or modify the application.

## Use the exact candidate

The previously published `0.1.0` does not contain this event API. This PR prepares `0.2.0-rc.0`; changing the manifest does **not** publish it. Test either a tarball built from this PR's exact commit or the exact prerelease after a maintainer publishes it. Do not use an unpinned branch or assume `latest` contains these changes.

```sh
# In this package's checkout at the reviewed commit:
npm ci
npm run typecheck
npm run build
npm run test:package
npm pack --pack-destination /path/to/shared-artifacts
# In the application repository, its integration owner installs that exact file:
# npm install --save-exact /path/to/shared-artifacts/ataylorme-tanstack-workflow-aws-0.2.0-rc.0.tgz
```

Record the package commit, tarball SHA-256 and application commit alongside AWS evidence. The application build and native consumer bundle must use the same reviewed package. A file dependency must also be copied into the Docker build context before `npm ci`; alternatively use an exact published prerelease with existing GitHub Packages authentication. Select only needed optional SDK peers. `npm run test:package` verifies the installed tarball, no optional clients in a core-only install, every bridge subpath, generic payload types, and compatibility with AWS Lambda's `DynamoDBStreamHandler` under strict TypeScript settings.

## Application integration contract

| Integration owner supplies | Package supplies |
| --- | --- |
| Same isolated MRSC table name in east and west; Ohio witness | Conditional immutable event writes and strong-read retry reconciliation |
| Producer role with `dynamodb:PutItem` and `dynamodb:GetItem` on that table | Stable IDs, original-envelope returns on duplicates, conflict rejection |
| Stable ID per domain transition, JSON payload/schema and correlation | Generic `ApplicationEvent<T>` envelope; no task-specific runtime behavior |
| Stream enabled with `NEW_IMAGE` or `NEW_AND_OLD_IMAGES` | INSERT decoder that ignores internal workflow/receipt records |
| One Region-local stream mapping, matching Lambda bundle and IAM | Handler returning partial batch failure sequence numbers, structured failure logs |
| Event source mapping with `ReportBatchItemFailures`, retry/age bounds and failure archive | Example template and one selected optional bridge |
| Destination idempotency; unordered-arrival handling; state-to-event recovery | At-least-once transport only; no cross-item transaction or side-effect atomicity |
| Region/SDK request timeouts and integration-specific validation | Injectable SDK clients and a remaining-Lambda-time guard |

Do not wire a new bridge inside the HTTP `publish()` operation. Publishing acknowledges durable DynamoDB storage; delivery happens asynchronously. Check downstream delivery independently. The example CloudFormation consumer is private: do not add a public Function URL. Both app Regions may publish; start with the consumer only in one chosen Region. Do not create four competing readers on a global-table shard. EventBridge is needed only if that optional bridge is selected; workflow timer infrastructure is separate.

## Prepare an isolated destination

Use a disposable test table/stack and a dedicated standard SQS queue. The queue is a simple observable destination for testing this package even if the final application uses another bridge. Provision the queue before the bridge. Use the template in `cloudformation/event-bridge-consumer.yaml` with `BridgeKind=sqs`, `Destination=<queue ARN>`, `QueueUrl=<same queue URL>` and the consumer Region's `LatestStreamArn` from `DescribeTable`. The template creates its own private failure archive and alarms; it does not create the destination queue or table.

Bundle `examples/event-bridge-handler.ts` exactly as described in the application-events guide. Upload the ZIP to a private, Region-local artifact bucket with an immutable key. Deploy the mapping and wait until AWS reports `State=Enabled`; do not rely only on the CloudFormation stack completing. Inspect `LastProcessingResult`, `FunctionArn`, `EventSourceArn`, filter criteria, and `FunctionResponseTypes`. Confirm the stream ARN belongs to the intended table in the same Region as the function. `TRIM_HORIZON` can deliver retained pre-existing events, so use an isolated table and idempotent consumers.

The sample SQS IAM policy assumes a standard, same-account queue. A customer-managed KMS key requires its additional permissions; cross-account resources require destination policies. Inspect the failure alarm and S3 destination permissions before the workload.

## Bounded live smoke runner

The runner has an offline plan by default. It makes AWS calls only with `--execute`; it never creates/deletes stacks or tables, purges queues, or removes event items. It publishes three unique facts and attempts same-ID retries/conflicts. Test data stays for diagnosis. Its optional queue check reads a dedicated queue and deletes only messages whose IDs belong to that run; other messages may be temporarily hidden by the receive visibility timeout, so do not use a business queue.

```sh
node scripts/test-events-live.mjs          # offline plan
export AWS_PROFILE=your-sandbox-profile
export EVENT_TEST_TABLE=your-isolated-mrsc-table
export EVENT_TEST_QUEUE_REGION=us-east-1
export EVENT_TEST_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/ACCOUNT/TEST_QUEUE
npm run test:events:live -- --execute
```

The profile needs `dynamodb:DescribeTable`, `dynamodb:PutItem` and `dynamodb:GetItem` on both regional replicas. Queue verification also needs `sqs:ReceiveMessage` and `sqs:DeleteMessage` on the dedicated queue and the optional `@aws-sdk/client-sqs` peer (already installed in this repository). The SDK resolves credentials normally; tokens never belong in command arguments. `EVENT_TEST_QUEUE_URL` may be omitted for a publisher-only test, but **that is not a stream-delivery pass**.

The runner checks MRSC and stream configuration in both Regions; publishes east/west facts; submits one ID concurrently in both Regions; checks identical committed envelopes and retained timestamps on retry; rejects different content using the same ID; strongly reads all facts from both replicas; and optionally waits up to three minutes for the full envelopes to reach SQS. Nonzero exit means failure. Evidence is saved to `.event-test-results/` (ignored by Git), including test IDs and outcomes, with owner-only permissions. It uses the default `EVENT` partition prefix. The runner intentionally does not verify CloudFront, the app's authenticated HTTP boundary, or business state atomicity.

## Required failure and replay drills

After a happy-path smoke pass, record these cases in the app's acceptance evidence. Use bounded fault injection only on dedicated test resources.

1. **Producer retry after response loss:** replay the same ID and payload from the opposite Region. The response and original timestamp must match; no extra stream INSERT should appear. A changed payload must fail with `ApplicationEventConflictError`.
2. **Destination unavailable:** temporarily deny the bridge's send permission or have a controlled webhook return 503. Publish one known ID. Confirm `application_event_delivery_failed`, a delivery-failure alarm, retry attempts, and eventual full batch in S3. Restore the permission/endpoint. A Lambda HTTP 200 with `batchItemFailures` is still a failed delivery.
3. **Malformed record and later progress:** inject a malformed `APPLICATION_EVENT` only in the isolated table. It must be retried/archived rather than acknowledged. With the sample one-record batch, later good records must resume after the bounded retry policy discards that record.
4. **Duplicate consumer invocation:** invoke the same saved stream batch twice against a test consumer. An idempotent business consumer must not apply its effect twice. SQS standard queues may contain duplicates; the library does not hide them.
5. **Consumer Region change:** disable the old mapping and wait for `Disabled`, enable the replacement mapping in the other Region, and repeat publishes. Retained records may replay; require idempotency. This is an operational handoff test, not proof of automatic regional failover or DynamoDB quorum recovery.
6. **Timeout and archive failure:** use a bounded slow handler to trigger the remaining-time path, then verify retries. Check `IteratorAge`, `Errors`, and `DestinationDeliveryFailures`; a missing archive due to IAM failure is a test failure, not a successful discard.

S3 failure objects contain a `payload` string holding the original Lambda event. Download a specific failure object with the AWS CLI, inspect its IDs, and parse that string to a local JSON batch. After correcting the fault, invoke the same private consumer Lambda with that batch; inspect both the invocation error indicator and `batchItemFailures` before marking it recovered. Preserve the event IDs. **Calling `publish()` again with the same ID does not generate a new stream record**, so it is not a replay mechanism. Replacing the ID invents a second fact and defeats deduplication. Preserve the failure object until verified recovery. The sample archive expires records after 30 days.

## Limits and completion criteria

Local/emulator tests do not simulate regional quorum, stream propagation, IAM, AWS retries, destination routing, or CloudFront. A pass requires observed delivery through the real mapping and receiver, plus the failure/replay checks above. Report publisher-only, stream-to-SQS, other bridge, and application-end-to-end outcomes separately; do not promote one to another.

For EventBridge, add an actual matching target and assert the full `detail` arrives; a `PutEvents` success alone is insufficient. For SNS, verify a confirmed subscriber receives and unwraps the message. For HTTPS, use a controlled endpoint with request capture, test 2xx/503/timeout and receiver deduplication; never use real notifications in this lab. For FIFO, use a custom `messageGroupId` function and test beyond the service's deduplication window.

No live AWS execution is claimed by this PR. The artifact is ready for controlled integration testing after its local gates pass; production qualification still requires the application's idempotency, ordering, authorization, recovery, retention and capacity design.

## Local verification for this candidate

On 2026-09-27, all 91 tests across 12 suites passed with DynamoDB Local. Strict typecheck, clean build, installed-tarball checks (including optional dependency isolation and Lambda handler typing), all four Lambda example bundles, and `cfn-lint cloudformation/*.yaml` passed. The smoke runner's no-AWS plan path is covered by a test. These results are local evidence, not live AWS outcomes.
