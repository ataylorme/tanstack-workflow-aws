# Application events with DynamoDB Streams

The application event API is generic and independent of TanStack Workflow's internal replay log. It stores immutable domain facts in DynamoDB. DynamoDB Streams triggers consumers; EventBridge is optional. The API is not a transactional outbox or a durable consumer registry.

## Publish and retry

```ts
import { createDynamoApplicationEventPublisher } from '@ataylorme/tanstack-workflow-aws/events'

const events = createDynamoApplicationEventPublisher({ tableName: process.env.TABLE_NAME! })
await events.publish<{ taskId: string; requestedBy: string }>({
  id: 'task-123:requested',
  type: 'task.requested',
  data: { taskId: 'task-123', requestedBy: 'user-456' },
})
```

An omitted ID generates a UUID; **supply the same stable ID on retries**. The publisher conditionally creates `PK = EVENT#<id>`, `SK = META`. On a duplicate or ambiguous write response, it strongly reads that item: identical content returns the original envelope; different content throws `ApplicationEventConflictError`. If the caller omits `timestamp`, reconciliation preserves the timestamp already stored. An explicitly supplied timestamp must match. Reads require `dynamodb:GetItem` as well as the writer's `dynamodb:PutItem`. Transient MRSC write conflicts are retried with a bounded backoff; unresolved infrastructure errors still reject. Retrying without a stable ID can produce a second event.

The JSON envelope contains `id`, `type`, positive integer `version` (default 1), ISO timestamp, generic `data`, optional `correlationId`, `causationId`, and a string `metadata` map. Data must be portable JSON: no `undefined`, bigint, non-finite/unsafe integer numbers, cycles, class instances, dates, sets, or maps. Convert those deliberately before publishing. IDs/correlation fields are bounded to 256 characters, types to 100, nesting to 24, and the serialized envelope to 240 KiB. This leaves space for DynamoDB and bridge wrappers. Store larger payloads externally and publish their location. Validation happens before writing.

Application state updates and event publication are separate operations. A successful state update followed by a failed publish needs application recovery or an outbox in the authoritative store. MRSC tables do not support DynamoDB transaction APIs; do not assume a multi-item transaction can make this atomic.

## Consume stream records

```ts
import type { DynamoDBStreamHandler } from 'aws-lambda'
import { createApplicationStreamHandler } from '@ataylorme/tanstack-workflow-aws/event-stream'

export const handler: DynamoDBStreamHandler = createApplicationStreamHandler(
  async event => { /* validate event.data for your domain; apply an idempotent effect */ },
  { minRemainingTimeMs: 15_000 },
)
```

The adapter accepts Lambda's stream event type, ignores non-INSERT and unrelated items, validates application envelopes, processes a batch sequentially, and stops at the first failure. It returns the failing **sequence number** in `batchItemFailures`. Enable `ReportBatchItemFailures` on the event source mapping. If no usable sequence exists, it throws to retry the entire batch. It logs a stable `application_event_delivery_failed` marker plus IDs and error name, without payloads. An optional `onError` hook can add application diagnostics. A remaining-time guard avoids starting another record near timeout; each handler must also bound its own I/O. It cannot interrupt an already-running handler.

Delivery is at least once. Retried batches, duplicate stream delivery, regional failover, and an acknowledged external effect followed by a crash can all repeat side effects. Every destination needs idempotency using the application event ID. A conditional receipt written *before* a nontransactional side effect can lose delivery after a crash; writing it *after* can repeat that effect. This package does not claim exactly-once delivery.

There is no global business-event ordering: separate event IDs are separate DynamoDB items and can use different shards. Do not assume `task.requested` will be delivered before `task.completed`. Use domain versions or sequence numbers where order matters. FIFO bridges preserve arrival order to their destination, not an order that the source never supplied.

## Optional bridges

| Subpath | Factory | Optional SDK client | Transport envelope |
| --- | --- | --- | --- |
| `bridges/eventbridge` | `createEventBridgeBridge` | `@aws-sdk/client-eventbridge` | Full event in `detail`; `detail-type` is `<type>.v<version>` |
| `bridges/sns` | `createSnsBridge` | `@aws-sdk/client-sns` | Full event JSON in SNS `Message` |
| `bridges/sqs` | `createSqsBridge` | `@aws-sdk/client-sqs` | Full event JSON in SQS `MessageBody` |
| `bridges/webhook` | `createWebhookBridge` | None (Node fetch) | Full event JSON in HTTPS POST body |

Install only the AWS clients used by your own handler. Core, events, stream, and webhook imports work without them. The configurable example imports all adapters, so building that entire example requires all three clients, included in this repository's dev dependencies. A custom SQS-only handler needs only the SQS peer:

```ts
import { createSqsBridge } from '@ataylorme/tanstack-workflow-aws/bridges/sqs'
import { createApplicationStreamHandler } from '@ataylorme/tanstack-workflow-aws/event-stream'
export const handler = createApplicationStreamHandler(
  createSqsBridge({ queueUrl: process.env.QUEUE_URL! }),
  { minRemainingTimeMs: 15_000 },
)
```

Each AWS bridge accepts its own SDK client for explicit Region, retry and request-timeout configuration. Configure bounded SDK request timeouts within your Lambda budget. The EventBridge bridge checks individual entry failures, even on HTTP success. A nonexistent event bus can still silently discard events, so provision the bus and verify an actual receiving target. SNS subscribers normally receive SNS's outer envelope unless raw delivery is configured; `eventType` is also supplied as a message attribute for SNS/SQS.

FIFO SNS/SQS destinations require a `messageGroupId` function. The adapter hashes `event.id` with SHA-256 for a stable, bounded `MessageDeduplicationId`; the original ID remains in the payload. Choose valid group IDs and retain downstream deduplication beyond AWS's finite FIFO deduplication window. The stock configurable Lambda and template target standard SNS/SQS; supply a custom handler for FIFO.

Webhooks require HTTPS, reject credentials in URLs and redirects, default to a ten-second timeout, and throw on non-2xx or network failures. `x-event-id` contains `encodeURIComponent(event.id)`; decode it if using the header as an idempotency key. Use the JSON `id` unchanged for all other consumers. Inject authentication through `headers` using your secret management process. Responses are cancelled after reading their status to free connections without buffering arbitrary bodies. Authentication, signatures, rate limiting and receiver-side idempotency are application responsibilities.

## Deploy and operate

The [bridge Lambda](../examples/event-bridge-handler.ts) selects one target via `BRIDGE_KIND`. The [CloudFormation template](../cloudformation/event-bridge-consumer.yaml) provides IAM, an INSERT/application-item filter, partial batch reporting, one-record batches, bounded retries, retained logs, delivery-failure/iterator-age/runtime-error alarms, and a private S3 failure archive with 30-day expiration. Attach notification actions to the alarms for your deployment. The archive preserves the full original batch, unlike an SQS/SNS failure destination that only retains metadata.

Supply the Region-local `StreamArn`, regional code bucket, immutable ZIP key, `BridgeKind`, and `Destination` (event bus/topic/queue ARN or HTTPS URL). SQS also needs `QueueUrl`; EventBridge has an `EventSource` parameter. Match the ARN and queue URL, use same-account regional resources for the example, and provision the selected target first. KMS-encrypted targets or cross-account targets need additional policies outside this minimal example.

Bundle from this repository:

```sh
npm ci
npx --no-install esbuild examples/event-bridge-handler.ts --bundle --platform=node \
  --target=node22 --format=cjs --outfile=build/bridge/handler.js
(cd build/bridge && zip -X bridge.zip handler.js)
```

The mapping uses `TRIM_HORIZON` to avoid missing records during eventual mapping creation. It can replay retained records; enforce idempotency. DynamoDB Streams retains records for 24 hours; older table items are not automatically replayed. The example sends exhausted/over-age batches to S3 after at most five retries or one hour of record age, so that archive is part of recovery, not a guarantee of successful final delivery. Monitor archive write failures (`DestinationDeliveryFailures`) as well as the included alarms. Confirm the mapping is `Enabled` before beginning acceptance tests.

For global tables AWS recommends one simultaneous stream reader per shard. Do not deploy all four example bridges plus multiple application consumers onto the same replica stream. Choose one reader and fan out through SNS or another application-owned delivery mechanism if many independent consumers are needed. MRSC replicas contain the same stream changes, so deploying the same logical consumer in both Regions can duplicate every side effect. Start with one chosen consumer Region for testing. Regional consumer failover requires controlled mapping activation and destination deduplication; producers may remain active in both Regions.

See [live AWS event testing](event-testing.md) for artifact pinning, the bounded smoke runner, fault/replay checks, and the application integration contract. [Publishing examples](../examples/application-events.ts) illustrate task facts; [consumer stubs](../examples/event-consumer.ts) illustrate routing and must be connected to real services with idempotency.

References: [MRSC behavior](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html), [Lambda stream processing](https://docs.aws.amazon.com/lambda/latest/dg/with-ddb.html), and [failure destinations](https://docs.aws.amazon.com/lambda/latest/dg/services-dynamodb-errors.html).
