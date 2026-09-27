# Application events with DynamoDB Streams

`tanstack-workflow-aws` can also act as the durable event source for an event-driven application without AWS Step Functions or EventBridge.

Application events are intentionally separate from TanStack Workflow's internal `WorkflowEvent` log. The workflow event log is part of replay and execution semantics. Application events are domain facts such as `task.requested`, `task.approved`, `task.started`, and `task.completed`.

## Publish a generic event

```ts
import { createDynamoApplicationEventPublisher } from '@ataylorme/tanstack-workflow-aws/events'

const events = createDynamoApplicationEventPublisher({
  tableName: process.env.TABLE_NAME!,
})

await events.publish({
  type: 'task.requested',
  data: {
    taskId: 'task-123',
    requestedBy: 'user-456',
  },
})
```

The payload type is generic and application-defined.

```ts
interface TaskCompleted {
  taskId: string
  resultLocation: string
}

await events.publish<TaskCompleted>({
  type: 'task.completed',
  data: {
    taskId: 'task-123',
    resultLocation: 's3://bucket/key',
  },
})
```

Each event is written as an immutable DynamoDB item using a conditional create. Supplying a stable `id` makes producer retries idempotent.

## DynamoDB Streams fan-out

Enable DynamoDB Streams with `NEW_IMAGE` on the table and attach one or more Lambda event source mappings. Each Lambda can filter for `entityType = APPLICATION_EVENT` and then dispatch based on `event.type`.

This supports independent consumers for notifications, projections, audit history, analytics, or workflow triggers without EventBridge.

Consumers must still be idempotent because DynamoDB Streams + Lambda provides at-least-once processing semantics.

## Optional destination bridges

The DynamoDB table remains the source of events. Deploy a separate stream Lambda for each optional destination, using `@ataylorme/tanstack-workflow-aws/event-stream` and the selected `.../bridges/<destination>` subpath. The bridges forward the complete event envelope (including its stable `id`) to EventBridge, SNS, SQS, or an HTTPS webhook. No bridge runs during the producer's `publish()` call.

```ts
import { createApplicationStreamHandler } from '@ataylorme/tanstack-workflow-aws/event-stream'
import { createSqsBridge } from '@ataylorme/tanstack-workflow-aws/bridges/sqs'

export const handler = createApplicationStreamHandler(
  createSqsBridge({ queueUrl: process.env.QUEUE_URL! }),
)
```

The [configurable bridge Lambda](../examples/event-bridge-handler.ts) selects one destination via `BRIDGE_KIND`. The [CloudFormation example](../cloudformation/event-bridge-consumer.yaml) deploys one copy in a chosen Region with stream filters, least-privilege destination permissions, partial batch responses, and an SQS failure destination. Pass that Region's *regional stream ARN*, the target ARN/URL, an existing failure queue ARN, and a ZIP of the bundled example handler. For example, bundle with `esbuild examples/event-bridge-handler.ts --bundle --platform=node --target=node22 --format=cjs --outfile=handler.js`, zip `handler.js`, upload to the regional S3 bucket, then deploy the template with the corresponding parameters. The `eventbridge` target uses `EventBusArn`; `sns` uses `TopicArn`; `sqs` uses both `QueueArn` (IAM) and `QueueUrl` (SDK); `webhook` uses `WebhookUrl`. Install the AWS SDK client for each selected bridge (`@aws-sdk/client-eventbridge`, `@aws-sdk/client-sns`, or `@aws-sdk/client-sqs`); the core package does not install these optional clients. Only provision destinations you need. Give the failure queue an operator alarm and redrive procedure. `LATEST` starts with new stream records after the mapping is created; this template does not backfill earlier events.

For FIFO SNS/SQS, provide a stable `messageGroupId` function in your own handler; the adapters then set `MessageDeduplicationId` to the application event ID. A missing FIFO group ID will cause delivery to fail. SNS/SQS deduplication windows are finite; external consumers and webhook receivers must deduplicate persistently by `event.id`. EventBridge assigns its own transport ID, while the original event ID remains inside `detail`. The EventBridge adapter checks per-entry failures and throws so the stream mapping retries. A nonexistent event bus may cause `PutEvents` to return success while discarding the event, so provision and monitor the bus before enabling a bridge.

Bridge execution is at least once. Configure receiving systems to treat `event.id` as an idempotency key. The handler retries from its first failed record, so previously successful records in the same batch may be delivered again. An SQS failure destination receives invocation metadata rather than the full stream payload; a redrive tool must retrieve the record while it remains in the stream or use an application-maintained event index to locate the original item. Stream records expire, so monitor iterator age and failure queues. For multi-Region MRSC, deploy each logical bridge in **one** Region, or deduplicate globally in the destination if both regional streams are consumed. During Region failover, deploy/repoint the mapping to the surviving Region and account for possible duplicate delivery. The event publisher and optional bridges do not make publication atomic with unrelated workflow or application writes.

HTTPS webhook credentials should be supplied through your own secret injection/rotation process; do not put credentials in the URL. The adapter rejects HTTP URLs and redirects, uses a bounded request timeout, includes `x-event-id`, and retries non-2xx responses through the stream mapping.

The [task lifecycle publishing example](../examples/application-events.ts) shows stable IDs for requested, approved, started, and completed transitions. The [consumer example](../examples/event-consumer.ts) separates notification and projection handlers; replace its logging stubs with application services and persistent per-consumer deduplication. The example publishes after the application has committed a transition; for guaranteed atomicity with that state change, add a transactional outbox in the application's authoritative store.
