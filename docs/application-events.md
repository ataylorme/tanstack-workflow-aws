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
