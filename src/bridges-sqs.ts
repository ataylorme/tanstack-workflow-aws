import { applicationEventDeduplicationId } from './event-identity.js'
import { serializeApplicationEvent } from './event-validation.js'
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs'
import type { ApplicationEvent, ApplicationEventHandler } from './events.js'

export interface SqsBridgeOptions {
  queueUrl: string
  client?: SQSClient
  /** Required for FIFO queues; use a stable partition key for ordered delivery. */
  messageGroupId?: (event: ApplicationEvent) => string
}

export function createSqsBridge(options: SqsBridgeOptions): ApplicationEventHandler {
  if (!options.queueUrl) throw new Error('queueUrl is required')
  if (options.queueUrl.endsWith('.fifo') && !options.messageGroupId) throw new Error('messageGroupId is required for FIFO destinations')
  const client = options.client ?? new SQSClient({})
  return async event => {
    await client.send(new SendMessageCommand({
      QueueUrl: options.queueUrl,
      MessageBody: serializeApplicationEvent(event),
      MessageAttributes: { eventType: { DataType: 'String', StringValue: event.type }, ordered: { DataType: 'String', StringValue: event.ordering ? 'true' : 'false' } },
      ...(options.messageGroupId ? { MessageGroupId: options.messageGroupId(event), MessageDeduplicationId: applicationEventDeduplicationId(event) } : {}),
    }))
  }
}

