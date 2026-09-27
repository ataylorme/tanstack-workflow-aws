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
  const client = options.client ?? new SQSClient({})
  return async event => {
    await client.send(new SendMessageCommand({
      QueueUrl: options.queueUrl,
      MessageBody: JSON.stringify(event),
      MessageAttributes: { eventType: { DataType: 'String', StringValue: event.type } },
      ...(options.messageGroupId ? { MessageGroupId: options.messageGroupId(event), MessageDeduplicationId: event.id } : {}),
    }))
  }
}

