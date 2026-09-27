import { SNSClient, PublishCommand } from '@aws-sdk/client-sns'
import type { ApplicationEvent, ApplicationEventHandler } from './events.js'

export interface SnsBridgeOptions {
  topicArn: string
  client?: SNSClient
  /** Required for FIFO topics; use a stable partition key for ordered delivery. */
  messageGroupId?: (event: ApplicationEvent) => string
}

export function createSnsBridge(options: SnsBridgeOptions): ApplicationEventHandler {
  if (!options.topicArn) throw new Error('topicArn is required')
  const client = options.client ?? new SNSClient({})
  return async event => {
    await client.send(new PublishCommand({
      TopicArn: options.topicArn,
      Message: JSON.stringify(event),
      MessageAttributes: { eventType: { DataType: 'String', StringValue: event.type } },
      ...(options.messageGroupId ? { MessageGroupId: options.messageGroupId(event), MessageDeduplicationId: event.id } : {}),
    }))
  }
}

