import { unmarshall } from '@aws-sdk/util-dynamodb'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'
import type { ApplicationEvent, ApplicationEventHandler } from './events.js'

export interface ApplicationStreamRecord {
  eventID?: string
  eventName?: string
  dynamodb?: { SequenceNumber?: string; NewImage?: Record<string, AttributeValue> }
}

/** Ignores workflow and consumer-state items; rejects malformed application items. */
export function decodeApplicationEvent(record: ApplicationStreamRecord): ApplicationEvent | undefined {
  if (record.eventName !== 'INSERT' || !record.dynamodb?.NewImage) return undefined
  const item = unmarshall(record.dynamodb.NewImage)
  if (item.entityType !== 'APPLICATION_EVENT') return undefined
  const event = item.event as ApplicationEvent | undefined
  if (!event || typeof event.id !== 'string' || typeof event.type !== 'string' ||
    !Number.isInteger(event.version) || typeof event.timestamp !== 'string' || !('data' in event)) {
    throw new Error('Malformed application event item')
  }
  return event
}

export interface ApplicationStreamBatch { Records: ApplicationStreamRecord[] }
export interface ApplicationStreamBatchResponse { batchItemFailures: { itemIdentifier: string }[] }

/** Process records in order, and retry from the first failure in that shard. */
export function createApplicationStreamHandler(handler: ApplicationEventHandler) {
  return async (batch: ApplicationStreamBatch): Promise<ApplicationStreamBatchResponse> => {
    for (const record of batch.Records) {
      try {
        const event = decodeApplicationEvent(record)
        if (event) await handler(event)
      } catch (error) {
        const sequence = record.dynamodb?.SequenceNumber
        if (!sequence) throw error // Lambda retries the entire batch.
        return { batchItemFailures: [{ itemIdentifier: sequence }] }
      }
    }
    return { batchItemFailures: [] }
  }
}
