import { unmarshall } from '@aws-sdk/util-dynamodb'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'
import type { ApplicationEvent, ApplicationEventHandler } from './events.js'
import { serializeApplicationEvent } from './event-validation.js'

/** Structural input compatible with the AWS Lambda DynamoDBStreamEvent type. */
export interface ApplicationStreamRecord {
  eventID?: string | undefined
  eventName?: string | undefined
  dynamodb?: { SequenceNumber?: string | undefined; NewImage?: Record<string, unknown> | undefined; OldImage?: Record<string, unknown> | undefined } | undefined
}

/** Decode immutable event inserts and committed ordered-head changes; never staged slots. */
export function decodeApplicationEvent(record: ApplicationStreamRecord): ApplicationEvent | undefined {
  if (!['INSERT', 'MODIFY'].includes(record.eventName ?? '') || !record.dynamodb?.NewImage) return undefined
  const image = record.dynamodb.NewImage
  if ((image.entityType as { S?: string } | undefined)?.S === 'ORDERED_STREAM') {
    const item = unmarshall(image as Record<string, AttributeValue>)
    if (item.schemaVersion !== 1) throw new Error('Unsupported ordered stream storage version')
    const old = record.dynamodb.OldImage ? unmarshall(record.dynamodb.OldImage as Record<string, AttributeValue>) : undefined
    if (!item.committed || item.committed === old?.committed) return undefined
    serializeApplicationEvent(item.event)
    if (item.event.ordering?.streamId !== item.streamId || item.event.ordering.sequence !== item.committed) throw new Error('Invalid ordered head notification')
    return item.event as ApplicationEvent
  }
  if (record.eventName !== 'INSERT') return undefined
  if ((image.entityType as { S?: string } | undefined)?.S !== 'APPLICATION_EVENT') return undefined
  const item = unmarshall(image as Record<string, AttributeValue>)
  serializeApplicationEvent(item.event)
  return item.event as ApplicationEvent
}

export interface ApplicationStreamBatch { Records: ApplicationStreamRecord[] }
export interface ApplicationStreamBatchResponse { batchItemFailures: { itemIdentifier: string }[] }
export interface ApplicationStreamContext { getRemainingTimeInMillis(): number }
export interface ApplicationStreamHandlerOptions {
  /** Yield before starting another record. Must cover a single handler's I/O budget. */
  minRemainingTimeMs?: number
  /** Receives failures, including validation failures; never log event payloads or secrets. */
  onError?: (error: unknown, record: ApplicationStreamRecord, event?: ApplicationEvent) => void | Promise<void>
}

/** Requires ReportBatchItemFailures on the event source mapping. Does not deduplicate side effects. */
export function createApplicationStreamHandler(handler: ApplicationEventHandler, options: ApplicationStreamHandlerOptions = {}) {
  const minRemainingTimeMs = options.minRemainingTimeMs ?? 2_000
  if (!Number.isFinite(minRemainingTimeMs) || minRemainingTimeMs < 0) throw new TypeError('minRemainingTimeMs must be nonnegative')
  return async (batch: ApplicationStreamBatch, context?: ApplicationStreamContext): Promise<ApplicationStreamBatchResponse> => {
    for (const record of batch.Records) {
      let event: ApplicationEvent | undefined
      try {
        event = decodeApplicationEvent(record)
        if (!event) continue
        if (context && context.getRemainingTimeInMillis() < minRemainingTimeMs) throw new Error('Insufficient time to start another application event')
        await handler(event)
      } catch (error) {
        // Partial failures do not increment Lambda's Errors metric. This stable
        // structured log supports a CloudWatch metric filter without leaking data.
        console.error(JSON.stringify({ message: 'application_event_delivery_failed', eventId: event?.id,
          sequenceNumber: record.dynamodb?.SequenceNumber, errorName: error instanceof Error ? error.name : 'Error' }))
        try { await options.onError?.(error, record, event) } catch { /* diagnostics must not acknowledge a failed record */ }
        const sequence = record.dynamodb?.SequenceNumber
        if (!sequence) throw error // Lambda retries the entire batch.
        return { batchItemFailures: [{ itemIdentifier: sequence }] }
      }
    }
    return { batchItemFailures: [] }
  }
}
