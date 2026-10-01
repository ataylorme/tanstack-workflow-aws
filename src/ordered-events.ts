import { createHash, randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb'
import type { ApplicationEvent, PublishApplicationEventOptions } from './events.js'
import { serializeApplicationEvent } from './event-validation.js'
import type { QueueBatch, WorkerContext } from './wakeups.js'

export interface EventOrdering { streamId: string; sequence: number }
export type OrderedApplicationEvent<T = unknown> = ApplicationEvent<T> & { ordering: EventOrdering }
export class EventSequenceError extends Error { override name = 'EventSequenceError' }
export class OrderedDeliveryBlockedError extends Error { override name = 'OrderedDeliveryBlockedError' }
const hash = (value: string) => createHash('sha256').update(value).digest('hex')
export const orderedStreamKey = (streamId: string) => `ORDER#${hash(streamId)}`
const subscriptionKey = (subscriberId: string, streamId: string) => `SUB#${hash(JSON.stringify([subscriberId, streamId]))}`
const eventKey = (sequence: number) => `EVENT#${String(sequence).padStart(16, '0')}`
const conflict = (error: unknown) => ['ConditionalCheckFailedException', 'ReplicatedWriteConflictException'].includes((error as Error)?.name)
interface Connection { tableName: string; client?: DynamoDBDocumentClient }
function database(options: Connection) {
  if (!options.tableName) throw new TypeError('tableName is required')
  const client = options.client ?? DynamoDBDocumentClient.from(new DynamoDBClient({ maxAttempts: 3,
    requestHandler: { connectionTimeout: 1000, requestTimeout: 4000, throwOnRequestTimeout: true } }),
  { marshallOptions: { removeUndefinedValues: true } })
  const read = async (PK: string, SK = 'META') => {
    const item = (await client.send(new GetCommand({ TableName: options.tableName, Key: { PK, SK }, ConsistentRead: true }))).Item
    if (item && item.schemaVersion !== 1) throw new Error('Unsupported ordered event storage version')
    return item
  }
  const put = async (item: Record<string, unknown>, version?: number) => client.send(new PutCommand({
    TableName: options.tableName, Item: item,
    ConditionExpression: version === undefined ? 'attribute_not_exists(PK)' : '#v = :v',
    ...(version === undefined ? {} : { ExpressionAttributeNames: { '#v': 'version' }, ExpressionAttributeValues: { ':v': version } }),
  }))
  return { read, put }
}

/** Sequence positions are immutable. Only the single head CAS makes a staged event visible. */
export function createDynamoOrderedEventPublisher(options: Connection & { eventTypes?: readonly string[]; maxEvents?: number }) {
  const db = database(options)
  const maxEvents = options.maxEvents ?? 10_000
  if (!Number.isSafeInteger(maxEvents) || maxEvents < 1 || maxEvents > 100_000) throw new TypeError('maxEvents must be 1..100000')
  if (options.eventTypes && (!options.eventTypes.length || options.eventTypes.length > maxEvents || Buffer.byteLength(JSON.stringify(options.eventTypes)) > 32 * 1024 || options.eventTypes.some(type => typeof type !== 'string' || !type || type.length > 100))) throw new TypeError('Invalid eventTypes')
  return {
    async publish<T>(input: PublishApplicationEventOptions<T>): Promise<OrderedApplicationEvent<T>> {
      if (!input.ordering) throw new TypeError('ordering is required')
      const proposed = JSON.parse(serializeApplicationEvent({ ...input,
        id: input.id ?? `ordered-${hash(JSON.stringify([input.ordering.streamId, input.ordering.sequence]))}`,
        version: input.version ?? 1, timestamp: input.timestamp ?? new Date().toISOString(),
      })) as OrderedApplicationEvent<T>
      const { streamId, sequence } = proposed.ordering
      const PK = orderedStreamKey(streamId)
      // Initialize a definition once. A crash before the first event is harmless.
      let head = await db.read(PK)
      if (!head) {
        try { await db.put({ PK, SK: 'META', schemaVersion: 1, entityType: 'ORDERED_STREAM', version: 0,
          streamId, committed: 0, maxEvents, ...(options.eventTypes ? { eventTypes: [...options.eventTypes] } : {}) }) }
        catch (error) { if (!conflict(error) && !(await db.read(PK))) throw error }
        head = await db.read(PK)
      }
      if (!head || head.streamId !== streamId) throw new Error('Invalid ordered stream')
      if (options.eventTypes && !isDeepStrictEqual(head.eventTypes, [...options.eventTypes])) throw new EventSequenceError('Stream eventTypes cannot change')
      if (sequence > head.maxEvents || (head.eventTypes && head.eventTypes[sequence - 1] !== proposed.type)) throw new EventSequenceError('Event exceeds the stream definition or has the wrong type for its sequence')
      if (sequence > head.committed + 1) throw new EventSequenceError(`Expected sequence ${head.committed + 1}`)
      // A single immutable slot arbitrates competing publishers before the head commit.
      let slot = await db.read(PK, eventKey(sequence))
      if (!slot) {
        try { await db.put({ PK, SK: eventKey(sequence), schemaVersion: 1, event: proposed }) }
        catch (error) { slot = await db.read(PK, eventKey(sequence)); if (!slot) throw error }
        slot ??= await db.read(PK, eventKey(sequence))
      }
      const stored = slot?.event as OrderedApplicationEvent<T> | undefined
      const expected = { ...proposed, ...(input.timestamp === undefined && stored ? { timestamp: stored.timestamp } : {}) }
      if (!stored || !isDeepStrictEqual(stored, expected)) throw new EventSequenceError('Sequence already contains a different event')
      for (let attempt = 0; attempt < 8; attempt++) {
        head = await db.read(PK)
        if (!head) throw new Error('Ordered stream disappeared')
        if (head.committed >= sequence) return structuredClone(stored)
        if (head.committed !== sequence - 1) throw new EventSequenceError('Preceding event has not committed')
        try {
          await db.put({ ...head, version: head.version + 1, committed: sequence, event: stored }, head.version)
          return structuredClone(stored)
        } catch (error) {
          const confirmed = await db.read(PK)
          if (confirmed?.committed >= sequence) return structuredClone(stored)
          if (!conflict(error)) throw error
        }
      }
      throw new Error('Ordered publication contention')
    },
    /** Retained source log supports explicit replay; staged records are never returned. */
    async read(streamId: string, sequence: number): Promise<OrderedApplicationEvent | undefined> {
      if (!Number.isSafeInteger(sequence) || sequence < 1) throw new TypeError('Invalid sequence')
      const PK = orderedStreamKey(streamId)
      const head = await db.read(PK)
      if (!head || head.committed < sequence) return undefined
      const event = (await db.read(PK, eventKey(sequence)))?.event as OrderedApplicationEvent | undefined
      if (!event || event.ordering?.streamId !== streamId || event.ordering.sequence !== sequence) throw new EventSequenceError('Committed event is missing or corrupt')
      serializeApplicationEvent(event)
      return event
    },
  }
}

export interface OrderedDeliveryContext { subscriberId: string; claimId: string; idempotencyKey: string }
export interface OrderedSubscriberOptions extends Connection {
  /** Same logical subscriber ID and table in every Region. */
  subscriberId: string
  handler(event: OrderedApplicationEvent, context: OrderedDeliveryContext): Promise<void>
  maxEventsPerInvocation?: number
  minRemainingTimeMs?: number
}

/**
 * A durable, non-expiring claim serializes effects across Regions. An uncertain
 * effect stops delivery; time alone never authorizes a second handler invocation.
 */
export function createDynamoOrderedSubscriber(options: OrderedSubscriberOptions) {
  if (!options.subscriberId || options.subscriberId.length > 256) throw new TypeError('subscriberId is required (max 256 characters)')
  const db = database(options)
  const source = createDynamoOrderedEventPublisher(options)
  const limit = options.maxEventsPerInvocation ?? 10
  const minTime = options.minRemainingTimeMs ?? 15_000
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100 || !Number.isFinite(minTime) || minTime < 0) throw new TypeError('Invalid subscriber budget')
  async function process(notification: ApplicationEvent, context?: Pick<WorkerContext, 'getRemainingTimeInMillis'>) {
    serializeApplicationEvent(notification)
    if (!notification.ordering) throw new TypeError('Ordered subscriber requires ordering')
    const { streamId, sequence: target } = notification.ordering
    // Queue bodies are wakeups, never the authoritative payload or a cursor override.
    if (!(await source.read(streamId, target))) throw new EventSequenceError('Notification references an uncommitted event')
    const PK = subscriptionKey(options.subscriberId, streamId)
    for (let processed = 0; processed < limit; processed++) {
      let cursor = await db.read(PK)
      if (!cursor) {
        try { await db.put({ PK, SK: 'META', schemaVersion: 1, version: 0,
          subscriberId: options.subscriberId, streamId, completed: 0 }) }
        catch (error) { if (!conflict(error) && !(await db.read(PK))) throw error }
        cursor = await db.read(PK)
      }
      if (!cursor || cursor.subscriberId !== options.subscriberId || cursor.streamId !== streamId) throw new Error('Invalid subscription cursor')
      if (cursor.completed >= target) return
      if (cursor.claim) throw new OrderedDeliveryBlockedError(`Unresolved delivery ${cursor.claim.id} at sequence ${cursor.completed + 1}`)
      if (context && context.getRemainingTimeInMillis() < minTime) throw new Error('Insufficient ordered subscriber budget')
      const event = await source.read(streamId, cursor.completed + 1)
      if (!event) throw new EventSequenceError('Missing committed predecessor')
      const claim = { id: randomUUID(), sequence: event.ordering.sequence, startedAt: Date.now() }
      const claimed = { ...cursor, version: cursor.version + 1, claim }
      try { await db.put(claimed, cursor.version) }
      catch (error) {
        // A lost claim response must never run an effect until ownership is proven.
        const confirmed = await db.read(PK)
        if (confirmed?.claim?.id !== claim.id) throw error
      }
      // On any failure the claim remains. Redelivery cannot overlap or bypass it.
      await options.handler(event, { subscriberId: options.subscriberId, claimId: claim.id,
        idempotencyKey: hash(JSON.stringify([options.subscriberId, streamId, claim.sequence])) })
      try { await db.put({ ...claimed, version: claimed.version + 1, completed: claim.sequence, claim: undefined }, claimed.version) }
      catch (error) {
        const confirmed = await db.read(PK)
        if (!confirmed || confirmed.completed < claim.sequence) throw error
        // The effect and acknowledgement both succeeded; retry is a duplicate.
      }
    }
    if ((await db.read(PK))?.completed < target) throw new Error('Ordered subscriber batch limit reached')
  }
  return {
    process,
    /** Inspect blocked claims before performing operator-coordinated recovery. */
    async inspect(streamId: string) { return db.read(subscriptionKey(options.subscriberId, streamId)) },
    /**
     * Operator only: first stop/fence the original invocation and settle its
     * external effect. Retry only if it did not occur; acknowledge only if complete.
     */
    async resolve(args: { streamId: string; claimId: string; outcome: 'retry' | 'acknowledge' }) {
      if (!['retry', 'acknowledge'].includes(args.outcome)) throw new TypeError('Invalid resolution')
      const PK = subscriptionKey(options.subscriberId, args.streamId)
      const cursor = await db.read(PK)
      if (!cursor?.claim || cursor.claim.id !== args.claimId) throw new OrderedDeliveryBlockedError('Claim changed or is already resolved')
      await db.put({ ...cursor, version: cursor.version + 1,
        completed: args.outcome === 'acknowledge' ? cursor.claim.sequence : cursor.completed, claim: undefined,
        resolution: { claimId: args.claimId, outcome: args.outcome, at: Date.now() } }, cursor.version)
    },
    async handler(batch: QueueBatch, context?: Pick<WorkerContext, 'getRemainingTimeInMillis'>) {
      for (let i = 0; i < batch.Records.length; i++) {
        const message = batch.Records[i]!
        try {
          if (message.eventSource !== 'aws:sqs') throw new TypeError('Expected SQS message')
          await process(JSON.parse(message.body), context)
        } catch (error) {
          console.error(JSON.stringify({ kind: 'ordered_delivery_blocked', subscriberId: options.subscriberId,
            messageId: message.messageId, error: error instanceof Error ? error.name : 'Error' }))
          // FIFO batches must not acknowledge later messages after a failure.
          return { batchItemFailures: batch.Records.slice(i).map(record => ({ itemIdentifier: record.messageId })) }
        }
      }
      return { batchItemFailures: [] }
    },
  }
}

export { applicationEventGroupId, applicationEventDeduplicationId } from './event-identity.js'
