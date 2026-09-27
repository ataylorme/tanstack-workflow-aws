import { randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { setTimeout } from 'node:timers/promises'
import { serializeApplicationEvent } from './event-validation.js'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb'

export interface ApplicationEvent<T = unknown> {
  id: string
  type: string
  version: number
  timestamp: string
  data: T
  correlationId?: string
  causationId?: string
  metadata?: Record<string, string>
}

export interface PublishApplicationEventOptions<T> {
  id?: string
  type: string
  version?: number
  timestamp?: string
  data: T
  correlationId?: string
  causationId?: string
  metadata?: Record<string, string>
}

export interface DynamoApplicationEventPublisherOptions {
  tableName: string
  client?: DynamoDBDocumentClient
  partitionKeyPrefix?: string
}

export interface ApplicationEventPublisher {
  publish<T>(event: PublishApplicationEventOptions<T>): Promise<ApplicationEvent<T>>
}

export class ApplicationEventConflictError extends Error {
  override name = 'ApplicationEventConflictError'
  constructor(id: string) { super(`Application event ID already contains different content: ${id}`) }
}

export function createDynamoApplicationEventPublisher(
  options: DynamoApplicationEventPublisherOptions,
): ApplicationEventPublisher {
  const client =
    options.client ??
    DynamoDBDocumentClient.from(new DynamoDBClient({}), {
      marshallOptions: { removeUndefinedValues: true },
    })
  const TableName = options.tableName
  const prefix = options.partitionKeyPrefix ?? 'EVENT'

  if (!TableName) throw new Error('tableName is required')
  if (!prefix || Buffer.byteLength(prefix) > 512) throw new Error('partitionKeyPrefix must be nonempty and at most 512 bytes')

  return {
    async publish<T>(input: PublishApplicationEventOptions<T>): Promise<ApplicationEvent<T>> {
      const event: ApplicationEvent<T> = {
        id: input.id ?? randomUUID(),
        type: input.type,
        version: input.version ?? 1,
        timestamp: input.timestamp ?? new Date().toISOString(),
        data: input.data,
        correlationId: input.correlationId,
        causationId: input.causationId,
        metadata: input.metadata,
      }

      // Snapshot before asynchronous I/O so caller mutation cannot alter a retry.
      const snapshot = JSON.parse(serializeApplicationEvent(event)) as ApplicationEvent<T>
      const timestampWasProvided = input.timestamp !== undefined
      const Key = { PK: `${prefix}#${snapshot.id}`, SK: 'META' }
      for (let attempt = 0; ; attempt++) {
        try {
          await client.send(new PutCommand({
            TableName,
            Item: { ...Key, entityType: 'APPLICATION_EVENT', event: snapshot,
              eventType: snapshot.type, eventVersion: snapshot.version, createdAt: snapshot.timestamp },
            ConditionExpression: 'attribute_not_exists(PK)',
          }))
          return snapshot
        } catch (error) {
          // Includes a response lost after commit. Strong reads are required for
          // reconciliation and are supported by MRSC in either replica Region.
          let existing: Record<string, any> | undefined
          try { existing = (await client.send(new GetCommand({ TableName, Key, ConsistentRead: true }))).Item }
          catch { throw error }
          if (existing) {
            const stored = existing.event as ApplicationEvent<T>
            const expected = { ...snapshot, ...(!timestampWasProvided ? { timestamp: stored?.timestamp } : {}) }
            if (existing.entityType !== 'APPLICATION_EVENT' || !isDeepStrictEqual(stored, expected)) {
              throw new ApplicationEventConflictError(snapshot.id)
            }
            return structuredClone(stored)
          }
          if ((error as { name?: string })?.name !== 'ReplicatedWriteConflictException' || attempt >= 4) throw error
          await setTimeout(25 * 2 ** attempt + Math.floor(Math.random() * 25))
        }
      }
    },
  }
}

export interface ApplicationEventHandler<T = unknown> {
  (event: ApplicationEvent<T>): Promise<void> | void
}

export interface ApplicationEventRoute<T = unknown> {
  type: string
  handler: ApplicationEventHandler<T>
}

export function routeApplicationEvent<T = unknown>(
  event: ApplicationEvent<T>,
  routes: readonly ApplicationEventRoute<any>[],
): Promise<void[]> {
  return Promise.all(
    routes
      .filter(route => route.type === event.type)
      .map(route => Promise.resolve().then(() => route.handler(event))),
  )
}
