import { randomUUID } from 'node:crypto'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb'

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

      await client.send(
        new PutCommand({
          TableName,
          Item: {
            PK: `${prefix}#${event.id}`,
            SK: 'META',
            entityType: 'APPLICATION_EVENT',
            event,
            eventType: event.type,
            eventVersion: event.version,
            createdAt: event.timestamp,
          },
          ConditionExpression: 'attribute_not_exists(PK)',
        }),
      )

      return structuredClone(event)
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
      .map(route => Promise.resolve(route.handler(event))),
  )
}
