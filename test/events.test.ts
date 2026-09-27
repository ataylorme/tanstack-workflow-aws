import { describe, expect, it, vi } from 'vitest'
import { PutCommand } from '@aws-sdk/lib-dynamodb'
import {
  createDynamoApplicationEventPublisher,
  routeApplicationEvent,
} from '../src/events.js'

describe('application events', () => {
  it('publishes generic typed events into DynamoDB with an idempotent create', async () => {
    const send = vi.fn(async (command: any) => {
      expect(command).toBeInstanceOf(PutCommand)
      expect(command.input.ConditionExpression).toBe('attribute_not_exists(PK)')
      expect(command.input.Item).toMatchObject({
        entityType: 'APPLICATION_EVENT',
        eventType: 'task.requested',
        eventVersion: 2,
      })
      return {}
    })

    const publisher = createDynamoApplicationEventPublisher({
      tableName: 'events',
      client: { send } as any,
    })

    const event = await publisher.publish({
      id: 'evt-1',
      type: 'task.requested',
      version: 2,
      timestamp: '2026-09-27T15:00:00.000Z',
      data: { taskId: 'task-1', requestedBy: 'user-1' },
      correlationId: 'corr-1',
    })

    expect(event).toEqual({
      id: 'evt-1',
      type: 'task.requested',
      version: 2,
      timestamp: '2026-09-27T15:00:00.000Z',
      data: { taskId: 'task-1', requestedBy: 'user-1' },
      correlationId: 'corr-1',
      causationId: undefined,
      metadata: undefined,
    })
    expect(send).toHaveBeenCalledTimes(1)
  })

  it('routes an application event to every matching consumer', async () => {
    const a = vi.fn()
    const b = vi.fn()
    const c = vi.fn()

    await routeApplicationEvent(
      {
        id: 'evt-1',
        type: 'task.completed',
        version: 1,
        timestamp: '2026-09-27T15:00:00.000Z',
        data: { taskId: 'task-1' },
      },
      [
        { type: 'task.completed', handler: a },
        { type: 'task.completed', handler: b },
        { type: 'task.started', handler: c },
      ],
    )

    expect(a).toHaveBeenCalledTimes(1)
    expect(b).toHaveBeenCalledTimes(1)
    expect(c).not.toHaveBeenCalled()
  })
})
