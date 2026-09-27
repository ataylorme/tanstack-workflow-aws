import { describe, expect, it } from 'vitest'
import { GetCommand } from '@aws-sdk/lib-dynamodb'
import { createTestStore } from './support/dynamodb.js'
import { ApplicationEventConflictError, createDynamoApplicationEventPublisher } from '../src/events.js'

describe.skipIf(!process.env.DYNAMODB_ENDPOINT)('application event persistence (DynamoDB Local)', () => {
  it('reconciles concurrent duplicates without overwriting a committed event', async () => {
    const fixture = await createTestStore()
    try {
      const publisher = createDynamoApplicationEventPublisher(fixture)
      const input = { id: 'same-event', type: 'test.requested', data: { message: 'one fact' } }
      const results = await Promise.all(Array.from({ length: 8 }, () => publisher.publish(input)))
      for (const result of results) expect(result).toEqual(results[0])
      expect(await publisher.publish(input)).toEqual(results[0])
      await expect(publisher.publish({ ...input, data: { message: 'different fact' } })).rejects.toBeInstanceOf(ApplicationEventConflictError)
      const saved = await fixture.client.send(new GetCommand({ TableName: fixture.tableName, Key: { PK: 'EVENT#same-event', SK: 'META' }, ConsistentRead: true }))
      expect(saved.Item?.event).toEqual(results[0])
    } finally { await fixture.cleanup() }
  })
})
