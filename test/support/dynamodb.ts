import { randomUUID } from 'node:crypto'
import { CreateTableCommand, DeleteTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { createDynamoWorkflowExecutionStore, type DynamoWorkflowStoreOptions } from '../../src/index.js'

/** Integration fixture intentionally targets only a local emulator; never creates/deletes AWS tables. */
export async function createTestStore(options: Pick<DynamoWorkflowStoreOptions, 'limits'> = {}) {
  const endpoint = process.env.DYNAMODB_ENDPOINT
  if (!endpoint || !['localhost', '127.0.0.1', '[::1]'].includes(new URL(endpoint).hostname)) {
    throw new Error('DYNAMODB_ENDPOINT must point to a local emulator')
  }
  const tableName = `workflow-test-${randomUUID()}`
  const raw = new DynamoDBClient({ endpoint, region: 'us-east-1', credentials: { accessKeyId: 'local', secretAccessKey: 'local' } })
  const client = DynamoDBDocumentClient.from(raw, { marshallOptions: { removeUndefinedValues: true } })
  await raw.send(new CreateTableCommand({
    TableName: tableName, BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: [{ AttributeName: 'PK', AttributeType: 'S' }, { AttributeName: 'SK', AttributeType: 'S' }, { AttributeName: 'duePK', AttributeType: 'S' }, { AttributeName: 'dueSK', AttributeType: 'N' }],
    KeySchema: [{ AttributeName: 'PK', KeyType: 'HASH' }, { AttributeName: 'SK', KeyType: 'RANGE' }],
    GlobalSecondaryIndexes: [{ IndexName: 'DueIndex', KeySchema: [{ AttributeName: 'duePK', KeyType: 'HASH' }, { AttributeName: 'dueSK', KeyType: 'RANGE' }], Projection: { ProjectionType: 'KEYS_ONLY' } }],
  }))
  const store = createDynamoWorkflowExecutionStore({ ...options, tableName, client })
  return { store, client, tableName, async cleanup() { try { await raw.send(new DeleteTableCommand({ TableName: tableName })) } finally { raw.destroy() } } }
}
