import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdirSync, writeFileSync, readFileSync } from 'node:fs'
import { setTimeout } from 'node:timers/promises'

// Plan mode deliberately does not load AWS clients, resolve credentials, or make calls.
if (!process.argv.includes('--execute')) {
  console.log('Plan only: verify MRSC + streams, publish bounded test events in east/west, reconcile duplicate IDs and reject conflicts. Optionally verify delivery to an isolated SQS test queue. Set EVENT_TEST_TABLE and pass --execute. No resources are created or deleted; event items are retained.')
  process.exit(0)
}
const tableName = process.env.EVENT_TEST_TABLE
assert.match(tableName ?? '', /^[a-zA-Z0-9_.-]{3,255}$/, 'Set EVENT_TEST_TABLE to the isolated MRSC test table')
const regions = ['us-east-1', 'us-west-2']
const runId = `test-${randomUUID()}`
const expected = new Map()
const outcomes = []
const packageVersion = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')).version
const queueUrl = process.env.EVENT_TEST_QUEUE_URL
const queueRegion = process.env.EVENT_TEST_QUEUE_REGION ?? 'us-east-1'
assert.ok(regions.includes(queueRegion), 'EVENT_TEST_QUEUE_REGION must be us-east-1 or us-west-2')
const { DynamoDBClient, DescribeTableCommand } = await import('@aws-sdk/client-dynamodb')
const { DynamoDBDocumentClient, GetCommand } = await import('@aws-sdk/lib-dynamodb')
const { createDynamoApplicationEventPublisher, ApplicationEventConflictError } = await import('../dist/events.js')
const clients = regions.map(region => new DynamoDBClient({ region, maxAttempts: 3, requestHandler: { connectionTimeout: 3000, requestTimeout: 10000, throwOnRequestTimeout: true } }))
const docs = clients.map(client => DynamoDBDocumentClient.from(client, { marshallOptions: { removeUndefinedValues: true } }))
let queueClient
try {
  const descriptions = await Promise.all(clients.map(client => client.send(new DescribeTableCommand({ TableName: tableName }))))
  const accounts = []
  for (const { Table: table } of descriptions) {
    assert.equal(table?.TableStatus, 'ACTIVE')
    assert.equal(table?.MultiRegionConsistency, 'STRONG', 'Use MRSC; an MREC table is not a coordination substitute')
    assert.equal(table?.StreamSpecification?.StreamViewType, 'NEW_AND_OLD_IMAGES', 'Enable old and new images for the unified router')
    assert.ok(table?.LatestStreamArn, 'Missing regional stream ARN')
    assert.equal(table?.TableName, tableName)
    accounts.push(table.TableArn.split(':')[4])
  }
  assert.equal(accounts[0], accounts[1], 'Regional clients must use the same AWS account')
  outcomes.push({ scenario: 'MRSC and stream preflight', passed: true })
  // Resolve optional client before writing so a missing peer dependency fails early.
  const sqs = queueUrl ? await import('@aws-sdk/client-sqs') : undefined
  if (sqs) queueClient = new sqs.SQSClient({ region: queueRegion, maxAttempts: 3, requestHandler: { connectionTimeout: 3000, requestTimeout: 15000, throwOnRequestTimeout: true } })
  const publishers = docs.map(client => createDynamoApplicationEventPublisher({ tableName, client }))
  for (let index = 0; index < regions.length; index++) {
    const event = await publishers[index].publish({ id: `${runId}-${regions[index]}`, type: 'integration.event', data: { runId, producerRegion: regions[index] } })
    expected.set(event.id, event)
  }
  const input = { id: `${runId}-concurrent`, type: 'integration.event', data: { runId, purpose: 'duplicate reconciliation' } }
  const copies = await Promise.all(publishers.map(publisher => publisher.publish(input)))
  assert.deepEqual(copies[0], copies[1], 'Concurrent publishers must return one stored envelope')
  expected.set(copies[0].id, copies[0])
  assert.deepEqual(await publishers[1].publish(input), copies[0], 'A later retry must retain the original timestamp')
  await assert.rejects(publishers[0].publish({ ...input, data: { runId, purpose: 'ID conflict' } }), ApplicationEventConflictError)
  for (const client of docs) {
    for (const event of expected.values()) {
      const saved = await client.send(new GetCommand({ TableName: tableName, Key: { PK: `EVENT#${event.id}`, SK: 'META' }, ConsistentRead: true }))
      assert.deepEqual(saved.Item?.event, event)
    }
  }
  outcomes.push({ scenario: 'regional publishing, concurrent retries, conflict and strong reads', passed: true })
  if (sqs && queueClient) {
    const received = new Set()
    const deadline = Date.now() + 180_000
    while (Date.now() < deadline && received.size < expected.size) {
      const batch = await queueClient.send(new sqs.ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 5, VisibilityTimeout: 10 }))
      for (const message of batch.Messages ?? []) {
        let event
        try { event = JSON.parse(message.Body ?? '') } catch { continue }
        if (!expected.has(event?.id)) continue // Never delete another run's message.
        assert.deepEqual(event, expected.get(event.id), 'Bridge must preserve the complete envelope')
        received.add(event.id)
        await queueClient.send(new sqs.DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }))
      }
      if (!batch.Messages?.length) await setTimeout(250)
    }
    assert.equal(received.size, expected.size, 'Stream-to-SQS delivery timed out; inspect mapping, role, logs, and failure archive')
    outcomes.push({ scenario: 'stream-to-SQS full envelope delivery', passed: true, uniqueEvents: received.size })
  }
} catch (error) {
  outcomes.push({ scenario: 'failure', passed: false, error: error instanceof Error ? error.message : String(error) })
  process.exitCode = 1
} finally {
  clients.forEach(client => client.destroy())
  queueClient?.destroy()
  mkdirSync('.event-test-results', { recursive: true, mode: 0o700 })
  const report = `.event-test-results/${runId}.json`
  writeFileSync(report, JSON.stringify({ runId, regions, packageVersion, testedAt: new Date().toISOString(), eventIds: [...expected.keys()], queueChecked: !!queueUrl, outcomes }, null, 2), { mode: 0o600 })
  console.log(JSON.stringify({ report, passed: outcomes.every(item => item.passed), outcomes }, null, 2))
}
