import { createHash } from 'node:crypto'
import { describe, expect, it, vi } from 'vitest'
import { marshall } from '@aws-sdk/util-dynamodb'
import { PutEventsCommand } from '@aws-sdk/client-eventbridge'
import { PublishCommand } from '@aws-sdk/client-sns'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import { createEventBridgeBridge } from '../src/bridges-eventbridge.js'
import { createSnsBridge } from '../src/bridges-sns.js'
import { createSqsBridge } from '../src/bridges-sqs.js'
import { createWebhookBridge } from '../src/bridges-webhook.js'
import { createApplicationStreamHandler, decodeApplicationEvent } from '../src/event-stream.js'

const event = { id: 'stable-id', type: 'task.completed', version: 2, timestamp: '2026-09-27T15:00:00.000Z', data: { taskId: 'task-1' } }
const record = (sequence: string, type = 'APPLICATION_EVENT') => ({ eventName: 'INSERT', dynamodb: { SequenceNumber: sequence, NewImage: marshall({ entityType: type, event }) } })

describe('optional bridges', () => {
  it('maps the complete envelope to EventBridge and checks per-entry failures', async () => {
    const send = vi.fn(async (_command: unknown): Promise<any> => ({ Entries: [{ EventId: 'aws-id' }], FailedEntryCount: 0 }))
    const bridge = createEventBridgeBridge({ eventBusName: 'bus', source: 'my.app', client: { send } as any })
    await bridge(event)
    expect(send.mock.calls[0][0]).toBeInstanceOf(PutEventsCommand)
    expect((send.mock.calls[0][0] as PutEventsCommand).input.Entries?.[0]).toMatchObject({ DetailType: 'task.completed.v2', Detail: JSON.stringify(event) })
    send.mockImplementationOnce(async () => ({ Entries: [{ ErrorCode: 'InternalFailure' }], FailedEntryCount: 1 }))
    await expect(bridge(event)).rejects.toThrow('InternalFailure')
  })

  it('preserves stable ID for FIFO SNS and SQS targets', async () => {
    const snsSend = vi.fn(async (_command: unknown) => ({}))
    const sqsSend = vi.fn(async (_command: unknown) => ({}))
    await createSnsBridge({ topicArn: 'topic.fifo', messageGroupId: () => 'task-1', client: { send: snsSend } as any })(event)
    await createSqsBridge({ queueUrl: 'queue.fifo', messageGroupId: () => 'task-1', client: { send: sqsSend } as any })(event)
    expect(snsSend.mock.calls[0][0]).toBeInstanceOf(PublishCommand)
    expect((snsSend.mock.calls[0][0] as PublishCommand).input).toMatchObject({ MessageDeduplicationId: createHash('sha256').update(event.id).digest('hex'), MessageGroupId: 'task-1', Message: JSON.stringify(event) })
    expect(sqsSend.mock.calls[0][0]).toBeInstanceOf(SendMessageCommand)
    expect((sqsSend.mock.calls[0][0] as SendMessageCommand).input).toMatchObject({ MessageDeduplicationId: createHash('sha256').update(event.id).digest('hex'), MessageGroupId: 'task-1', MessageBody: JSON.stringify(event) })
  })

  it('POSTs HTTPS with an idempotency header and fails on non-2xx', async () => {
    const fetch = vi.fn(async () => ({ ok: false, status: 503 })) as any
    const bridge = createWebhookBridge({ url: 'https://example.com/events', fetch })
    await expect(bridge(event)).rejects.toThrow('503')
    expect(fetch.mock.calls[0][1]).toMatchObject({ method: 'POST', redirect: 'error', body: JSON.stringify(event) })
    expect(fetch.mock.calls[0][1].headers.get('x-event-id')).toBe('stable-id')
    expect(() => createWebhookBridge({ url: 'http://example.com/events' })).toThrow('HTTPS')
  })
})

describe('DynamoDB stream adapter', () => {
  it('decodes only inserted application events', () => {
    expect(decodeApplicationEvent(record('1'))).toEqual(event)
    expect(decodeApplicationEvent(record('2', 'WORKFLOW'))).toBeUndefined()
    expect(decodeApplicationEvent({ ...record('3'), eventName: 'MODIFY' })).toBeUndefined()
  })

  it('returns first failed sequence while preserving ordering and retries', async () => {
    const send = vi.fn(async (value: any) => { if (value.id === event.id) throw new Error('down') })
    const handler = createApplicationStreamHandler(send)
    const result = await handler({ Records: [record('1', 'WORKFLOW'), record('2'), record('3')] })
    expect(result).toEqual({ batchItemFailures: [{ itemIdentifier: '2' }] })
    expect(send).toHaveBeenCalledTimes(1)
  })
})
