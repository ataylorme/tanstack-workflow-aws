import { describe, expect, it, vi, afterEach } from 'vitest'
import { marshall } from '@aws-sdk/util-dynamodb'
import { GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb'
import { ApplicationEventConflictError, createDynamoApplicationEventPublisher, routeApplicationEvent } from '../src/events.js'
import { createApplicationStreamHandler, decodeApplicationEvent } from '../src/event-stream.js'
import { createWebhookBridge } from '../src/bridges-webhook.js'
import { createSqsBridge } from '../src/bridges-sqs.js'

const event = { id: 'test-1', type: 'test.requested', version: 1, timestamp: '2026-09-27T15:00:00.000Z', data: { message: 'hello' } }
const record = (id: string, value: unknown = event) => ({ eventName: 'INSERT', dynamodb: { SequenceNumber: id, NewImage: marshall({ entityType: 'APPLICATION_EVENT', event: value }) } })
afterEach(() => vi.restoreAllMocks())

describe('publication retry guarantees', () => {
  it.each(['ConditionalCheckFailedException', 'TimeoutError'])('reconciles %s using a strong read and retains the original timestamp', async name => {
    const send = vi.fn(async (command: unknown) => {
      if (command instanceof PutCommand) throw Object.assign(new Error('write response unavailable'), { name })
      expect(command).toBeInstanceOf(GetCommand)
      expect((command as GetCommand).input.ConsistentRead).toBe(true)
      return { Item: { entityType: 'APPLICATION_EVENT', event } }
    })
    const publisher = createDynamoApplicationEventPublisher({ tableName: 'events', client: { send } as any })
    const { timestamp: _, ...input } = event
    expect(await publisher.publish(input)).toEqual(event)
  })
  it('rejects an ID reused with a different payload or explicit timestamp', async () => {
    const send = vi.fn(async (command: unknown) => {
      if (command instanceof PutCommand) throw Object.assign(new Error('exists'), { name: 'ConditionalCheckFailedException' })
      return { Item: { entityType: 'APPLICATION_EVENT', event } }
    })
    const publisher = createDynamoApplicationEventPublisher({ tableName: 'events', client: { send } as any })
    await expect(publisher.publish({ ...event, data: { message: 'different' } })).rejects.toBeInstanceOf(ApplicationEventConflictError)
    await expect(publisher.publish({ ...event, timestamp: '2026-09-28T15:00:00.000Z' })).rejects.toBeInstanceOf(ApplicationEventConflictError)
  })
  it('preserves the original failure if no committed item can be found', async () => {
    const failure = new Error('access denied')
    const send = vi.fn(async (command: unknown) => { if (command instanceof PutCommand) throw failure; return {} })
    const publisher = createDynamoApplicationEventPublisher({ tableName: 'events', client: { send } as any })
    await expect(publisher.publish(event)).rejects.toBe(failure)
  })
  it('retries a transient MRSC conflict with the same envelope', async () => {
    let attempts = 0
    const send = vi.fn(async (command: unknown) => {
      if (command instanceof PutCommand && attempts++ === 0) throw Object.assign(new Error('conflict'), { name: 'ReplicatedWriteConflictException' })
      return {}
    })
    expect(await createDynamoApplicationEventPublisher({ tableName: 'events', client: { send } as any }).publish(event)).toEqual(event)
    expect(attempts).toBe(2)
  })
  it.each([undefined, NaN, Infinity, BigInt(1), new Date(), new Map(), { value: undefined }, [undefined], 'x'.repeat(250 * 1024)])('rejects nonportable/oversized JSON before persistence %#', async data => {
    const send = vi.fn()
    const publisher = createDynamoApplicationEventPublisher({ tableName: 'events', client: { send } as any })
    await expect(publisher.publish({ ...event, data })).rejects.toThrow()
    expect(send).not.toHaveBeenCalled()
  })
  it('routes synchronous exceptions as rejections without preventing another consumer from starting', async () => {
    const other = vi.fn()
    await expect(routeApplicationEvent(event, [
      { type: event.type, handler: () => { throw new Error('failed') } },
      { type: event.type, handler: other },
    ])).rejects.toThrow('failed')
    expect(other).toHaveBeenCalledOnce()
  })
})

describe('stream failure boundaries', () => {
  it('returns the successful prefix checkpoint and does not start later records', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const handler = vi.fn(async value => { if (value.id === 'bad') throw new Error('down') })
    const consume = createApplicationStreamHandler(handler)
    expect(await consume({ Records: [record('1'), record('2', { ...event, id: 'bad' }), record('3')] })).toEqual({ batchItemFailures: [{ itemIdentifier: '2' }] })
    expect(handler).toHaveBeenCalledTimes(2)
  })
  it('reports malformed application items, but ignores unrelated records before decoding', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const handler = vi.fn()
    const diagnostic = vi.fn(() => { throw new Error('logger failure') })
    const consume = createApplicationStreamHandler(handler, { onError: diagnostic })
    expect(await consume({ Records: [record('4', { ...event, version: 0 })] })).toEqual({ batchItemFailures: [{ itemIdentifier: '4' }] })
    expect(diagnostic).toHaveBeenCalledOnce()
    expect(handler).not.toHaveBeenCalled()
    expect(decodeApplicationEvent({ eventName: 'INSERT', dynamodb: { NewImage: { bad: {} } } })).toBeUndefined()
  })
  it('throws on a failed record without a sequence so Lambda retries the batch', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const consume = createApplicationStreamHandler(() => { throw new Error('delivery failed') })
    const item = record('1'); delete (item.dynamodb as { SequenceNumber?: string }).SequenceNumber
    await expect(consume({ Records: [item] })).rejects.toThrow('delivery failed')
  })
  it('does not begin a delivery when the Lambda has insufficient time', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const handler = vi.fn()
    const consume = createApplicationStreamHandler(handler, { minRemainingTimeMs: 15_000 })
    expect(await consume({ Records: [record('5')] }, { getRemainingTimeInMillis: () => 5000 })).toEqual({ batchItemFailures: [{ itemIdentifier: '5' }] })
    expect(handler).not.toHaveBeenCalled()
  })
})

describe('bridge failure boundaries', () => {
  it('releases webhook bodies and propagates network failures', async () => {
    const cancel = vi.fn(async () => {})
    const fetch = vi.fn(async () => ({ ok: true, status: 204, body: { cancel } }))
    await createWebhookBridge({ url: 'https://example.test', fetch: fetch as any })(event)
    expect(cancel).toHaveBeenCalledOnce()
    const failedFetch = vi.fn(async () => { throw new Error('aborted') })
    await expect(createWebhookBridge({ url: 'https://example.test', fetch: failedFetch as any })(event)).rejects.toThrow('aborted')
    expect((failedFetch.mock.calls as any)[0][1].signal).toBeInstanceOf(AbortSignal)
  })
  it('requires a FIFO group ID before sending', () => {
    expect(() => createSqsBridge({ queueUrl: 'https://sqs.us-east-1.amazonaws.com/123/test.fifo' })).toThrow('messageGroupId')
  })
})

describe('live runner safety', () => {
  it('plans without credentials, SDK imports or external executables', async () => {
    const { execFileSync } = await import('node:child_process')
    const output = execFileSync(process.execPath, ['scripts/test-events-live.mjs'], { env: { PATH: '' }, encoding: 'utf8' })
    expect(output).toContain('Plan only')
    expect(output).toContain('No resources are created or deleted')
  })
})
