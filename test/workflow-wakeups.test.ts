import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { SchedulerClient } from '@aws-sdk/client-scheduler'
import { SQSClient } from '@aws-sdk/client-sqs'
import type { DynamoDBStreamEvent } from 'aws-lambda'
import { createWakeupIO, dispatchKey, dueKinds, dueWakeup, parseKey, parseWakeup, processWakeup, scheduleInput, workTarget, type DueWakeup, type WakeupIO } from '../examples/wakeups.js'
import { dispatchStream } from '../examples/dispatcher.js'

const key = { PK: 'RUN#fixture', SK: 'META' }
const wakeup: DueWakeup = { version: 1, kind: 'due', key, dueKind: 'RUNNING', dueAt: 100 }
const config = { tableName: 'fixture-table', queueUrl: 'fixture-queue', queueArn: 'fixture-target', group: 'fixture-group', roleArn: 'fixture-role', dlqArn: 'fixture-failure' }
function item(extra: Record<string, unknown> = {}) { return { ...key, duePK: 'RUNNING', dueSK: 100, run: { status: 'running' }, ...extra } }
function io(current: Record<string, unknown> | undefined = item()): WakeupIO {
  return { read: vi.fn().mockResolvedValue(current), enqueue: vi.fn().mockResolvedValue(undefined), schedule: vi.fn().mockResolvedValue(undefined) }
}
beforeEach(() => {
  // Never let unit tests fall through to authenticated AWS clients.
  vi.spyOn(SQSClient.prototype, 'send').mockRejectedValue(new Error('Unexpected SQS call') as never)
  vi.spyOn(SchedulerClient.prototype, 'send').mockRejectedValue(new Error('Unexpected Scheduler call') as never)
  vi.spyOn(DynamoDBDocumentClient.prototype, 'send').mockRejectedValue(new Error('Unexpected DynamoDB call') as never)
})
afterEach(() => vi.restoreAllMocks())

describe('durable wakeup contracts', () => {
  it.each(dueKinds)('recognizes %s metadata', duePK => {
    expect(dueWakeup(item({ duePK }))).toMatchObject({ dueKind: duePK, dueAt: 100 })
  })
  it.each([
    ['RUNNING', { run: { status: 'running', lease: { expiresAt: 900 } } }],
    ['TIMER_RUN', { timerLease: { expiresAt: 900 } }],
    ['TIMER', { lease: { expiresAt: 900 } }],
    ['SCHEDULE', { pendingBucket: { lease: { expiresAt: 900 } } }],
  ])('respects %s lease even when due index is earlier', (duePK, extra) => {
    expect(dueWakeup(item({ duePK, ...extra }))?.dueAt).toBe(900)
  })
  it.each(['finished', 'errored', 'aborted'])('ignores terminal %s metadata', status => {
    expect(dueWakeup(item({ run: { status } }))).toBeUndefined()
  })
  it('ignores manual waits, absent records, deleted records, and invalid due fields', () => {
    for (const current of [undefined, { ...key, run: { status: 'waiting' } }, item({ deleted: true }), item({ duePK: 'EVENT' }), item({ dueSK: NaN }), item({ dueSK: -1 })]) {
      expect(dueWakeup(current)).toBeUndefined()
    }
  })
  it('validates envelope version, kind, key, and finite deadline', () => {
    expect(parseWakeup(wakeup)).toEqual(wakeup)
    expect(parseWakeup({ version: 1, kind: 'drain' })).toEqual({ version: 1, kind: 'drain' })
    for (const bad of [null, { ...wakeup, version: 2 }, { ...wakeup, dueKind: 'EVENT' }, { ...wakeup, dueAt: Infinity }, { ...wakeup, dueAt: -1 }, { ...wakeup, key: { ...key, SK: 'EVENT#1' } }]) expect(() => parseWakeup(bad)).toThrow()
    expect(() => parseKey({ PK: 'APPLICATION#event', SK: 'META' })).toThrow()
  })
  it('schedules latest authoritative state instead of old stream deadlines', async () => {
    const services = io(item({ dueSK: 500 }))
    await dispatchKey(key, services, 100)
    expect(services.read).toHaveBeenCalledWith(key)
    expect(services.schedule).toHaveBeenCalledWith({ ...wakeup, dueAt: 500 }, 500)
    expect(services.enqueue).not.toHaveBeenCalled()
  })
  it('enqueues overdue work and leaves inactive work alone', async () => {
    const services = io()
    await dispatchKey(key, services, 100)
    expect(services.enqueue).toHaveBeenCalledWith(wakeup)
    const inactive = io(item({ duePK: undefined }))
    await dispatchKey(key, inactive, 100)
    expect(inactive.enqueue).not.toHaveBeenCalled()
    expect(inactive.schedule).not.toHaveBeenCalled()
  })
  it('defers stale deliveries until the current lease expires', async () => {
    const services = io(item({ run: { status: 'running', lease: { expiresAt: 1000 } } }))
    const handlers = { processTarget: vi.fn(), sweepLegacy: vi.fn() }
    await processWakeup(wakeup, services, handlers, () => 200)
    expect(handlers.processTarget).not.toHaveBeenCalled()
    expect(handlers.sweepLegacy).not.toHaveBeenCalled()
    expect(services.schedule).toHaveBeenCalledWith({ ...wakeup, dueAt: 1000 }, 1000)
  })
  it('processes only the named target and persists a successor for unresolved work', async () => {
    const services = io()
    const handlers = { processTarget: vi.fn(), sweepLegacy: vi.fn() }
    await processWakeup(wakeup, services, handlers, () => 200)
    expect(handlers.processTarget).toHaveBeenCalledExactlyOnceWith({ kind: 'run', runId: 'fixture' })
    expect(handlers.sweepLegacy).not.toHaveBeenCalled()
    expect(services.enqueue).not.toHaveBeenCalled()
    expect(services.read).toHaveBeenCalledTimes(2)
    expect(services.schedule).toHaveBeenCalledWith(wakeup, 1200)
  })
  it('continues old drain batches without making new keyed work use discovery', async () => {
    const services = io()
    const handlers = { processTarget: vi.fn(), sweepLegacy: vi.fn().mockResolvedValue({ remainingMayExist: true }) }
    await processWakeup({ version: 1, kind: 'drain' }, services, handlers, () => 200)
    expect(services.enqueue).toHaveBeenCalledWith({ version: 1, kind: 'drain' }, 1)
    expect(handlers.processTarget).not.toHaveBeenCalled()
    expect(services.read).not.toHaveBeenCalled()
  })
  it('does not acknowledge until the targeted successor is durable', async () => {
    const services = io()
    vi.mocked(services.schedule).mockRejectedValue(new Error('scheduler unavailable'))
    await expect(processWakeup(wakeup, services, { processTarget: vi.fn(), sweepLegacy: vi.fn() }, () => 200)).rejects.toThrow('scheduler unavailable')
  })
  it('does not acknowledge failed execution or the post-execution authoritative read', async () => {
    const services = io()
    await expect(processWakeup(wakeup, services, { processTarget: vi.fn().mockRejectedValue(new Error('claim failed')), sweepLegacy: vi.fn() })).rejects.toThrow('claim failed')
    vi.mocked(services.read).mockResolvedValueOnce(item()).mockRejectedValueOnce(new Error('read failed'))
    await expect(processWakeup(wakeup, services, { processTarget: vi.fn(), sweepLegacy: vi.fn() })).rejects.toThrow('read failed')
  })
  it('uses the current category and carries a subsequent category change into the successor', async () => {
    const services = io()
    vi.mocked(services.read).mockResolvedValueOnce(item({ duePK: 'TIMER_RUN' })).mockResolvedValueOnce(item({ dueSK: 5000 }))
    const handlers = { processTarget: vi.fn(), sweepLegacy: vi.fn() }
    await processWakeup(wakeup, services, handlers, () => 200)
    expect(handlers.processTarget).toHaveBeenCalledExactlyOnceWith({ kind: 'timer', runId: 'fixture' })
    expect(services.schedule).toHaveBeenCalledWith({ ...wakeup, dueAt: 5000 }, 5000)
    expect(handlers.sweepLegacy).not.toHaveBeenCalled()
  })
  it('does not reschedule completed work or execute obsolete wakeups', async () => {
    const services = io()
    vi.mocked(services.read).mockResolvedValueOnce(item()).mockResolvedValue(undefined)
    const handlers = { processTarget: vi.fn(), sweepLegacy: vi.fn() }
    await processWakeup(wakeup, services, handlers, () => 200)
    await processWakeup(wakeup, services, handlers, () => 200)
    expect(handlers.processTarget).toHaveBeenCalledTimes(1)
    expect(services.schedule).not.toHaveBeenCalled()
  })
  it('maps schedule and encoded standalone timer keys without confusing IDs', () => {
    expect(workTarget({ ...wakeup, dueKind: 'SCHEDULE', key: { PK: 'SCHEDULE#daily#1', SK: 'META' } })).toEqual({ kind: 'schedule', scheduleId: 'daily#1' })
    expect(workTarget({ ...wakeup, dueKind: 'TIMER', key: { PK: 'TIMER#run%23x#timer%3Ax%23y', SK: 'META' } })).toEqual({ kind: 'timer', runId: 'run#x', signalId: 'timer:x#y' })
    expect(() => workTarget({ ...wakeup, dueKind: 'SCHEDULE' })).toThrow('does not match')
    expect(() => workTarget({ ...wakeup, dueKind: 'TIMER', key: { PK: 'TIMER#a#b#c', SK: 'META' } })).toThrow('does not match')
  })

})

describe('scheduler adapter', () => {
  it('uses deterministic, distinct, automatically deleted UTC schedules', () => {
    const input = scheduleInput(config, wakeup, 1501)
    expect(input).toEqual(scheduleInput(config, { ...wakeup }, 1501))
    expect(input.Name).not.toBe(scheduleInput(config, wakeup, 2501).Name)
    expect(input.Name.length).toBeLessThanOrEqual(64)
    expect(input).toMatchObject({ ScheduleExpression: 'at(1970-01-01T00:00:02)', ScheduleExpressionTimezone: 'UTC', ActionAfterCompletion: 'DELETE', FlexibleTimeWindow: { Mode: 'OFF' } })
    expect(JSON.parse(input.Target.Input)).toEqual(wakeup)
  })
  it('requests strongly consistent base-table reads', async () => {
    const send = vi.spyOn(DynamoDBDocumentClient.prototype, 'send').mockResolvedValue({ Item: item() } as never)
    expect(await createWakeupIO(config).read(key)).toEqual(item())
    expect((send.mock.calls[0]?.[0] as { input: unknown }).input).toEqual({ TableName: config.tableName, Key: key, ConsistentRead: true })
  })
  it('accepts matching schedule conflicts but rejects mismatches', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(0)
    const conflict = Object.assign(new Error('existing'), { name: 'ConflictException' })
    const expected = scheduleInput(config, wakeup, 60_000)
    const send = vi.spyOn(SchedulerClient.prototype, 'send').mockRejectedValueOnce(conflict as never).mockResolvedValueOnce(expected as never)
    await expect(createWakeupIO(config).schedule(wakeup, 60_000)).resolves.toBeUndefined()
    send.mockRejectedValueOnce(conflict as never).mockResolvedValueOnce({ ...expected, State: 'DISABLED' } as never)
    await expect(createWakeupIO(config).schedule(wakeup, 60_000)).rejects.toThrow('Conflicting')
  })
  it('propagates failed creation and conflict lookup races for retry', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(0)
    const send = vi.spyOn(SchedulerClient.prototype, 'send').mockRejectedValueOnce(new Error('denied') as never)
    await expect(createWakeupIO(config).schedule(wakeup, 60_000)).rejects.toThrow('denied')
    send.mockRejectedValueOnce(Object.assign(new Error('exists'), { name: 'ConflictException' }) as never).mockRejectedValueOnce(new Error('deleted during lookup') as never)
    await expect(createWakeupIO(config).schedule(wakeup, 60_000)).rejects.toThrow('deleted during lookup')
  })
  it.each([false, true])('persists a queue fallback if create/lookup finishes after deadline (conflict=%s)', async conflict => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(0)
    const expected = scheduleInput(config, wakeup, 60_000)
    const send = vi.spyOn(SchedulerClient.prototype, 'send')
    if (conflict) send.mockRejectedValueOnce(Object.assign(new Error('exists'), { name: 'ConflictException' }) as never)
    send.mockImplementationOnce(async () => { now.mockReturnValue(60_001); return expected })
    const queue = vi.spyOn(SQSClient.prototype, 'send').mockResolvedValue({} as never)
    await expect(createWakeupIO(config).schedule(wakeup, 60_000)).resolves.toBeUndefined()
    expect((queue.mock.calls[0]?.[0] as { input: unknown }).input).toMatchObject({ QueueUrl: config.queueUrl, MessageBody: JSON.stringify(wakeup) })
  })
  it('does not acknowledge an elapsed schedule when its queue fallback fails', async () => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(0)
    vi.spyOn(SchedulerClient.prototype, 'send').mockImplementationOnce(async () => { now.mockReturnValue(60_001); return {} })
    vi.spyOn(SQSClient.prototype, 'send').mockRejectedValue(new Error('fallback unavailable') as never)
    await expect(createWakeupIO(config).schedule(wakeup, 60_000)).rejects.toThrow('fallback unavailable')
  })
  it('uses delayed SQS for a deadline inside the schedule creation safety window', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(5000)
    const send = vi.spyOn(SQSClient.prototype, 'send').mockResolvedValue({} as never)
    await createWakeupIO(config).schedule(wakeup, 19_000)
    expect((send.mock.calls[0]?.[0] as { input: unknown }).input).toMatchObject({ DelaySeconds: 14 })
    expect(SchedulerClient.prototype.send).not.toHaveBeenCalled()
  })
  it('enqueues instead when a deadline elapses before schedule creation', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(5000)
    const send = vi.spyOn(SQSClient.prototype, 'send').mockResolvedValue({} as never)
    const schedule = vi.spyOn(SchedulerClient.prototype, 'send')
    await createWakeupIO(config).schedule(wakeup, 4000)
    expect((send.mock.calls[0]?.[0] as { input: unknown }).input).toMatchObject({ DelaySeconds: 0, MessageBody: JSON.stringify(wakeup) })
    expect(schedule).not.toHaveBeenCalled()
  })
})

function streamRecord(sequence: string): DynamoDBStreamEvent['Records'][number] {
  return { eventName: 'MODIFY', dynamodb: { SequenceNumber: sequence, Keys: { PK: { S: key.PK }, SK: { S: key.SK } }, NewImage: { duePK: { S: 'RUNNING' }, dueSK: { N: '100' } } } }
}
describe('stream partial batch processing', () => {
  it('reports only failures and continues processing subsequent records', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const services = io()
    vi.mocked(services.read).mockRejectedValueOnce(new Error('read failed')).mockResolvedValue(item())
    expect(await dispatchStream({ Records: [streamRecord('1'), streamRecord('2')] }, { getRemainingTimeInMillis: () => 60_000 }, services)).toEqual({ batchItemFailures: [{ itemIdentifier: '1' }] })
    expect(services.enqueue).toHaveBeenCalledTimes(1)
  })
  it('returns unprocessed records on exhausted budget without accessing state', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const services = io()
    expect(await dispatchStream({ Records: [streamRecord('1'), streamRecord('2')] }, { getRemainingTimeInMillis: () => 1000 }, services)).toEqual({ batchItemFailures: [{ itemIdentifier: '1' }, { itemIdentifier: '2' }] })
    expect(services.read).not.toHaveBeenCalled()
  })
  it('ignores removes and metadata without due fields', async () => {
    const services = io()
    const removed = { ...streamRecord('1'), eventName: 'REMOVE' as const }
    expect(await dispatchStream({ Records: [removed, { eventName: 'INSERT', dynamodb: { SequenceNumber: '2' } }] }, { getRemainingTimeInMillis: () => 60_000 }, services)).toEqual({ batchItemFailures: [] })
    expect(services.read).not.toHaveBeenCalled()
  })
})

describe('SQS sweeper validation and retry boundaries', () => {
  it('rejects events without an SQS batch', async () => {
    const { handler } = await import('../examples/sweeper.js')
    const context = { getRemainingTimeInMillis: () => 60_000 } as import('aws-lambda').Context
    await expect(handler({}, context)).rejects.toThrow('Expected an SQS wakeup batch')
  })
  it('returns malformed messages as partial failures without dropping siblings', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const { handler } = await import('../examples/sweeper.js')
    const context = { getRemainingTimeInMillis: () => 60_000 } as import('aws-lambda').Context
    expect(await handler({ Records: [
      { eventSource: 'aws:sqs', messageId: 'bad-json', body: '{' },
      { eventSource: 'aws:sqs', messageId: 'bad-envelope', body: JSON.stringify({ version: 99 }) },
      { eventSource: 'other', messageId: 'bad-source', body: JSON.stringify(wakeup) },
    ] }, context)).toEqual({ batchItemFailures: [
      { itemIdentifier: 'bad-json' }, { itemIdentifier: 'bad-envelope' }, { itemIdentifier: 'bad-source' },
    ] })
  })
  it('returns every unprocessed SQS message when the worker budget is exhausted', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const { handler } = await import('../examples/sweeper.js')
    const context = { getRemainingTimeInMillis: () => 1000 } as import('aws-lambda').Context
    expect(await handler({ Records: [
      { eventSource: 'aws:sqs', messageId: 'one', body: JSON.stringify(wakeup) },
      { eventSource: 'aws:sqs', messageId: 'two', body: JSON.stringify(wakeup) },
    ] }, context)).toEqual({ batchItemFailures: [{ itemIdentifier: 'one' }, { itemIdentifier: 'two' }] })
  })
})
