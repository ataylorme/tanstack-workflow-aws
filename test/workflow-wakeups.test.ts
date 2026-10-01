import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import { SchedulerClient } from '@aws-sdk/client-scheduler'
import { SQSClient } from '@aws-sdk/client-sqs'
import { marshall } from '@aws-sdk/util-dynamodb'
import { createAwsWorkflowTransport, scheduleInput } from '../src/aws.js'
import { dispatchKey, dueWakeup, itemWakeups, parseWakeup, processWakeup, workTarget, createWorkflowStreamRouter, type DueWakeup, type WakeupIO, type StreamRecord } from '../src/wakeups.js'
const key = { PK: 'RUN#fixture', SK: 'META' }
const wakeup: DueWakeup = { version: 1, kind: 'due', key, dueKind: 'RUNNING', dueAt: 100 }
const config = { tableName: 'fixture-table', queueUrl: 'fixture-queue', queueArn: 'fixture-target', group: 'fixture-group', roleArn: 'fixture-role', dlqArn: 'fixture-failure' }
function item(extra: Record<string, unknown> = {}) { return { ...key, schemaVersion: 1, duePK: 'RUNNING', dueSK: 100, run: { status: 'running' }, ...extra } }
function io(current: Record<string, unknown> | undefined = item()): WakeupIO {
  return { read: vi.fn().mockResolvedValue(current), enqueue: vi.fn().mockResolvedValue(undefined), schedule: vi.fn().mockResolvedValue(undefined) }
}
const handlers = () => ({ processTarget: vi.fn(), drainEffects: vi.fn(), cleanupItem: vi.fn() })
beforeEach(() => {
  vi.spyOn(SQSClient.prototype, 'send').mockRejectedValue(new Error('Unexpected SQS call') as never)
  vi.spyOn(SchedulerClient.prototype, 'send').mockRejectedValue(new Error('Unexpected Scheduler call') as never)
  vi.spyOn(DynamoDBDocumentClient.prototype, 'send').mockRejectedValue(new Error('Unexpected DynamoDB call') as never)
})
afterEach(() => vi.restoreAllMocks())
it('validates metadata schema and wakeup envelope', () => {
  expect(parseWakeup(wakeup)).toEqual(wakeup)
  expect(() => parseWakeup({ ...wakeup, version: 9 })).toThrow()
  expect(() => itemWakeups(item({ schemaVersion: 9 }))).toThrow('storage version')
})
it('derives work, outbox and cleanup deadlines independently', () => {
  const tasks = itemWakeups(item({ outboxEnd: 10, outboxCursor: 5, cleanupAt: 999 }))
  expect(tasks.map(t => t.dueKind)).toEqual(['RUNNING', 'OUTBOX', 'CLEANUP'])
  expect(dueWakeup(item({ run: { status: 'running', lease: { expiresAt: 500 } } }))?.dueAt).toBe(500)
})
it('uses the current category and durably continues unresolved work', async () => {
  const services = io(item({ duePK: 'TIMER_RUN' }))
  const worker = handlers()
  await processWakeup(wakeup, services, worker, () => 200)
  expect(worker.processTarget).toHaveBeenCalledWith({ kind: 'timer', runId: 'fixture' })
  expect(services.schedule).toHaveBeenCalledWith({ ...wakeup, dueKind: 'TIMER_RUN' }, 1200)
  vi.mocked(services.schedule).mockRejectedValue(new Error('transport down'))
  await expect(processWakeup(wakeup, services, worker, () => 200)).rejects.toThrow('transport down')
})
it('reschedules held leases without executing and ignores completed work', async () => {
  const services = io(item({ dueSK: 500 }))
  const worker = handlers()
  await processWakeup(wakeup, services, worker, () => 200)
  expect(worker.processTarget).not.toHaveBeenCalled()
  expect(services.schedule).toHaveBeenCalledWith({ ...wakeup, dueAt: 500 }, 500)
  vi.mocked(services.read).mockResolvedValue(item({ duePK: undefined, run: { status: 'finished' } }))
  await processWakeup(wakeup, services, worker)
  expect(worker.processTarget).not.toHaveBeenCalled()
})
it('routes outbox and retention messages without treating them as runtime claims', async () => {
  const services = io(item({ outboxEnd: 2, cleanupAt: 100 }))
  const worker = handlers()
  await processWakeup({ ...wakeup, dueKind: 'OUTBOX' }, services, worker, () => 200)
  await processWakeup({ ...wakeup, dueKind: 'CLEANUP' }, services, worker, () => 200)
  expect(worker.drainEffects).toHaveBeenCalledWith('fixture')
  expect(worker.cleanupItem).toHaveBeenCalledWith('RUN#fixture')
  expect(worker.processTarget).not.toHaveBeenCalled()
})
it('decodes identifiers containing delimiters', () => {
  expect(workTarget({ ...wakeup, dueKind: 'TIMER', key: { PK: 'TIMER#run%23x#timer%3Ax%23y', SK: 'META' } })).toEqual({ kind: 'timer', runId: 'run#x', signalId: 'timer:x#y' })
})
function record(sequence: string, next: Record<string, unknown>, old?: Record<string, unknown>): StreamRecord {
  return { eventName: old ? 'MODIFY' : 'INSERT', dynamodb: { SequenceNumber: sequence,
    NewImage: marshall(next, { removeUndefinedValues: true }), ...(old ? { OldImage: marshall(old, { removeUndefinedValues: true }) } : {}) } }
}
it('coalesces keys and suppresses unchanged metadata and lease heartbeats', async () => {
  const transport = io()
  const metrics = vi.fn()
  const route = createWorkflowStreamRouter({ transport, publishApplicationEvent: vi.fn(), onMetrics: metrics })
  const first = item({ run: { status: 'running', lease: { owner: 'one', expiresAt: 500 } } })
  const heartbeat = item({ dueSK: 600, run: { status: 'running', lease: { owner: 'one', expiresAt: 600 } } })
  await route({ Records: [record('1', first), record('2', heartbeat, first), record('3', { ...heartbeat, head: 'new' }, heartbeat)] })
  expect(transport.read).toHaveBeenCalledTimes(1)
  expect(metrics).toHaveBeenCalledWith({ records: 3, unchanged: 2, keys: 1, applicationEvents: 0 })
})
it('retries the earliest deferred key when application queue delivery fails', async () => {
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const transport = io()
  const publish = vi.fn().mockRejectedValue(new Error('queue down'))
  const route = createWorkflowStreamRouter({ transport, publishApplicationEvent: publish })
  const event = { id: 'e', type: 'task.done', version: 1, timestamp: new Date().toISOString(), data: {} }
  const result = await route({ Records: [record('10', item()), record('11', { PK: 'EVENT#e', SK: 'META', schemaVersion: 1, entityType: 'APPLICATION_EVENT', event })] })
  expect(result.batchItemFailures).toEqual([{ itemIdentifier: '10' }])
  expect(transport.read).not.toHaveBeenCalled()
})
it('does not route staged event segments as committed publication intents', async () => {
  const transport = io()
  const publish = vi.fn()
  await createWorkflowStreamRouter({ transport, publishApplicationEvent: publish })({ Records: [record('1', { PK: 'RUN#one', SK: 'SEG#uncommitted', schemaVersion: 1 })] })
  expect(publish).not.toHaveBeenCalled()
  expect(transport.read).not.toHaveBeenCalled()
})
it('rejects an unsupported event storage version before fanout', async () => {
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const publish = vi.fn()
  const route = createWorkflowStreamRouter({ transport: io(), publishApplicationEvent: publish })
  const result = await route({ Records: [record('1', { PK: 'EVENT#e', SK: 'META', schemaVersion: 9,
    entityType: 'APPLICATION_EVENT', event: { id: 'e', type: 'test', version: 1, timestamp: new Date().toISOString(), data: {} } })] })
  expect(result.batchItemFailures).toEqual([{ itemIdentifier: '1' }])
  expect(publish).not.toHaveBeenCalled()
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
    expect(await createAwsWorkflowTransport(config).read(key)).toEqual(item())
    expect((send.mock.calls[0]?.[0] as { input: unknown }).input).toEqual({ TableName: config.tableName, Key: key, ConsistentRead: true })
  })
  it('accepts matching schedule conflicts but rejects mismatches', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(0)
    const conflict = Object.assign(new Error('existing'), { name: 'ConflictException' })
    const expected = scheduleInput(config, wakeup, 60_000)
    const send = vi.spyOn(SchedulerClient.prototype, 'send').mockRejectedValueOnce(conflict as never).mockResolvedValueOnce(expected as never)
    await expect(createAwsWorkflowTransport(config).schedule(wakeup, 60_000)).resolves.toBeUndefined()
    send.mockRejectedValueOnce(conflict as never).mockResolvedValueOnce({ ...expected, State: 'DISABLED' } as never)
    await expect(createAwsWorkflowTransport(config).schedule(wakeup, 60_000)).rejects.toThrow('Conflicting')
  })
  it('propagates failed creation and conflict lookup races for retry', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(0)
    const send = vi.spyOn(SchedulerClient.prototype, 'send').mockRejectedValueOnce(new Error('denied') as never)
    await expect(createAwsWorkflowTransport(config).schedule(wakeup, 60_000)).rejects.toThrow('denied')
    send.mockRejectedValueOnce(Object.assign(new Error('exists'), { name: 'ConflictException' }) as never).mockRejectedValueOnce(new Error('deleted during lookup') as never)
    await expect(createAwsWorkflowTransport(config).schedule(wakeup, 60_000)).rejects.toThrow('deleted during lookup')
  })
  it.each([false, true])('persists a queue fallback if create/lookup finishes after deadline (conflict=%s)', async conflict => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(0)
    const expected = scheduleInput(config, wakeup, 60_000)
    const send = vi.spyOn(SchedulerClient.prototype, 'send')
    if (conflict) send.mockRejectedValueOnce(Object.assign(new Error('exists'), { name: 'ConflictException' }) as never)
    send.mockImplementationOnce(async () => { now.mockReturnValue(60_001); return expected })
    const queue = vi.spyOn(SQSClient.prototype, 'send').mockResolvedValue({} as never)
    await expect(createAwsWorkflowTransport(config).schedule(wakeup, 60_000)).resolves.toBeUndefined()
    expect((queue.mock.calls[0]?.[0] as { input: unknown }).input).toMatchObject({ QueueUrl: config.queueUrl, MessageBody: JSON.stringify(wakeup) })
  })
  it('does not acknowledge an elapsed schedule when its queue fallback fails', async () => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(0)
    vi.spyOn(SchedulerClient.prototype, 'send').mockImplementationOnce(async () => { now.mockReturnValue(60_001); return {} })
    vi.spyOn(SQSClient.prototype, 'send').mockRejectedValue(new Error('fallback unavailable') as never)
    await expect(createAwsWorkflowTransport(config).schedule(wakeup, 60_000)).rejects.toThrow('fallback unavailable')
  })
  it('uses delayed SQS for a deadline inside the schedule creation safety window', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(5000)
    const send = vi.spyOn(SQSClient.prototype, 'send').mockResolvedValue({} as never)
    await createAwsWorkflowTransport(config).schedule(wakeup, 19_000)
    expect((send.mock.calls[0]?.[0] as { input: unknown }).input).toMatchObject({ DelaySeconds: 14 })
    expect(SchedulerClient.prototype.send).not.toHaveBeenCalled()
  })
  it('enqueues instead when a deadline elapses before schedule creation', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(5000)
    const send = vi.spyOn(SQSClient.prototype, 'send').mockResolvedValue({} as never)
    const schedule = vi.spyOn(SchedulerClient.prototype, 'send')
    await createAwsWorkflowTransport(config).schedule(wakeup, 4000)
    expect((send.mock.calls[0]?.[0] as { input: unknown }).input).toMatchObject({ DelaySeconds: 0, MessageBody: JSON.stringify(wakeup) })
    expect(schedule).not.toHaveBeenCalled()
  })
})
