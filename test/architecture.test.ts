import { afterEach, describe, expect, it, vi } from 'vitest'
import { GetCommand, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb'
import { createWorkflow } from '../src/workflow.js'
import { defineWorkflowRuntime, materializeWorkflowSchedules } from '../src/runtime.js'
import { publishWorkflowEvent, continueAsNew } from '../src/workflow-effects.js'
import { createDynamoApplicationEventPublisher } from '../src/events.js'
import { nextScheduleTime } from '../src/schedules.js'
import { WorkflowLimitError } from '../src/index.js'
import { createTestStore } from './support/dynamodb.js'

it('computes future interval and zoned cron deadlines and validates definitions', () => {
  expect(nextScheduleTime({ kind: 'interval', everyMs: 100 }, 100)).toBe(200)
  expect(new Date(nextScheduleTime({ kind: 'cron', expression: '0 9 * * *', timezone: 'America/Los_Angeles' }, Date.parse('2026-03-07T17:00:00Z'))).toISOString()).toBe('2026-03-08T16:00:00.000Z')
  expect(new Date(nextScheduleTime({ kind: 'cron', expression: '0 9 * * THU' }, Date.parse('2026-03-07T17:00:00Z'))).toISOString()).toBe('2026-03-12T09:00:00.000Z')
  expect(() => nextScheduleTime({ kind: 'interval', everyMs: 0 }, 1)).toThrow()
  expect(() => nextScheduleTime({ kind: 'cron', expression: 'H * * * *' }, 1)).toThrow()
  expect(() => nextScheduleTime({ kind: 'cron', expression: '0 9 * * *', timezone: 'Invalid/Zone' }, 1)).toThrow()
})

describe.skipIf(!process.env.DYNAMODB_ENDPOINT)('committed effects, recurring schedules and retention', () => {
  const cleanups: Array<() => Promise<unknown>> = []
  afterEach(async () => { vi.restoreAllMocks(); await Promise.all(cleanups.splice(0).map(fn => fn())) })
  async function fixture(limits = {}) {
    const value = await createTestStore({ limits }); cleanups.push(value.cleanup)
    const read = async (PK: string) => (await value.client.send(new GetCommand({ TableName: value.tableName, Key: { PK, SK: 'META' }, ConsistentRead: true }))).Item!
    const publisher = createDynamoApplicationEventPublisher({ tableName: value.tableName, client: value.client })
    return { ...value, read, publisher }
  }

  it('publishes only committed checkpoints and recovers a crash after publication before acknowledgement', async () => {
    const { store, publisher, read } = await fixture()
    const workflow = createWorkflow({ id: 'task' }).handler(ctx => publishWorkflowEvent(ctx, 'requested', { type: 'task.requested', data: { value: 42 } }))
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => workflow } } })
    const result = await store.withLeaseOwner('west', () => runtime.startRun({ workflowId: 'task', runId: 'one', input: {}, leaseOwner: 'west' }))
    const output = result.run!.output as { id: string }
    expect(await read(`EVENT#${output.id}`)).toBeUndefined()
    const failAfterWrite = { publish: vi.fn(async (event: any) => { await publisher.publish(event); throw new Error('lost ack') }) }
    await expect(store.drainEffects('one', failAfterWrite)).rejects.toThrow('lost ack')
    expect((await read('RUN#one')).outboxCursor).toBeUndefined()
    expect(await store.drainEffects('one', publisher)).toBe(1)
    expect(await store.drainEffects('one', publisher)).toBe(0)
    expect((await read(`EVENT#${output.id}`)).event).toMatchObject({ id: output.id, type: 'task.requested', data: { value: 42 } })
    const meta = await read('RUN#one')
    expect(meta.outboxCursor).toBe(meta.outboxEnd)
  })

  it('acknowledges trailing log records and drains accepted intents after abort', async () => {
    const { store, publisher, read } = await fixture()
    await store.createRun({ runId: 'batch', workflowId: 'task', input: {}, now: 1 })
    await store.appendEvents({ runId: 'batch', expectedNextIndex: 0, events: [
      { type: 'STEP_FINISHED', stepId: 'effect', ts: 1, result: { $workflowEffect: 'publish', event: {
        id: 'accepted', type: 'test', version: 1, timestamp: new Date().toISOString(), data: {},
      } } },
      { type: 'STEP_FINISHED', stepId: 'ordinary', ts: 2, result: 'ok' },
    ] })
    await store.deleteRun('batch', 'aborted')
    expect(await store.loadRun('batch')).toBeUndefined()
    expect(await store.drainEffects('batch', publisher)).toBe(1)
    expect((await read('RUN#batch')).outboxCursor).toBe(2)
    expect((await read('EVENT#accepted')).event.id).toBe('accepted')
  })

  it('preserves the full tombstone window when another cleanup finishes concurrently', async () => {
    const { store, publisher, client, read } = await fixture({ tombstoneRetentionMs: 1000 })
    await publisher.publish({ id: 'race', type: 'test', data: {} })
    const now = (await read('EVENT#race')).cleanupAt
    const original = client.send.bind(client)
    let reads = 0
    const spy = vi.spyOn(client, 'send').mockImplementation((async (command: any) => {
      if (command instanceof GetCommand && ++reads === 2) {
        spy.mockRestore()
        await store.cleanupItem('EVENT#race', now)
      }
      return original(command)
    }) as typeof client.send)
    await store.cleanupItem('EVENT#race', now)
    expect(await read('EVENT#race')).toMatchObject({ deleted: true, cleanupAt: now + 1000 })
  })

  it('makes history exhaustion terminal during targeted recovery', async () => {
    const { store } = await fixture({ maxHistoryEvents: 2 })
    const workflow = createWorkflow({ id: 'task' }).handler(async ctx => {
      await ctx.step('one', async () => 1)
      await ctx.step('two', async () => 2)
      return 'done'
    })
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => workflow } } })
    await store.createRun({ runId: 'limited', workflowId: 'task', input: {}, now: 1 })
    await store.withLeaseOwner('east', () => runtime.processTarget({ target: { kind: 'run', runId: 'limited' }, leaseOwner: 'east' }))
    expect((await store.loadRun('limited'))?.status).toBe('errored')
  })

  it('never publishes an unreachable staged segment', async () => {
    const { store, client } = await fixture()
    await store.createRun({ runId: 'one', workflowId: 'task', input: {}, now: Date.now() })
    const original = client.send.bind(client)
    const spy = vi.spyOn(client, 'send').mockImplementation(((command: any) => {
      if (command instanceof UpdateCommand) throw new Error('commit unavailable')
      return original(command)
    }) as typeof client.send)
    await expect(store.appendEvents({ runId: 'one', expectedNextIndex: 0, events: [{ type: 'STEP_FINISHED', stepId: 'event', ts: Date.now(), result: {
      $workflowEffect: 'publish', event: { id: 'never', type: 'test', version: 1, timestamp: new Date().toISOString(), data: {} },
    } }] })).rejects.toThrow('commit unavailable')
    spy.mockRestore()
    const publisher = { publish: vi.fn() }
    expect(await store.drainEffects('one', publisher)).toBe(0)
    expect(publisher.publish).not.toHaveBeenCalled()
  })

  it('continues as new through an idempotent committed successor intent', async () => {
    const { store, publisher } = await fixture()
    const workflow = createWorkflow({ id: 'task' }).handler(async ctx => {
      const input = ctx.input as { next: boolean }
      return input.next ? 'finished' : continueAsNew(ctx, { next: true })
    })
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => workflow } } })
    const result = await store.withLeaseOwner('west', () => runtime.startRun({ workflowId: 'task', runId: 'one', input: { next: false }, leaseOwner: 'west' }))
    const successor = (result.run!.output as { runId: string }).runId
    expect(await store.loadRun(successor)).toBeUndefined()
    await store.drainEffects('one', publisher)
    await store.drainEffects('one', publisher)
    expect(await store.readEvents({ runId: successor })).toEqual([])
    await store.withLeaseOwner('east', () => runtime.processTarget({ target: { kind: 'run', runId: successor }, leaseOwner: 'east' }))
    expect(await store.loadRun(successor)).toMatchObject({ status: 'finished', input: { next: true }, output: 'finished' })
  })

  it.each(['checkpoint', 'state'] as const)('recovers a continuation after a failed terminal %s write', async phase => {
    const { store, publisher } = await fixture()
    const body = vi.fn(async (ctx: any) => continueAsNew(ctx, { next: true }))
    const workflow = createWorkflow({ id: 'task' }).handler(body)
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => workflow } } })
    const append = store.appendEvents.bind(store)
    const save = store.saveRunState.bind(store)
    const appendSpy = vi.spyOn(store, 'appendEvents').mockImplementation(async args => {
      if (phase === 'checkpoint' && args.events.some(event => event.type === 'RUN_FINISHED')) throw new Error('terminal write failed')
      return append(args)
    })
    const stateSpy = vi.spyOn(store, 'saveRunState').mockImplementation(async args => {
      if (phase === 'state' && args.state.status === 'finished') throw new Error('terminal write failed')
      return save(args)
    })
    await expect(store.withLeaseOwner('west', () => runtime.startRun({ workflowId: 'task', runId: 'parent', input: {}, leaseOwner: 'west' }))).rejects.toThrow('terminal write failed')
    appendSpy.mockRestore(); stateSpy.mockRestore()
    expect((await store.loadRun('parent'))?.status).toBe('running')
    await store.withLeaseOwner('east', () => runtime.processTarget({ target: { kind: 'run', runId: 'parent' }, leaseOwner: 'east' }))
    const parent = await store.loadRun('parent')
    expect(parent?.status).toBe('finished')
    expect(body).toHaveBeenCalledTimes(phase === 'state' ? 1 : 2)
    await store.drainEffects('parent', publisher)
    expect(await store.loadRun((parent!.output as { runId: string }).runId)).toMatchObject({ status: 'queued', input: { next: true } })
    expect((await store.readEvents({ runId: 'parent' })).filter(entry => entry.event.type === 'RUN_FINISHED')).toHaveLength(1)
  })

  it.each(['skip', 'run-once', 'catch-up'] as const)('advances %s schedules without another materialization pass', async missedTickPolicy => {
    const { store, read } = await fixture()
    await store.upsertSchedule({ scheduleId: 'periodic', workflowId: 'task', schedule: { kind: 'interval', everyMs: 100 },
      overlapPolicy: 'allow', enabled: true, now: 0, nextFireAt: 100, missedTickPolicy, maxCatchUp: 2 })
    const claim = () => store.claimScheduleBucket({ scheduleId: 'periodic', now: 550, leaseOwner: 'east', leaseMs: 100 })
    const first = await claim()
    if (missedTickPolicy === 'skip') expect(first).toBeUndefined()
    else {
      await store.withLeaseOwner('east', () => store.markScheduleBucketStarted({ scheduleId: 'periodic', bucketId: first!.bucketId, runId: first!.runId, now: 550 }))
      if (missedTickPolicy === 'catch-up') {
        const second = await claim()
        expect(second!.fireAt).toBe(200)
        await store.withLeaseOwner('east', () => store.markScheduleBucketStarted({ scheduleId: 'periodic', bucketId: second!.bucketId, runId: second!.runId, now: 550 }))
      }
    }
    expect((await read('SCHEDULE#periodic')).nextFireAt).toBe(600)
    expect(await claim()).toBeUndefined()
    expect((await store.claimScheduleBucket({ scheduleId: 'periodic', now: 600, leaseOwner: 'east', leaseMs: 100 }))!.fireAt).toBe(600)
  })

  it('seeds future ticks once and disables future execution without discarding an accepted bucket', async () => {
    const { store, read } = await fixture()
    const workflows = { task: { load: async () => createWorkflow({ id: 'task' }).handler(async () => 'ok'), schedules: [{ id: 'daily', schedule: { kind: 'interval' as const, everyMs: 100 } }] } }
    const runtime = defineWorkflowRuntime({ store, workflows })
    await materializeWorkflowSchedules(runtime, { now: 1 })
    await materializeWorkflowSchedules(runtime, { now: 50 })
    expect((await read('SCHEDULE#daily')).generation).toBe(1)
    const bucket = await store.claimScheduleBucket({ scheduleId: 'daily', now: 100, leaseOwner: 'east', leaseMs: 100 })
    await store.upsertSchedule({ scheduleId: 'daily', workflowId: 'task', schedule: { kind: 'interval', everyMs: 100 }, overlapPolicy: 'skip', enabled: false, now: 150 })
    await store.withLeaseOwner('east', () => store.markScheduleBucketStarted({ scheduleId: 'daily', bucketId: bucket!.bucketId, runId: bucket!.runId, now: 200 }))
    expect((await read('SCHEDULE#daily')).duePK).toBeUndefined()
  })

  it('bounds history, payloads and signal receipts before accepting excess data', async () => {
    const { store } = await fixture({ maxHistoryEvents: 2, maxItemBytes: 4096, maxSignalIds: 1 })
    await store.createRun({ runId: 'one', workflowId: 'task', input: {}, now: 0 })
    const event = { type: 'RUN_STARTED' as const, runId: 'one', ts: 1 }
    await store.appendEvents({ runId: 'one', expectedNextIndex: 0, events: [event, event] })
    await expect(store.appendEvents({ runId: 'one', expectedNextIndex: 2, events: [event] })).rejects.toBeInstanceOf(WorkflowLimitError)
    await expect(store.createRun({ runId: 'large', workflowId: 'task', input: 'x'.repeat(5000), now: 0 })).rejects.toBeInstanceOf(WorkflowLimitError)
    const paused = { runId: 'signal', workflowId: 'task', status: 'paused' as const, input: {}, waitingFor: { signalName: 'go' }, createdAt: 0, updatedAt: 1 }
    await store.saveRunState({ state: paused })
    await store.deliverSignal({ runId: 'signal', delivery: { signalId: 'one', name: 'go', payload: {} }, now: 2 })
    await store.saveRunState({ state: paused })
    await expect(store.deliverSignal({ runId: 'signal', delivery: { signalId: 'two', name: 'go', payload: {} }, now: 3 })).rejects.toBeInstanceOf(WorkflowLimitError)
  })

  it('defers cleanup until effects are delivered, removes history in bounded pages, and retains a tombstone', async () => {
    const { store, publisher, client, tableName, read } = await fixture({ terminalRetentionMs: 1, tombstoneRetentionMs: 1000 })
    const workflow = createWorkflow({ id: 'task' }).handler(ctx => publishWorkflowEvent(ctx, 'done', { type: 'done', data: {} }))
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => workflow } } })
    await store.withLeaseOwner('east', () => runtime.startRun({ runId: 'one', workflowId: 'task', input: {}, leaseOwner: 'east' }))
    let now = Date.now() + 100
    await store.cleanupItem('RUN#one', now, 1)
    expect((await read('RUN#one')).deleted).not.toBe(true)
    await store.drainEffects('one', publisher)
    now += 60_001
    // Include an unreachable segment; terminal cleanup must remove it as well.
    await client.send(new PutCommand({ TableName: tableName, Item: { PK: 'RUN#one', SK: 'SEG#orphan', schemaVersion: 1 } }))
    for (let i = 0; i < 20 && !(await read('RUN#one')).purgedAt; i++) await store.cleanupItem('RUN#one', now, 1)
    const tombstone = await read('RUN#one')
    expect(tombstone).toMatchObject({ deleted: true, purgedAt: now, cleanupAt: now + 1000 })
    expect(tombstone.state).toBeUndefined()
    expect(tombstone.deliveredIds).toBeUndefined()
    expect((await client.send(new QueryCommand({ TableName: tableName, KeyConditionExpression: 'PK = :pk', ExpressionAttributeValues: { ':pk': 'RUN#one' }, ConsistentRead: true }))).Items).toHaveLength(1)
    await expect(store.createRun({ runId: 'one', workflowId: 'task', input: {}, now })).rejects.toThrow('cannot be reused')
    await store.cleanupItem('RUN#one', now + 1001)
    expect(await read('RUN#one')).toBeUndefined()
  })

  it('expires application payloads while preventing publication ID reuse during the tombstone window', async () => {
    const { store, publisher, read } = await fixture()
    const event = await publisher.publish({ id: 'one', type: 'test', data: { a: 1 } })
    const now = (await read('EVENT#one')).cleanupAt
    await store.cleanupItem('EVENT#one', now)
    expect(await read('EVENT#one')).toMatchObject({ entityType: 'EVENT_TOMBSTONE', deleted: true })
    await expect(publisher.publish(event)).rejects.toThrow('already contains different content')
  })
})
