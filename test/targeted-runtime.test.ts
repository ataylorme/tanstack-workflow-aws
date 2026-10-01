import { afterEach, describe, expect, it, vi } from 'vitest'
import { QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb'
import { createWorkflow } from '../src/workflow.js'
import { defineWorkflowRuntime, inMemoryWorkflowExecutionStore } from '../src/runtime.js'
import type { WorkflowWorkTarget } from '../src/runtime.js'
import { createTestStore } from './support/dynamodb.js'

it('rejects unsupported targeted claims instead of silently sweeping', async () => {
  const store = inMemoryWorkflowExecutionStore()
  const discovery = vi.spyOn(store, 'claimStaleRuns')
  const runtime = defineWorkflowRuntime({ store, workflows: {} })
  await expect(runtime.processTarget({ target: { kind: 'run', runId: 'one' } })).rejects.toThrow('does not support')
  expect(discovery).not.toHaveBeenCalled()
})

describe.skipIf(!process.env.DYNAMODB_ENDPOINT)('targeted execution without discovery', () => {
  const cleanups: Array<() => Promise<unknown>> = []
  afterEach(async () => { vi.restoreAllMocks(); await Promise.all(cleanups.splice(0).map(cleanup => cleanup())) })
  async function fixture() {
    const fixture = await createTestStore()
    cleanups.push(fixture.cleanup)
    // These fail even if the eventual index is empty: a targeted path must never use discovery.
    const send = fixture.client.send.bind(fixture.client)
    vi.spyOn(fixture.client, 'send').mockImplementation(((command: any) => {
      if (command instanceof QueryCommand || command instanceof ScanCommand) throw new Error('Unexpected discovery')
      return send(command)
    }) as typeof fixture.client.send)
    for (const method of ['claimStaleRuns', 'claimDueTimers', 'claimDueScheduleBuckets'] as const) {
      vi.spyOn(fixture.store, method).mockRejectedValue(new Error('Unexpected broad claim'))
    }
    return fixture
  }

  it('runs only the named queued item, respects a live lease, and tolerates concurrent/obsolete messages', async () => {
    const { store } = await fixture()
    const now = Date.now()
    const effect = vi.fn(() => 'done')
    const workflow = createWorkflow({ id: 'task' }).handler(async () => effect())
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => workflow } } })
    for (const runId of ['one', 'unrelated']) await store.createRun({ runId, workflowId: 'task', input: {}, now })
    await store.claimRun({ runId: 'one', leaseOwner: 'west:old', now, leaseMs: 1000 })
    const run = (leaseOwner: string, at: number) => store.withLeaseOwner(leaseOwner, () => runtime.processTarget({ target: { kind: 'run', runId: 'one' }, leaseOwner, now: at }))
    expect((await run('east:early', now + 500)).recovered).toEqual([])
    await Promise.all([run('east:new', now + 2000), run('west:new', now + 2000)])
    expect(effect).toHaveBeenCalledTimes(1)
    expect(await store.loadRun('one')).toMatchObject({ status: 'finished', output: 'done' })
    expect(await store.loadRun('unrelated')).toMatchObject({ status: 'queued' })
    expect((await run('east:obsolete', now + 3000)).recovered).toEqual([])
    expect((await runtime.processTarget({ target: { kind: 'run', runId: 'missing' } })).recovered).toEqual([])
  })

  it('resumes successive timer waits by run key while unrelated signal waits stay idle', async () => {
    const { store } = await fixture()
    const workflow = createWorkflow({ id: 'task' }).handler(async ctx => { await ctx.sleep(10); await ctx.sleep(10); return 'awake' })
    const waiting = createWorkflow({ id: 'waiting' }).handler(async ctx => ctx.waitForEvent('continue'))
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => workflow }, waiting: { load: async () => waiting } } })
    for (const workflowId of ['task', 'waiting']) await store.withLeaseOwner(`start:${workflowId}`, () => runtime.startRun({ workflowId, runId: workflowId, input: {}, leaseOwner: `start:${workflowId}` }))
    const process = (owner: string, now: number, target: WorkflowWorkTarget = { kind: 'timer', runId: 'task' }) => store.withLeaseOwner(owner, () => runtime.processTarget({ target, now, leaseOwner: owner }))
    const first = (await store.loadRun('task'))!.waitingFor!.deadline!
    expect((await process('early', first - 1)).timers).toEqual([])
    await process('east:first', first + 1)
    expect(await store.loadRun('task')).toMatchObject({ status: 'paused' })
    const second = (await store.loadRun('task'))!.waitingFor!.deadline!
    await process('west:second', second + 1)
    expect(await store.loadRun('task')).toMatchObject({ status: 'finished', output: 'awake' })
    await process('obsolete', second + 100)
    await process('wrong-category', second + 100, { kind: 'run', runId: 'waiting' })
    expect(await store.loadRun('waiting')).toMatchObject({ status: 'paused' })
  })

  it('recovers a claimed schedule bucket before advancing to a newer definition', async () => {
    const { store } = await fixture()
    const now = Date.now()
    const workflow = createWorkflow({ id: 'task' }).handler(async ctx => ctx.input)
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => workflow } } })
    const schedule = (scheduleId: string, fireAt: number) => store.upsertSchedule({ scheduleId, workflowId: 'task', input: { fireAt }, schedule: { kind: 'interval', everyMs: 100 }, nextFireAt: fireAt, overlapPolicy: 'allow', enabled: true, now: fireAt })
    await schedule('one', now)
    await schedule('other', now)
    const old = await store.claimScheduleBucket({ scheduleId: 'one', leaseOwner: 'crashed', now, leaseMs: 10 })
    await schedule('one', now + 100)
    const process = (at: number) => store.withLeaseOwner('east', () => runtime.processTarget({ target: { kind: 'schedule', scheduleId: 'one' }, leaseOwner: 'east', now: at }))
    expect((await process(now + 5)).scheduled).toEqual([])
    const first = await process(now + 100)
    expect(first.scheduled).toHaveLength(1)
    expect(await store.loadRun(old!.runId)).toMatchObject({ status: 'finished', output: { fireAt: now } })
    expect(await store.loadRun(`task:other:${now}`)).toBeUndefined()
    expect(await store.loadRun(`task:one:${now + 100}`)).toBeUndefined()
    await process(now + 101)
    expect(await store.loadRun(`task:one:${now + 100}`)).toMatchObject({ status: 'finished' })
    expect((await process(now + 102)).scheduled).toEqual([])
  })

  it.each(['skip', 'allow'] as const)('preserves %s overlap policy for targeted schedule delivery', async overlapPolicy => {
    const { store } = await fixture()
    const now = Date.now()
    const workflow = createWorkflow({ id: 'task' }).handler(async ctx => ctx.waitForEvent('done'))
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => workflow } } })
    const tick = async (at: number) => {
      await store.upsertSchedule({ scheduleId: 'one', workflowId: 'task', input: {}, schedule: { kind: 'interval', everyMs: 100 }, nextFireAt: at, overlapPolicy, enabled: true, now: at })
      return store.withLeaseOwner('east', () => runtime.processTarget({ target: { kind: 'schedule', scheduleId: 'one' }, leaseOwner: 'east', now: at }))
    }
    await tick(now)
    expect((await tick(now + 100)).scheduled).toHaveLength(overlapPolicy === 'skip' ? 0 : 1)
  })

  it('claims only the specified standalone timer and reclaims it after lease expiry', async () => {
    const { store } = await fixture()
    const now = Date.now()
    for (const runId of ['one#encoded', 'other']) {
      await store.createRun({ runId, workflowId: 'task', input: {}, now })
      await store.scheduleTimer({ runId, workflowId: 'task', signalId: 'timer#1', wakeAt: now + 100, now })
    }
    const args = { runId: 'one#encoded', signalId: 'timer#1', leaseOwner: 'east', leaseMs: 10, now: now + 100 }
    expect(await store.claimTimer({ ...args, now })).toBeUndefined()
    expect(await store.claimTimer(args)).toMatchObject({ runId: 'one#encoded', signalId: 'timer#1' })
    expect(await store.claimTimer({ ...args, leaseOwner: 'west', now: now + 105 })).toBeUndefined()
    expect(await store.claimTimer({ ...args, leaseOwner: 'west', now: now + 111 })).toMatchObject({ runId: 'one#encoded' })
    expect(await store.claimTimer({ ...args, runId: 'other' })).toMatchObject({ runId: 'other' })
  })

  it('does not claim after the runtime budget expires and defers retriable loader failures', async () => {
    const { store } = await fixture()
    const now = Date.now()
    const load = vi.fn().mockRejectedValue(new Error('loader unavailable'))
    const runtime = defineWorkflowRuntime({ store, workflows: { task: { load } } })
    await store.createRun({ runId: 'one', workflowId: 'task', input: {}, now })
    const args = { target: { kind: 'run' as const, runId: 'one' }, leaseOwner: 'east', now }
    expect((await store.withLeaseOwner('east', () => runtime.processTarget({ ...args, deadline: now - 1 }))).deadlineReached).toBe(true)
    expect(await store.loadRun('one')).toMatchObject({ status: 'queued' })
    expect(load).not.toHaveBeenCalled()
    const failed = await store.withLeaseOwner('east', () => runtime.processTarget(args))
    expect(failed.recovered[0]?.events).toContainEqual(expect.objectContaining({ code: 'recovery_deferred' }))
    expect((await store.loadRun('one'))!.lease!.expiresAt).toBeGreaterThanOrEqual(now + 60_000)
    load.mockResolvedValue(createWorkflow({ id: 'task' }).handler(async () => 'recovered'))
    await store.withLeaseOwner('west', () => runtime.processTarget({ ...args, leaseOwner: 'west', now: now + 120_000 }))
    expect(await store.loadRun('one')).toMatchObject({ status: 'finished', output: 'recovered' })
  })
})
