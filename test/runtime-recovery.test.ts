import { afterEach, describe, expect, it, vi } from 'vitest'
import { createWorkflow } from '../src/workflow.js'
import { defineWorkflowRuntime } from '../src/runtime.js'
import { createTestStore } from './support/dynamodb.js'

describe.skipIf(!process.env.DYNAMODB_ENDPOINT)('real DynamoDB runtime crash recovery', () => {
  const cleanups: Array<() => Promise<unknown>> = []
  afterEach(async () => { vi.restoreAllMocks(); await Promise.all(cleanups.splice(0).map(cleanup => cleanup())) })
  async function fixture() {
    const fixture = await createTestStore()
    cleanups.push(fixture.cleanup)
    return fixture
  }

  it('sweeps a durable queued start after death before the initial claim', async () => {
    const { store } = await fixture()
    const workflow = createWorkflow({ id: 'queued' }).handler(async () => ({ recovered: true }))
    const runtime = defineWorkflowRuntime({ store, workflows: { queued: { load: async () => workflow } } })
    await store.createRun({ runId: 'queued-crash', workflowId: 'queued', input: {}, now: Date.now() })
    await store.withLeaseOwner('east:queued-recovery', () => runtime.sweep({ leaseOwner: 'east:queued-recovery', now: Date.now() + 120_000 }))
    expect(await store.loadRun('queued-crash')).toMatchObject({ status: 'finished', output: { recovered: true } })
  })

  it('replays an accepted signal payload after death before runtime resume', async () => {
    const { store } = await fixture()
    const workflow = createWorkflow({ id: 'signal' }).handler(async ctx => {
      return await ctx.waitForEvent<{ value: number }>('continue')
    })
    const runtime = defineWorkflowRuntime({ store, workflows: { signal: { load: async () => workflow } } })
    await store.withLeaseOwner('west:start', () => runtime.startRun({ workflowId: 'signal', runId: 'signal-crash', input: {}, leaseOwner: 'west:start' }))
    const args = { runId: 'signal-crash', delivery: { signalId: 'once', name: 'continue', payload: { value: 42 } }, now: Date.now() }
    expect((await store.deliverSignal(args)).kind).toBe('delivered')
    await store.withLeaseOwner('east:recover', () => runtime.sweep({ leaseOwner: 'east:recover', now: Date.now() + 120_000 }))
    expect(await store.loadRun(args.runId)).toMatchObject({ status: 'finished', output: { value: 42 } })
    expect((await store.deliverSignal(args)).kind).toBe('duplicate')
    const resolutions = (await store.readEvents({ runId: args.runId })).filter(item => item.event.type === 'SIGNAL_RESOLVED')
    expect(resolutions).toHaveLength(1)
  })

  it('replays an accepted approval after death before runtime resume', async () => {
    const { store } = await fixture()
    const workflow = createWorkflow({ id: 'approval' }).handler(async ctx => {
      await ctx.approve({ title: 'Continue?' })
      return { approved: true }
    })
    const runtime = defineWorkflowRuntime({ store, workflows: { approval: { load: async () => workflow } } })
    await store.withLeaseOwner('west:approve-start', () => runtime.startRun({ workflowId: 'approval', runId: 'approval-crash', input: {}, leaseOwner: 'west:approve-start' }))
    const state = await store.loadRunState('approval-crash')
    const approvalId = state!.pendingApproval!.approvalId
    expect((await store.deliverApproval({ runId: 'approval-crash', approval: { approvalId, approved: true, feedback: 'Proceed' }, now: Date.now() })).kind).toBe('delivered')
    await store.withLeaseOwner('east:approve-recover', () => runtime.sweep({ leaseOwner: 'east:approve-recover', now: Date.now() + 120_000 }))
    expect(await store.loadRun('approval-crash')).toMatchObject({ status: 'finished', output: { approved: true } })
    expect((await store.readEvents({ runId: 'approval-crash' })).filter(item => item.event.type === 'APPROVAL_RESOLVED')).toHaveLength(1)
  })

  it('discovers a paused timer even when the process fails before scheduleTimer', async () => {
    const { store } = await fixture()
    const workflow = createWorkflow({ id: 'timer' }).handler(async ctx => {
      await ctx.sleep(1)
      return { awake: true }
    })
    const runtime = defineWorkflowRuntime({ store, workflows: { timer: { load: async () => workflow } } })
    const timerCall = vi.spyOn(store, 'scheduleTimer').mockRejectedValueOnce(new Error('crash before timer scheduling'))
    await expect(store.withLeaseOwner('west:timer-start', () => runtime.startRun({ workflowId: 'timer', runId: 'timer-crash', input: {}, leaseOwner: 'west:timer-start' }))).rejects.toThrow('crash before timer scheduling')
    timerCall.mockRestore()
    expect((await store.loadRunState('timer-crash'))?.waitingFor?.signalName).toBe('__timer')
    await store.withLeaseOwner('east:timer-recover', () => runtime.sweep({ leaseOwner: 'east:timer-recover', now: Date.now() + 120_000 }))
    expect(await store.loadRun('timer-crash')).toMatchObject({ status: 'finished', output: { awake: true } })
  })

  it('recovers a running run after a failure releases its lease', async () => {
    const { store } = await fixture()
    const workflow = createWorkflow({ id: 'released' }).handler(async () => 'recovered')
    const runtime = defineWorkflowRuntime({ store, workflows: { released: { load: async () => workflow } } })
    await store.createRun({ runId: 'released-crash', workflowId: 'released', input: {}, now: Date.now() })
    await store.claimRun({ runId: 'released-crash', leaseOwner: 'west:released', leaseMs: 60_000, now: Date.now() })
    await store.withLeaseOwner('west:released', () => store.saveRunState({ state: { runId: 'released-crash', workflowId: 'released', input: {}, status: 'running', createdAt: Date.now(), updatedAt: Date.now() } }))
    await store.releaseRunLease({ runId: 'released-crash', leaseOwner: 'west:released' })
    await store.withLeaseOwner('east:released-recover', () => runtime.sweep({ leaseOwner: 'east:released-recover', now: Date.now() + 120_000 }))
    expect(await store.loadRun('released-crash')).toMatchObject({ status: 'finished', output: 'recovered' })
  })
})

describe.skipIf(!process.env.DYNAMODB_ENDPOINT)('review: poison candidates and timer reuse', () => {
  const cleanups: Array<() => Promise<unknown>> = []
  afterEach(async () => { await Promise.all(cleanups.splice(0).map(cleanup => cleanup())) })
  async function fixture() { const value = await createTestStore(); cleanups.push(value.cleanup); return value }

  it('persists schema-invalid input as terminal instead of retrying forever', async () => {
    const { store } = await fixture()
    const input = { '~standard': { version: 1 as const, vendor: 'test', validate: (_: unknown) => ({ issues: [{ message: 'Invalid input' }] }) } }
    const workflow = createWorkflow({ id: 'invalid', input }).handler(async () => 'unreachable')
    const runtime = defineWorkflowRuntime({ store, workflows: { invalid: { load: async () => workflow } } })
    const result = await store.withLeaseOwner('west:invalid', () => runtime.startRun({ workflowId: 'invalid', runId: 'invalid:1', input: {}, leaseOwner: 'west:invalid' }))
    expect(result.kind).toBe('errored')
    expect((await store.loadRun('invalid:1'))?.status).toBe('errored')
    expect(await store.claimStaleRuns({ leaseOwner: 'east:invalid', leaseMs: 1000, limit: 10, now: Date.now() + 120_000 })).toEqual([])
  })

  it('defers an unknown workflow while recovering healthy work in the same sweep', async () => {
    const { store } = await fixture()
    const workflow = createWorkflow({ id: 'healthy' }).handler(async () => 'ok')
    const runtime = defineWorkflowRuntime({ store, workflows: { healthy: { load: async () => workflow } } })
    await store.createRun({ runId: 'unknown:1', workflowId: 'missing', input: {}, now: Date.now() - 2000 })
    await store.createRun({ runId: 'healthy:1', workflowId: 'healthy', input: {}, now: Date.now() - 1000 })
    const now = Date.now()
    const result = await store.withLeaseOwner('east:isolated', () => runtime.sweep({ leaseOwner: 'east:isolated', now }))
    expect(result.recovered.some(run => run.events.some(event => event.type === 'RUN_ERRORED' && event.code === 'recovery_deferred'))).toBe(true)
    expect(await store.loadRun('healthy:1')).toMatchObject({ status: 'finished', output: 'ok' })
    expect((await store.loadRun('unknown:1'))?.lease?.expiresAt).toBeGreaterThan(now + 59_000)
  })

  it('does not carry the first timer claim lease into a second short sleep', async () => {
    const { store } = await fixture()
    const workflow = createWorkflow({ id: 'two-timers' }).handler(async ctx => { await ctx.sleep(1); await ctx.sleep(1); return 'awake' })
    const runtime = defineWorkflowRuntime({ store, workflows: { 'two-timers': { load: async () => workflow } } })
    await store.withLeaseOwner('west:two', () => runtime.startRun({ workflowId: 'two-timers', runId: 'two:1', input: {}, leaseOwner: 'west:two' }))
    const now = Date.now() + 1000
    await store.withLeaseOwner('east:first', () => runtime.sweep({ leaseOwner: 'east:first', now, maxTimers: 1 }))
    expect((await store.loadRun('two:1'))?.status).toBe('paused')
    await store.withLeaseOwner('west:second', () => runtime.sweep({ leaseOwner: 'west:second', now: now + 10, maxTimers: 1 }))
    expect(await store.loadRun('two:1')).toMatchObject({ status: 'finished', output: 'awake' })
  })
})
