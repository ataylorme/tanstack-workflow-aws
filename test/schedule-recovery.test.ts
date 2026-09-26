import { afterEach, describe, expect, it } from 'vitest'
import { createTestStore } from './support/dynamodb.js'
import type { UpsertScheduleArgs } from '../src/runtime.js'

describe.skipIf(!process.env.DYNAMODB_ENDPOINT)('DynamoDB schedule crash recovery and overlap', () => {
  const cleanups: Array<() => Promise<unknown>> = []
  afterEach(async () => { await Promise.all(cleanups.splice(0).map(cleanup => cleanup())) })
  async function fixture() { const result = await createTestStore(); cleanups.push(result.cleanup); return result }
  const schedule = (fireAt: number, overlapPolicy: UpsertScheduleArgs['overlapPolicy'] = 'allow'): UpsertScheduleArgs => ({
    scheduleId: 'periodic', workflowId: 'task', input: { fireAt }, schedule: { kind: 'interval', everyMs: 100 },
    nextFireAt: fireAt, overlapPolicy, enabled: true, now: fireAt,
  })
  const claim = (now: number, leaseOwner = 'west') => ({ now, leaseOwner, leaseMs: 30, limit: 1 })

  it('retains a crashed older bucket when the materializer advances to a newer tick', async () => {
    const { store } = await fixture()
    await store.upsertSchedule(schedule(100))
    const [old] = await store.claimDueScheduleBuckets(claim(100))
    await store.upsertSchedule(schedule(200))
    expect(await store.claimDueScheduleBuckets(claim(120, 'east'))).toEqual([])
    const [recovered] = await store.claimDueScheduleBuckets(claim(200, 'east'))
    expect(recovered).toMatchObject({ bucketId: '100', runId: old!.runId, input: { fireAt: 100 } })
    await store.withLeaseOwner('east', () => store.markScheduleBucketStarted({ scheduleId: 'periodic', bucketId: '100', runId: recovered!.runId, now: 200 }))
    expect((await store.claimDueScheduleBuckets(claim(200, 'east')))[0]).toMatchObject({ bucketId: '200', input: { fireAt: 200 } })
  })

  it('does not reclaim a live bucket in the same owner context and rejects stale owners', async () => {
    const { store } = await fixture()
    await store.upsertSchedule(schedule(100))
    const [first] = await store.claimDueScheduleBuckets(claim(100))
    expect(await store.claimDueScheduleBuckets(claim(110))).toEqual([])
    expect(await store.claimDueScheduleBuckets(claim(110, 'east'))).toEqual([])
    await store.claimDueScheduleBuckets(claim(131, 'east'))
    await expect(store.withLeaseOwner('west', () => store.markScheduleBucketStarted({ scheduleId: 'periodic', bucketId: '100', runId: first!.runId, now: 131 }))).rejects.toThrow('Lost schedule bucket lease')
    await expect(store.withLeaseOwner('east', () => store.markScheduleBucketStarted({ scheduleId: 'periodic', bucketId: '100', runId: 'wrong', now: 131 }))).rejects.toThrow('mismatched bucket')
    await store.withLeaseOwner('east', () => store.markScheduleBucketStarted({ scheduleId: 'periodic', bucketId: '100', runId: first!.runId, now: 131 }))
    expect(await store.claimDueScheduleBuckets(claim(200))).toEqual([])
  })

  it.each(['skip', 'allow'] as const)('enforces %s for an existing paused scheduled run', async policy => {
    const { store } = await fixture()
    await store.upsertSchedule(schedule(100, policy))
    const [first] = await store.claimDueScheduleBuckets(claim(100))
    await store.saveRunState({ state: { runId: first!.runId, workflowId: 'task', status: 'paused', input: {}, waitingFor: { signalName: 'external' }, createdAt: 100, updatedAt: 100 } })
    await store.withLeaseOwner('west', () => store.markScheduleBucketStarted({ scheduleId: 'periodic', bucketId: '100', runId: first!.runId, now: 101 }))
    await store.upsertSchedule(schedule(200, policy))
    const second = await store.claimDueScheduleBuckets(claim(200, 'east'))
    expect(second).toHaveLength(policy === 'skip' ? 0 : 1)
    if (policy === 'skip') {
      await store.saveRunState({ state: { runId: first!.runId, workflowId: 'task', status: 'finished', input: {}, createdAt: 100, updatedAt: 210 } })
      expect(await store.claimDueScheduleBuckets(claim(220))).toEqual([])
      await store.upsertSchedule(schedule(300, policy))
      expect((await store.claimDueScheduleBuckets(claim(300)))[0]?.bucketId).toBe('300')
    }
  })

  it('ignores stale definitions and prevents a newer writer rolling the due tick backward', async () => {
    const { store } = await fixture()
    await store.upsertSchedule(schedule(200))
    await store.upsertSchedule({ ...schedule(100), enabled: false })
    await store.upsertSchedule({ ...schedule(100), now: 300 })
    expect((await store.claimDueScheduleBuckets(claim(300)))[0]).toMatchObject({ bucketId: '200', input: { fireAt: 200 } })
  })

  it('rejects workflow and overlap policy changes but permits input updates', async () => {
    const { store } = await fixture()
    await store.upsertSchedule(schedule(100, 'allow'))
    await expect(store.upsertSchedule(schedule(200, 'skip'))).rejects.toThrow('immutable')
    await expect(store.upsertSchedule({ ...schedule(200), workflowId: 'different' })).rejects.toThrow('immutable')
    await store.upsertSchedule(schedule(200, 'allow'))
    expect((await store.claimDueScheduleBuckets(claim(200)))[0]).toMatchObject({ workflowId: 'task', input: { fireAt: 200 } })
  })

  it.each(['buffer-one', 'cancel-previous', 'terminate-previous'] as const)('rejects unsupported %s explicitly', async policy => {
    const { store } = await fixture()
    await expect(store.upsertSchedule(schedule(100, policy))).rejects.toThrow('Unsupported schedule overlap policy')
    expect(await store.claimDueScheduleBuckets(claim(100))).toEqual([])
  })
})
