import { describe, expect, it } from 'vitest'
import { GetCommand, PutCommand, UpdateCommand, QueryCommand, DeleteCommand, ScanCommand } from '@aws-sdk/lib-dynamodb'
import { LogConflictError } from '../src/workflow.js'
import { createDynamoWorkflowExecutionStore } from '../src/index.js'

function fakeClient() {
  const items = new Map<string, any>()
  const fail = () => { throw Object.assign(new Error('conditional conflict'), { name: 'ConditionalCheckFailedException' }) }
  const key = (x: any) => `${x.PK}/${x.SK}`
  const clone = (x: any) => structuredClone(x)
  return {
    items,
    async send(command: any): Promise<any> {
      const x = command.input
      if (command instanceof GetCommand) return { Item: clone(items.get(key(x.Key))) }
      if (command instanceof PutCommand) {
        const old = items.get(key(x.Item))
        if (x.ConditionExpression?.includes('attribute_not_exists') && old) fail()
        if (x.ConditionExpression?.includes('#v') && old?.version !== x.ExpressionAttributeValues[':v']) fail()
        items.set(key(x.Item), clone(x.Item)); return {}
      }
      if (command instanceof UpdateCommand) {
        const old = items.get(key(x.Key)), vals = x.ExpressionAttributeValues
        if (old?.nextIndex !== vals[':index'] || old.version !== vals[':version']) fail()
        if (vals[':owner'] && old.run?.lease?.owner !== vals[':owner']) fail()
        items.set(key(x.Key), { ...old, head: vals[':head'], nextIndex: vals[':next'], version: old.version + 1 }); return {}
      }
      if (command instanceof DeleteCommand) { items.delete(key(x.Key)); return {} }
      if (command instanceof QueryCommand) return { Items: [...items.values()].filter(i => i.duePK === x.ExpressionAttributeValues[':p'] && i.dueSK <= x.ExpressionAttributeValues[':n']) }
      if (command instanceof ScanCommand) return { Items: [...items.values()].filter(i => i.PK.startsWith('RUN#') && i.SK === 'META') }
      throw new Error('Unsupported command')
    },
  }
}

function stores() {
  const client = fakeClient()
  return { client, a: createDynamoWorkflowExecutionStore({ tableName: 'mrsc', client: client as any }), b: createDynamoWorkflowExecutionStore({ tableName: 'mrsc', client: client as any }) }
}

describe('distributed execution store', () => {
  it('commits an entire event batch atomically and fences a competing regional writer', async () => {
    const { a, b, client } = stores()
    await a.createRun({ runId: 'r', workflowId: 'w', input: {}, now: 0 })
    const claim = await a.claimRun({ runId: 'r', leaseOwner: 'west', leaseMs: 50, now: 0 })
    expect(claim.kind).toBe('claimed')
    expect((await b.claimRun({ runId: 'r', leaseOwner: 'east', leaseMs: 50, now: 20 })).kind).toBe('not-claimable')
    await a.withLeaseOwner('west', () => a.appendEvents({ runId: 'r', expectedNextIndex: 0, events: [{ type: 'CUSTOM', ts: 1, name: 'one', value: {} }, { type: 'CUSTOM', ts: 2, name: 'two', value: {} }] }))
    expect((await b.readEvents({ runId: 'r' })).map(e => e.eventIndex)).toEqual([0, 1])
    await expect(b.appendEvents({ runId: 'r', expectedNextIndex: 0, events: [{ type: 'CUSTOM', ts: 3, name: 'conflict', value: {} }] })).rejects.toBeInstanceOf(LogConflictError)
    expect((await b.claimRun({ runId: 'r', leaseOwner: 'east', leaseMs: 50, now: 51 })).kind).toBe('claimed')
    await expect(a.withLeaseOwner('west', () => a.appendEvents({ runId: 'r', expectedNextIndex: 2, events: [{ type: 'CUSTOM', ts: 4, name: 'stale', value: {} }] }))).rejects.toThrow()
    await b.withLeaseOwner('east', () => b.appendEvents({ runId: 'r', expectedNextIndex: 2, events: [{ type: 'CUSTOM', ts: 5, name: 'third', value: {} }] }))
    expect((await a.readEvents({ runId: 'r' })).map(e => e.eventIndex)).toEqual([0, 1, 2])
    expect([...client.items.values()].filter(i => i.SK.startsWith('SEG#')).length).toBe(2)
  })

  it('claims due work once across regions, recovers stale runs, and deduplicates a signal', async () => {
    const { a, b } = stores()
    await a.createRun({ runId: 'r', workflowId: 'w', input: {}, now: 0 })
    await a.scheduleTimer({ runId: 'r', workflowId: 'w', wakeAt: 100, signalId: 's', now: 0 })
    expect(await a.claimDueTimers({ now: 99, limit: 1, leaseOwner: 'west', leaseMs: 50 })).toHaveLength(0)
    expect(await a.claimDueTimers({ now: 100, limit: 1, leaseOwner: 'west', leaseMs: 50 })).toHaveLength(1)
    expect(await b.claimDueTimers({ now: 101, limit: 1, leaseOwner: 'east', leaseMs: 50 })).toHaveLength(0)
    await a.saveRunState({ state: { runId: 'r', workflowId: 'w', status: 'paused', input: {}, waitingFor: { signalName: 'done' }, createdAt: 0, updatedAt: 0 } })
    expect((await a.deliverSignal({ runId: 'r', delivery: { signalId: 's', name: 'done', payload: true }, now: 101 })).kind).toBe('delivered')
    expect((await b.deliverSignal({ runId: 'r', delivery: { signalId: 's', name: 'done', payload: true }, now: 102 })).kind).toBe('duplicate')
    await a.claimRun({ runId: 'r', leaseOwner: 'west', leaseMs: 50, now: 102 })
    expect(await b.claimStaleRuns({ now: 151, limit: 1, leaseOwner: 'east', leaseMs: 50 })).toHaveLength(0)
    expect((await b.claimStaleRuns({ now: 153, limit: 1, leaseOwner: 'east', leaseMs: 50 }))[0]?.lease.owner).toBe('east')
  })
})

describe('schedule coordination', () => {
  it('allows one regional scheduler to start a bucket and excludes a started bucket', async () => {
    const { a, b } = stores()
    await a.upsertSchedule({ scheduleId: 'hourly', workflowId: 'w', schedule: { kind: 'interval', everyMs: 3600000 }, overlapPolicy: 'skip', input: { x: 1 }, nextFireAt: 100, enabled: true, now: 0 })
    expect(await a.claimDueScheduleBuckets({ now: 99, limit: 1, leaseOwner: 'west', leaseMs: 30 })).toHaveLength(0)
    const first = await a.claimDueScheduleBuckets({ now: 100, limit: 1, leaseOwner: 'west', leaseMs: 30 })
    expect(first[0]).toMatchObject({ bucketId: '100', runId: 'w:hourly:100' })
    expect(await b.claimDueScheduleBuckets({ now: 101, limit: 1, leaseOwner: 'east', leaseMs: 30 })).toHaveLength(0)
    await a.withLeaseOwner('west', () => a.markScheduleBucketStarted({ scheduleId: 'hourly', bucketId: '100', runId: 'w:hourly:100', now: 101 }))
    expect(await b.claimDueScheduleBuckets({ now: 131, limit: 1, leaseOwner: 'east', leaseMs: 30 })).toHaveLength(0)
  })
})

describe('TanStack runtime integration', () => {
  it('starts in one region and resumes from a signal in the other', async () => {
    const { a, b } = stores()
    const { createWorkflow } = await import('../src/workflow.js')
    const { defineWorkflowRuntime } = await import('../src/runtime.js')
    const workflow = createWorkflow({ id: 'cross-region' }).handler(async ctx => {
      const result = await ctx.waitForEvent<{ ok: boolean }>('continue')
      return { ok: result.ok }
    })
    const west = defineWorkflowRuntime({ store: a, workflows: { 'cross-region': { load: async () => workflow } } })
    const east = defineWorkflowRuntime({ store: b, workflows: { 'cross-region': { load: async () => workflow } } })
    const started = await a.withLeaseOwner('west', () => west.startRun({ workflowId: 'cross-region', runId: 'cross:1', input: {}, leaseOwner: 'west' }))
    expect(started.kind).toBe('paused')
    const resumed = await b.withLeaseOwner('east', () => east.deliverSignal({ runId: 'cross:1', signalId: 'signal:1', name: 'continue', payload: { ok: true }, leaseOwner: 'east' }))
    expect(resumed.kind).toBe('completed')
    expect((await a.loadRun('cross:1'))?.status).toBe('finished')
  })
})
