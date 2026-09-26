import { afterEach, describe, expect, it, vi } from 'vitest'
import { UpdateCommand, GetCommand, PutCommand, ScanCommand } from '@aws-sdk/lib-dynamodb'
import { createTestStore } from './support/dynamodb.js'
import { createDynamoWorkflowExecutionStore } from '../src/index.js'
import { LogConflictError } from '../src/workflow.js'

const event = (name: string) => ({ type: 'CUSTOM' as const, ts: Date.now(), name, value: { nested: { z: 1, a: 2 } } })
describe.skipIf(!process.env.DYNAMODB_ENDPOINT)('DynamoDB API integration', () => {
  const cleanups: Array<() => Promise<void>> = []
  afterEach(async () => { vi.restoreAllMocks(); await Promise.all(cleanups.splice(0).map(cleanup => cleanup())) })
  async function fixture() { const value = await createTestStore(); cleanups.push(value.cleanup); return value }

  it('enforces concurrent append CAS against actual DynamoDB expressions', async () => {
    const { store, client, tableName } = await fixture()
    const other = createDynamoWorkflowExecutionStore({ client, tableName })
    await store.createRun({ runId: 'append', workflowId: 'w', input: {}, now: Date.now() })
    const results = await Promise.allSettled([
      store.appendEvents({ runId: 'append', expectedNextIndex: 0, events: [event('west'), event('batch')] }),
      other.appendEvents({ runId: 'append', expectedNextIndex: 0, events: [event('east')] }),
    ])
    expect(results.filter(r => r.status === 'fulfilled')).toHaveLength(1)
    const rejected = results.find(r => r.status === 'rejected') as PromiseRejectedResult
    expect(rejected.reason).toBeInstanceOf(LogConflictError)
    const events = await store.readEvents({ runId: 'append' })
    expect(events.map(e => e.eventIndex)).toEqual(events.map((_, index) => index))
    expect(events.length).toBe(results[0].status === 'fulfilled' ? 2 : 1)
  })

  it('has one lease winner and rejects a superseded or unbound state writer', async () => {
    const { store } = await fixture()
    const now = Date.now()
    await store.createRun({ runId: 'lease', workflowId: 'w', input: {}, now })
    const results = await Promise.all(['west', 'east'].map(leaseOwner => store.claimRun({ runId: 'lease', leaseOwner, leaseMs: 1000, now })))
    expect(results.filter(r => r.kind === 'claimed')).toHaveLength(1)
    const winner = results.find(r => r.kind === 'claimed')!
    if (winner.kind !== 'claimed') throw new Error('Missing claim')
    const oldOwner = winner.run.lease!.owner
    await store.claimRun({ runId: 'lease', leaseOwner: 'replacement', leaseMs: 1000, now: now + 1001 })
    const state = { runId: 'lease', workflowId: 'w', status: 'running' as const, input: {}, createdAt: now, updatedAt: now }
    await expect(store.withLeaseOwner(oldOwner, () => store.saveRunState({ state }))).rejects.toThrow('lease')
    await expect(store.saveRunState({ state })).rejects.toThrow('withLeaseOwner')
  })

  it('reconciles a committed publish whose response was lost', async () => {
    const { store, client } = await fixture()
    await store.createRun({ runId: 'lost-response', workflowId: 'w', input: {}, now: Date.now() })
    const send = client.send.bind(client)
    let failOnce = true
    vi.spyOn(client, 'send').mockImplementation((async (command: any) => {
      const result = await send(command)
      if (command instanceof UpdateCommand && failOnce) { failOnce = false; throw Object.assign(new Error('lost acknowledgement'), { name: 'TimeoutError' }) }
      return result
    }) as any)
    expect(await store.appendEvents({ runId: 'lost-response', expectedNextIndex: 0, events: [event('committed')] })).toEqual({ nextIndex: 1 })
    expect(await store.readEvents({ runId: 'lost-response' })).toHaveLength(1)
  })

  it('keeps staged but uncommitted events invisible after a crash', async () => {
    const { store, client, tableName } = await fixture()
    await store.createRun({ runId: 'stage-crash', workflowId: 'w', input: {}, now: Date.now() })
    const send = client.send.bind(client)
    const fault = vi.spyOn(client, 'send').mockImplementation((async (command: any) => {
      if (command instanceof UpdateCommand) throw new Error('crash before publish')
      return send(command)
    }) as any)
    await expect(store.appendEvents({ runId: 'stage-crash', expectedNextIndex: 0, events: [event('orphan')] })).rejects.toThrow('crash before publish')
    fault.mockRestore()
    expect(await store.readEvents({ runId: 'stage-crash' })).toEqual([])
    await store.appendEvents({ runId: 'stage-crash', expectedNextIndex: 0, events: [event('replacement')] })
    expect((await store.readEvents({ runId: 'stage-crash' }))[0]?.event).toMatchObject({ name: 'replacement' })
    expect((await client.send(new ScanCommand({ TableName: tableName }))).Items?.filter(i => i.SK.startsWith('SEG#'))).toHaveLength(2)
  })

  it('deletes logically and fences accidental ID reuse', async () => {
    const { store } = await fixture()
    await store.createRun({ runId: 'deleted', workflowId: 'w', input: {}, now: 0 })
    await store.appendEvents({ runId: 'deleted', expectedNextIndex: 0, events: [event('audit')] })
    await store.deleteRun('deleted', 'finished')
    expect(await store.loadRun('deleted')).toBeUndefined()
    expect(await store.loadRunState('deleted')).toBeUndefined()
    expect(await store.readEvents({ runId: 'deleted' })).toEqual([])
    await expect(store.createRun({ runId: 'deleted', workflowId: 'w', input: {}, now: 1 })).rejects.toThrow('Deleted')
    await expect(store.saveRunState({ state: { runId: 'deleted', workflowId: 'w', input: {}, status: 'running', createdAt: 1, updatedAt: 1 } })).rejects.toThrow('deleted')
  })

  it('validates the authoritative schedule after a stale GSI result', async () => {
    const { store, client, tableName } = await fixture()
    await store.upsertSchedule({ scheduleId: 'disabled', workflowId: 'w', schedule: { kind: 'interval', everyMs: 1000 }, overlapPolicy: 'allow', enabled: true, now: 0, nextFireAt: undefined })
    const send = client.send.bind(client)
    vi.spyOn(client, 'send').mockImplementation((async (command: any) => {
      if (command.constructor.name === 'QueryCommand') return { Items: [{ PK: 'SCHEDULE#disabled', SK: 'META' }] }
      return send(command)
    }) as any)
    expect(await store.claimDueScheduleBuckets({ now: 1000, limit: 1, leaseOwner: 'west', leaseMs: 100 })).toEqual([])
    const data = await client.send(new ScanCommand({ TableName: tableName }))
    expect(data.Items?.some(i => i.PK.startsWith('BUCKET#'))).toBe(false)
  })
})
