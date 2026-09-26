import { describe, expect, it } from 'vitest'
import { GetCommand, PutCommand, UpdateCommand, QueryCommand } from '@aws-sdk/lib-dynamodb'
import { createDynamoWorkflowExecutionStore } from '../src/index.js'

// Fault-injection model, not a replacement for the DynamoDB Local expression tests.
function fixture() {
  const items = new Map<string, any>()
  const failures = { stageAfterCommit: false, publishAfterCommit: false, replicationBeforeStage: 0 }
  const commands: any[] = []
  const id = (key: any) => `${key.PK}/${key.SK}`
  const conditional = () => { throw Object.assign(new Error('conflict'), { name: 'ConditionalCheckFailedException' }) }
  const client = { async send(command: any): Promise<any> {
    const x = command.input
    commands.push(command)
    if (command instanceof GetCommand) return { Item: structuredClone(items.get(id(x.Key))) }
    if (command instanceof PutCommand) {
      if (x.Item.SK.startsWith('SEG#') && failures.replicationBeforeStage-- > 0) throw Object.assign(new Error('replicated contention'), { name: 'ReplicatedWriteConflictException' })
      const old = items.get(id(x.Item))
      if (x.ConditionExpression.includes('attribute_not_exists') && old) conditional()
      if (x.ConditionExpression.includes('#v') && old?.version !== x.ExpressionAttributeValues[':v']) conditional()
      items.set(id(x.Item), structuredClone(x.Item))
      if (x.Item.SK.startsWith('SEG#') && failures.stageAfterCommit) {
        failures.stageAfterCommit = false
        throw Object.assign(new Error('SDK retry after commit'), { name: 'ConditionalCheckFailedException' })
      }
      return {}
    }
    if (command instanceof UpdateCommand) {
      const old = items.get(id(x.Key)), v = x.ExpressionAttributeValues
      for (const name of Object.keys(x.ExpressionAttributeNames)) {
        if (!`${x.ConditionExpression} ${x.UpdateExpression}`.includes(name)) throw new Error(`Unused expression name ${name}`)
      }
      if (old.nextIndex !== v[':index'] || old.version !== v[':version']) conditional()
      items.set(id(x.Key), { ...old, head: v[':head'], nextIndex: v[':next'], version: old.version + 1 })
      if (failures.publishAfterCommit) {
        failures.publishAfterCommit = false
        throw Object.assign(new Error('response lost after commit'), { name: 'TimeoutError' })
      }
      return {}
    }
    if (command instanceof QueryCommand) return { Items: [] }
    throw new Error('Unexpected command')
  } }
  return { store: createDynamoWorkflowExecutionStore({ tableName: 'test', client: client as any }), items, failures, commands }
}

const event = { type: 'CUSTOM' as const, ts: 1, name: 'committed', value: { ok: true } }

describe('review regression: DynamoDB failure ambiguity', () => {
  it.each(['stageAfterCommit', 'publishAfterCommit'] as const)('reconciles %s without duplicate events', async mode => {
    const { store, failures } = fixture()
    await store.createRun({ runId: 'r', workflowId: 'w', input: {}, now: 0 })
    failures[mode] = true
    expect(await store.appendEvents({ runId: 'r', expectedNextIndex: 0, events: [event] })).toEqual({ nextIndex: 1 })
    expect((await store.readEvents({ runId: 'r' })).map(x => x.event)).toEqual([event])
  })

  it('retries a transient replication conflict before staging', async () => {
    const { store, failures } = fixture()
    await store.createRun({ runId: 'r', workflowId: 'w', input: {}, now: 0 })
    failures.replicationBeforeStage = 1
    expect(await store.appendEvents({ runId: 'r', expectedNextIndex: 0, events: [event] })).toEqual({ nextIndex: 1 })
  })

  it('does not query or claim when the requested limit is zero', async () => {
    const { store, commands } = fixture()
    expect(await store.claimDueTimers({ now: 10, limit: 0, leaseOwner: 'west', leaseMs: 10 })).toEqual([])
    expect(commands).toHaveLength(0)
  })

  it('rejects heartbeat by a superseded or expired lease holder', async () => {
    const { store } = fixture()
    await store.createRun({ runId: 'r', workflowId: 'w', input: {}, now: 0 })
    await store.claimRun({ runId: 'r', leaseOwner: 'west', leaseMs: 10, now: 0 })
    await expect(store.heartbeatRunLease({ runId: 'r', leaseOwner: 'east', now: 5, leaseMs: 10 })).rejects.toThrow('Lost workflow lease')
    await expect(store.heartbeatRunLease({ runId: 'r', leaseOwner: 'west', now: 10, leaseMs: 10 })).rejects.toThrow('Lost workflow lease')
  })
})
