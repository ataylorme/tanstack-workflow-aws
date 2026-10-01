import { afterEach, describe, expect, it, vi } from 'vitest'
import { GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb'
import { marshall } from '@aws-sdk/util-dynamodb'
import { SQSClient } from '@aws-sdk/client-sqs'
import { createDynamoApplicationEventPublisher } from '../src/events.js'
import { createDynamoOrderedEventPublisher, createDynamoOrderedSubscriber, orderedStreamKey } from '../src/ordered-events.js'
import { createWorkflowStreamRouter, createApplicationQueueHandler } from '../src/wakeups.js'
import { createApplicationQueuePublisher } from '../src/aws.js'
import { createTestStore } from './support/dynamodb.js'
import { createWorkflow } from '../src/workflow.js'
import { defineWorkflowRuntime } from '../src/runtime.js'
import { publishWorkflowEvent } from '../src/workflow-effects.js'
const types = ['task.requested', 'task.approved', 'task.started', 'task.completed']
const input = (sequence: number, streamId = 'tenant:task') => ({ type: types[sequence - 1]!, data: { sequence }, ordering: { streamId, sequence } })
afterEach(() => vi.restoreAllMocks())

it('routes ordered head commits, never staged slots or unchanged heads', async () => {
  const publish = vi.fn()
  const transport = { read: vi.fn(), enqueue: vi.fn(), schedule: vi.fn() }
  const event = { ...input(1), id: 'one', version: 1, timestamp: new Date().toISOString() }
  const head = { PK: orderedStreamKey('tenant:task'), SK: 'META', schemaVersion: 1, entityType: 'ORDERED_STREAM', streamId: 'tenant:task', committed: 1, event }
  const record = (next: any, old?: any) => ({ eventName: old ? 'MODIFY' : 'INSERT', dynamodb: { SequenceNumber: '1', NewImage: marshall(next), ...(old ? { OldImage: marshall(old) } : {}) } })
  const route = createWorkflowStreamRouter({ transport, publishApplicationEvent: publish })
  await route({ Records: [record({ ...head, SK: 'EVENT#1', entityType: 'STAGED' }), record(head, { ...head, committed: 0 }), record(head, head)] })
  expect(publish).toHaveBeenCalledTimes(1)
  expect(publish).toHaveBeenCalledWith(event)
  expect(transport.read).not.toHaveBeenCalled()
})
it('sends stable FIFO groups per stream and distinct deduplication positions', async () => {
  const send = vi.spyOn(SQSClient.prototype, 'send').mockResolvedValue({} as never)
  const publish = createApplicationQueuePublisher({ queueUrl: 'https://sqs.test/application.fifo' })
  for (const sequence of [1, 2]) await publish({ ...input(sequence), id: 'same-id', version: 1, timestamp: new Date().toISOString() })
  const calls = send.mock.calls.map(([command]) => (command as any).input)
  expect(calls[0].MessageGroupId).toBe(calls[1].MessageGroupId)
  expect(calls[0].MessageDeduplicationId).not.toBe(calls[1].MessageDeduplicationId)
})
it('returns the failed and all remaining FIFO records without processing them', async () => {
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const handler = vi.fn().mockRejectedValue(new Error('destination down'))
  const result = await createApplicationQueueHandler(handler, { fifo: true })({ Records: [1, 2].map(n => ({ eventSource: 'aws:sqs', messageId: String(n), body: JSON.stringify({ ...input(n), id: String(n), version: 1, timestamp: new Date().toISOString() }) })) })
  expect(result.batchItemFailures).toHaveLength(2)
  expect(handler).toHaveBeenCalledTimes(1)
})

describe.skipIf(!process.env.DYNAMODB_ENDPOINT)('ordered event log and regional subscribers', () => {
  const cleanups: Array<() => Promise<unknown>> = []
  afterEach(async () => { await Promise.all(cleanups.splice(0).map(fn => fn())) })
  async function fixture() {
    const f = await createTestStore(); cleanups.push(f.cleanup)
    const publisher = createDynamoApplicationEventPublisher({ ...f, orderedEventTypes: types })
    const log = createDynamoOrderedEventPublisher(f)
    return { ...f, publisher, log }
  }
  it('enforces contiguous typed positions, preserves duplicate timestamps and rejects conflicting slots', async () => {
    const f = await fixture()
    await expect(f.publisher.publish(input(2))).rejects.toThrow('Expected sequence 1')
    await expect(f.publisher.publish({ ...input(1), type: 'task.completed' })).rejects.toThrow('wrong type')
    const first = await f.publisher.publish(input(1))
    expect(await f.publisher.publish(input(1))).toEqual(first)
    await expect(f.publisher.publish({ ...input(1), data: { sequence: 99 } })).rejects.toThrow('different event')
    await f.publisher.publish(input(2))
    expect(await f.publisher.publish(input(1))).toEqual(first)
    expect(await f.log.read('tenant:task', 2)).toMatchObject(input(2))
  })
  it('arbitrates conflicting regional publishers and resumes a staged event after a lost head write', async () => {
    const f = await fixture()
    const original = f.client.send.bind(f.client)
    const spy = vi.spyOn(f.client, 'send').mockImplementation((async (command: any) => {
      if (command instanceof PutCommand && command.input.Item?.committed === 1) throw new Error('head unavailable')
      return original(command)
    }) as typeof f.client.send)
    await expect(f.publisher.publish(input(1))).rejects.toThrow('head unavailable')
    expect(await f.log.read('tenant:task', 1)).toBeUndefined()
    spy.mockRestore()
    const outcomes = await Promise.allSettled([f.publisher.publish(input(1)), f.publisher.publish({ ...input(1), data: { sequence: 99 } })])
    expect(outcomes.filter(result => result.status === 'fulfilled')).toHaveLength(1)
    expect(await f.log.read('tenant:task', 1)).toMatchObject(input(1))
  })
  it('reconciles an acknowledged head write whose response was lost', async () => {
    const f = await fixture()
    const original = f.client.send.bind(f.client)
    vi.spyOn(f.client, 'send').mockImplementation((async (command: any) => {
      const result = await original(command)
      if (command instanceof PutCommand && command.input.Item?.committed === 1) throw new Error('lost response')
      return result
    }) as typeof f.client.send)
    expect(await f.publisher.publish(input(1))).toMatchObject(input(1))
  })
  it('observes requested, approved, started, completed despite reversed and duplicate notifications', async () => {
    const f = await fixture()
    const events = []
    for (let sequence = 1; sequence <= 4; sequence++) events.push(await f.publisher.publish(input(sequence)))
    const observed: string[] = []
    const subscriber = createDynamoOrderedSubscriber({ ...f, subscriberId: 'notifications', handler: async event => { observed.push(event.type) } })
    for (const event of [...events].reverse()) await subscriber.process(event)
    await subscriber.process(events[3]!)
    expect(observed).toEqual(types)
    expect((await subscriber.inspect('tenant:task'))?.completed).toBe(4)
  })
  it('shares one non-expiring effect claim between regional consumers', async () => {
    const f = await fixture()
    const first = await f.publisher.publish(input(1))
    const second = await f.publisher.publish(input(2))
    let release!: () => void
    let entered!: () => void
    const started = new Promise<void>(resolve => { entered = resolve })
    const gate = new Promise<void>(resolve => { release = resolve })
    const observed: string[] = []
    const west = createDynamoOrderedSubscriber({ ...f, subscriberId: 'notifications', handler: async event => { observed.push(event.type); entered(); await gate } })
    const east = createDynamoOrderedSubscriber({ ...f, subscriberId: 'notifications', handler: async event => { observed.push(event.type) } })
    const work = west.process(first)
    await started
    // Even after an arbitrary interval, there is no lease timeout permitting overlap.
    vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 86400_000)
    await expect(east.process(second)).rejects.toThrow('Unresolved delivery')
    expect(observed).toEqual([types[0]])
    release(); await work
    await east.process(second)
    expect(observed).toEqual(types.slice(0, 2))
  })
  it('blocks later events after handler failure until explicit effect resolution', async () => {
    const f = await fixture()
    const first = await f.publisher.publish(input(1)); const second = await f.publisher.publish(input(2))
    const effects: string[] = []
    const subscriber = createDynamoOrderedSubscriber({ ...f, subscriberId: 'notifications', handler: async event => {
      effects.push(event.type)
      if (event.ordering.sequence === 1) throw new Error('effect accepted but response lost')
    } })
    await expect(subscriber.process(first)).rejects.toThrow('response lost')
    await expect(subscriber.process(second)).rejects.toThrow('Unresolved delivery')
    const cursor = await subscriber.inspect('tenant:task')
    await expect(subscriber.resolve({ streamId: 'tenant:task', claimId: 'wrong', outcome: 'acknowledge' })).rejects.toThrow('Claim changed')
    await subscriber.resolve({ streamId: 'tenant:task', claimId: cursor!.claim.id, outcome: 'acknowledge' })
    await subscriber.process(second)
    expect(effects).toEqual(types.slice(0, 2))
  })
  it('does not repeat an effect when the cursor acknowledgement fails', async () => {
    const f = await fixture()
    const first = await f.publisher.publish(input(1))
    const handler = vi.fn(async () => {})
    const subscriber = createDynamoOrderedSubscriber({ ...f, subscriberId: 'notifications', handler })
    const original = f.client.send.bind(f.client)
    const spy = vi.spyOn(f.client, 'send').mockImplementation((async (command: any) => {
      if (command instanceof PutCommand && command.input.Item?.completed === 1) throw new Error('ack down')
      return original(command)
    }) as typeof f.client.send)
    await expect(subscriber.process(first)).rejects.toThrow('ack down')
    spy.mockRestore()
    await expect(subscriber.process(first)).rejects.toThrow('Unresolved delivery')
    expect(handler).toHaveBeenCalledTimes(1)
  })
  it.each(['claim', 'acknowledgement'] as const)('reconciles a lost %s response without repeating the handler', async phase => {
    const f = await fixture()
    const first = await f.publisher.publish(input(1))
    const handler = vi.fn(async () => {})
    const subscriber = createDynamoOrderedSubscriber({ ...f, subscriberId: 'notifications', handler })
    const original = f.client.send.bind(f.client)
    vi.spyOn(f.client, 'send').mockImplementation((async (command: any) => {
      const result = await original(command)
      const item = command.input.Item
      if (command instanceof PutCommand && item?.PK.startsWith('SUB#') &&
          (phase === 'claim' ? item.claim : item.completed === 1)) throw new Error('lost response')
      return result
    }) as typeof f.client.send)
    await subscriber.process(first)
    await subscriber.process(first)
    expect(handler).toHaveBeenCalledTimes(1)
  })
  it('allows a verified retry without bypassing the failed event or blocking another entity', async () => {
    const f = await fixture()
    const first = await f.publisher.publish(input(1))
    const second = await f.publisher.publish(input(2))
    const unrelated = await f.publisher.publish(input(1, 'other-task'))
    const effects: string[] = []
    let unavailable = true
    const subscriber = createDynamoOrderedSubscriber({ ...f, subscriberId: 'notifications', handler: async event => {
      if (event.ordering.streamId === 'tenant:task' && unavailable) throw new Error('not performed')
      effects.push(event.ordering.streamId + ':' + event.ordering.sequence)
    } })
    await expect(subscriber.process(first)).rejects.toThrow('not performed')
    await subscriber.process(unrelated)
    expect(effects).toEqual(['other-task:1'])
    const cursor = await subscriber.inspect('tenant:task')
    unavailable = false
    await subscriber.resolve({ streamId: 'tenant:task', claimId: cursor!.claim.id, outcome: 'retry' })
    await subscriber.process(second)
    expect(effects).toEqual(['other-task:1', 'tenant:task:1', 'tenant:task:2'])
  })
  it('does not acknowledge FIFO records after a blocked ordered delivery', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const f = await fixture()
    const events = [await f.publisher.publish(input(1)), await f.publisher.publish(input(2))]
    const handler = vi.fn(async () => { throw new Error('uncertain') })
    const subscriber = createDynamoOrderedSubscriber({ ...f, subscriberId: 'notifications', handler })
    const result = await subscriber.handler({ Records: events.map((event, i) => ({ eventSource: 'aws:sqs', messageId: String(i), body: JSON.stringify(event) })) })
    expect(result.batchItemFailures).toEqual([{ itemIdentifier: '0' }, { itemIdentifier: '1' }])
    expect(handler).toHaveBeenCalledTimes(1)
  })
  it('rejects malformed ordering, stream definition edits and events beyond the persisted bound', async () => {
    const f = await fixture()
    await expect(f.publisher.publish({ ...input(1), ordering: { streamId: 'task', sequence: 0 } })).rejects.toThrow('positive safe integer')
    const limited = createDynamoOrderedEventPublisher({ ...f, maxEvents: 1 })
    await limited.publish(input(1))
    await expect(limited.publish(input(2))).rejects.toThrow('exceeds')
    await expect(f.publisher.publish(input(1))).rejects.toThrow('eventTypes cannot change')
  })

  it('independently advances distinct subscribers and respects bounded invocation budgets', async () => {
    const f = await fixture()
    let last
    for (let sequence = 1; sequence <= 4; sequence++) last = await f.publisher.publish(input(sequence))
    const firstHandler = vi.fn(async () => {})
    const one = createDynamoOrderedSubscriber({ ...f, subscriberId: 'one', handler: firstHandler, maxEventsPerInvocation: 2 })
    const two = createDynamoOrderedSubscriber({ ...f, subscriberId: 'two', handler: async () => {} })
    await expect(one.process(last!)).rejects.toThrow('batch limit')
    await one.process(last!)
    await two.process(last!)
    expect(firstHandler).toHaveBeenCalledTimes(4)
    expect((await two.inspect('tenant:task'))?.completed).toBe(4)
  })
  it('integrates ordered publication with committed workflow intents', async () => {
    const f = await fixture()
    await f.publisher.publish(input(1))
    const workflow = createWorkflow({ id: 'approve' }).handler(ctx => publishWorkflowEvent(ctx, 'approve', input(2)))
    const runtime = defineWorkflowRuntime({ store: f.store, workflows: { approve: { load: async () => workflow } } })
    await f.store.withLeaseOwner('west', () => runtime.startRun({ workflowId: 'approve', runId: 'one', input: {}, leaseOwner: 'west' }))
    expect(await f.log.read('tenant:task', 2)).toBeUndefined()
    await f.store.drainEffects('one', createDynamoApplicationEventPublisher(f))
    expect(await f.log.read('tenant:task', 2)).toMatchObject(input(2))
  })
})
