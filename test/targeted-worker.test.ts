import { describe, expect, it, vi } from 'vitest'
import { createWorkflowWorker, createApplicationQueueHandler } from '../src/wakeups.js'
const context = { awsRequestId: 'request', getRemainingTimeInMillis: () => 60_000 }
const message = (id: string) => ({ eventSource: 'aws:sqs', messageId: id, body: JSON.stringify({ version: 1, kind: 'due', key: { PK: `RUN#${id}`, SK: 'META' }, dueKind: 'RUNNING', dueAt: 1 }) })
it('uses fenced targeted execution and reports only failed successors', async () => {
  vi.spyOn(console, 'log').mockImplementation(() => {})
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const metadata = (id: string) => ({ PK: `RUN#${id}`, SK: 'META', schemaVersion: 1, duePK: 'RUNNING', dueSK: 1, run: { status: 'running' } })
  const runtime = { processTarget: vi.fn().mockResolvedValue({ summary: {}, recovered: [], timers: [], scheduled: [] }) }
  const store = { withLeaseOwner: vi.fn(async (_: string, fn: () => Promise<unknown>) => fn()), drainEffects: vi.fn(), cleanupItem: vi.fn() }
  const transport = { read: vi.fn().mockResolvedValueOnce(metadata('one')).mockResolvedValueOnce(metadata('one')).mockResolvedValueOnce(metadata('two')).mockResolvedValueOnce(undefined), enqueue: vi.fn(), schedule: vi.fn().mockRejectedValueOnce(new Error('down')) }
  const worker = createWorkflowWorker({ runtime, store, transport, publisher: {}, region: 'us-east-1' } as any)
  expect(await worker({ Records: [message('one'), message('two')] }, context)).toEqual({ batchItemFailures: [{ itemIdentifier: 'one' }] })
  expect(runtime.processTarget).toHaveBeenCalledTimes(2)
  expect(store.withLeaseOwner).toHaveBeenCalledWith('us-east-1:request:one', expect.any(Function))
  vi.restoreAllMocks()
})
it('isolates application handler failures and validates queue envelopes', async () => {
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const effect = vi.fn()
  const event = { id: 'e', type: 'done', version: 1, timestamp: new Date().toISOString(), data: {} }
  const handler = createApplicationQueueHandler(effect)
  expect(await handler({ Records: [{ ...message('bad'), body: '{}' }, { ...message('good'), body: JSON.stringify(event) }] }, context)).toEqual({ batchItemFailures: [{ itemIdentifier: 'bad' }] })
  expect(effect).toHaveBeenCalledExactlyOnceWith(event)
  vi.restoreAllMocks()
})
