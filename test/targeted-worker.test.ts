import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import type { Context } from 'aws-lambda'

const mocks = vi.hoisted(() => ({
  processTarget: vi.fn(), sweep: vi.fn(), withLeaseOwner: vi.fn(),
  read: vi.fn(), enqueue: vi.fn(), schedule: vi.fn(),
}))
vi.mock('../examples/runtime.js', () => ({
  store: { withLeaseOwner: mocks.withLeaseOwner },
  runtime: { processTarget: mocks.processTarget, sweep: mocks.sweep },
}))
vi.mock('../examples/wakeups.js', async importOriginal => ({
  ...await importOriginal<typeof import('../examples/wakeups.js')>(),
  wakeupIO: () => ({ read: mocks.read, enqueue: mocks.enqueue, schedule: mocks.schedule }),
}))
import { handler } from '../examples/sweeper.js'
const context = { awsRequestId: 'delivery', getRemainingTimeInMillis: () => 60_000 } as Context
const message = (runId: string) => ({ eventSource: 'aws:sqs', messageId: runId,
  body: JSON.stringify({ version: 1, kind: 'due', key: { PK: `RUN#${runId}`, SK: 'META' }, dueKind: 'RUNNING', dueAt: 1 }),
})
const item = (runId: string, duePK = 'RUNNING') => ({ PK: `RUN#${runId}`, SK: 'META', duePK, dueSK: 1, run: { status: duePK === 'RUNNING' ? 'running' : 'paused' } })
beforeEach(() => {
  vi.resetAllMocks()
  vi.stubEnv('AWS_REGION', 'us-east-1')
  vi.spyOn(console, 'log').mockImplementation(() => {})
  vi.spyOn(console, 'error').mockImplementation(() => {})
  mocks.withLeaseOwner.mockImplementation(async (_owner, callback) => callback())
  mocks.processTarget.mockResolvedValue({ summary: {}, recovered: [], scheduled: [], timers: [], deadlineReached: false })
  mocks.sweep.mockResolvedValue({ summary: {}, recovered: [], scheduled: [], timers: [], remainingMayExist: false })
})
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs() })

it('routes keyed SQS batches through fenced targeted execution and retries only failed continuations', async () => {
  mocks.read.mockResolvedValueOnce(item('one')).mockResolvedValueOnce(item('one'))
    .mockResolvedValueOnce(item('two', 'TIMER_RUN')).mockResolvedValueOnce(undefined)
  mocks.schedule.mockRejectedValueOnce(new Error('continuation unavailable'))
  expect(await handler({ Records: [message('one'), message('two')] }, context)).toEqual({ batchItemFailures: [{ itemIdentifier: 'one' }] })
  expect(mocks.processTarget).toHaveBeenNthCalledWith(1, { target: { kind: 'run', runId: 'one' }, leaseOwner: 'us-east-1:delivery', maxDurationMs: 45_000, includeEvents: false })
  expect(mocks.processTarget).toHaveBeenNthCalledWith(2, { target: { kind: 'timer', runId: 'two' }, leaseOwner: 'us-east-1:delivery', maxDurationMs: 45_000, includeEvents: false })
  expect(mocks.withLeaseOwner).toHaveBeenCalledTimes(2)
  expect(mocks.withLeaseOwner).toHaveBeenCalledWith('us-east-1:delivery', expect.any(Function))
  expect(mocks.sweep).not.toHaveBeenCalled()
  expect(mocks.enqueue).not.toHaveBeenCalled()
})

it('continues supporting old drain envelopes through the bounded legacy path', async () => {
  expect(await handler({ Records: [{ eventSource: 'aws:sqs', messageId: 'legacy', body: JSON.stringify({ version: 1, kind: 'drain' }) }] }, context)).toEqual({ batchItemFailures: [] })
  expect(mocks.sweep).toHaveBeenCalledExactlyOnceWith({ leaseOwner: 'us-east-1:delivery', maxDurationMs: 45_000, limit: 10, includeEvents: false })
  expect(mocks.processTarget).not.toHaveBeenCalled()
  expect(mocks.enqueue).not.toHaveBeenCalled()
})
