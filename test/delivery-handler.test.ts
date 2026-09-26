import { beforeEach, describe, expect, it, vi } from 'vitest'
const mocked = vi.hoisted(() => ({ startRun: vi.fn(), deliverSignal: vi.fn(), sweep: vi.fn() }))
vi.mock('../examples/runtime.js', () => ({
  store: { withLeaseOwner: (_owner: string, action: () => unknown) => action() },
  runtime: mocked,
}))
import { handler } from '../examples/handler.js'
const context = { awsRequestId: 'test', getRemainingTimeInMillis: () => 60_000 }
async function invoke(...args: Parameters<typeof handler>) {
  const result = await handler(...args)
  if (!('statusCode' in result)) throw new Error('Expected HTTP response')
  return result
}
const request = (body: string) => ({ requestContext: { http: { method: 'POST' } }, rawPath: '/runs', headers: { 'x-workflow-run-id': 'run-1' }, body })
beforeEach(() => { vi.clearAllMocks(); mocked.startRun.mockResolvedValue({ kind: 'paused' }); mocked.deliverSignal.mockResolvedValue({ kind: 'completed' }) })
describe('example HTTP handler', () => {
  it('decodes base64 JSON and caps HTTP execution to 25 seconds', async () => {
    const before = Date.now()
    const result = await invoke({ ...request(Buffer.from('{"hello":"world"}').toString('base64')), isBase64Encoded: true }, context)
    expect(result.statusCode).toBe(202)
    expect(mocked.startRun.mock.calls[0][0].input).toEqual({ hello: 'world' })
    expect(mocked.startRun.mock.calls[0][0].deadline).toBeGreaterThanOrEqual(before + 25_000)
    expect(mocked.startRun.mock.calls[0][0].deadline).toBeLessThanOrEqual(Date.now() + 25_000)
  })
  it.each(['{', 'null', '[]', '1'])('rejects malformed/non-object JSON %s without executing', async body => {
    expect((await invoke(request(body), context)).statusCode).toBe(400)
    expect(mocked.startRun).not.toHaveBeenCalled()
  })
  it.each(['', ' ', 'x'.repeat(257), 'a\nb'])('rejects invalid run identifiers', async id => {
    expect((await invoke({ ...request('{}'), headers: { 'x-workflow-run-id': id } }, context)).statusCode).toBe(400)
    expect(mocked.startRun).not.toHaveBeenCalled()
  })
  it('decodes URL run IDs and validates signal IDs/message', async () => {
    const event = { ...request('{"signalId":"signal-1","message":"done"}'), rawPath: '/runs/tenant%2Frun/signals' }
    expect((await invoke(event, context)).statusCode).toBe(202)
    expect(mocked.deliverSignal.mock.calls[0][0].runId).toBe('tenant/run')
    expect((await invoke({ ...event, body: '{"signalId":42,"message":"done"}' }, context)).statusCode).toBe(400)
    expect((await invoke({ ...event, body: '{"signalId":"signal-1"}' }, context)).statusCode).toBe(400)
    expect((await invoke({ ...event, rawPath: '/runs/%ZZ/signals' }, context)).statusCode).toBe(400)
    expect(mocked.deliverSignal).toHaveBeenCalledTimes(1)
  })
  it('leaves a Lambda shutdown margin and rejects exhausted budgets', async () => {
    const before = Date.now()
    await invoke(request('{}'), { ...context, getRemainingTimeInMillis: () => 3_000 })
    expect(mocked.startRun.mock.calls[0][0].deadline).toBeLessThanOrEqual(before + 1_100)
    expect((await invoke(request('{}'), { ...context, getRemainingTimeInMillis: () => 1_000 })).statusCode).toBe(503)
    expect(mocked.startRun).toHaveBeenCalledTimes(1)
  })
})
