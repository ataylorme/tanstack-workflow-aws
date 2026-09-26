import { store, runtime } from './runtime.js'

const response = (statusCode: number, data: unknown) => ({ statusCode, headers: { 'content-type': 'application/json' }, body: JSON.stringify(data) })

export async function handler(event: any, context: any) {
  const owner = `${process.env.AWS_REGION}:${context.awsRequestId}`
  return store.withLeaseOwner(owner, async () => {
    if (event.kind === 'sweep') {
      return runtime.sweep({ leaseOwner: owner, deadline: Date.now() + context.getRemainingTimeInMillis() - 1000, limit: 20, includeEvents: false })
    }
    const method = event.requestContext?.http?.method
    const path = event.rawPath
    if (method === 'POST' && path === '/runs') {
      const runId = event.headers?.['x-workflow-run-id']
      if (!runId) return response(400, { error: 'x-workflow-run-id is required for idempotency' })
      const result = await runtime.startRun({ workflowId: 'task', runId, input: JSON.parse(event.body || '{}'), leaseOwner: owner, deadline: Date.now() + context.getRemainingTimeInMillis() - 1000 })
      return response(202, result)
    }
    const match = /^\/runs\/([^/]+)\/signals$/.exec(path || '')
    if (method === 'POST' && match) {
      const { signalId, message } = JSON.parse(event.body || '{}')
      if (!signalId) return response(400, { error: 'signalId is required for idempotency' })
      const result = await runtime.deliverSignal({ runId: decodeURIComponent(match[1]), signalId, name: 'complete', payload: { message }, leaseOwner: owner, deadline: Date.now() + context.getRemainingTimeInMillis() - 1000 })
      return response(202, result)
    }
    return response(404, { error: 'Not found' })
  })
}
