import { store, runtime } from './runtime.js'

type Request = {
  kind?: string
  requestContext?: { http?: { method?: string } }
  rawPath?: string
  headers?: Record<string, string | undefined>
  body?: string | null
  isBase64Encoded?: boolean
}
type LambdaContext = { awsRequestId: string; getRemainingTimeInMillis(): number }
const response = (statusCode: number, data: unknown) => ({ statusCode, headers: { 'content-type': 'application/json' }, body: JSON.stringify(data) })
const validId = (value: unknown): value is string => typeof value === 'string' && value.length <= 256 && value.trim().length > 0 && !/[\u0000-\u001f\u007f]/.test(value)
function parseBody(event: Request): Record<string, unknown> {
  const body = event.isBase64Encoded ? Buffer.from(event.body ?? '', 'base64').toString('utf8') : event.body
  const value: unknown = JSON.parse(body || '{}')
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Expected a JSON object')
  return value as Record<string, unknown>
}

export async function handler(event: Request, context: LambdaContext) {
  const owner = `${process.env.AWS_REGION}:${context.awsRequestId}`
  const remaining = context.getRemainingTimeInMillis()
  return store.withLeaseOwner(owner, async () => {
    // HTTP API has a shorter timeout than the Lambda. Yield before its response
    // budget expires; individual workflow steps must also bound their own I/O.
    if (remaining <= 2_000) return response(503, { error: 'Insufficient execution time; retry with the same IDs' })
    const deadline = Date.now() + Math.min(25_000, remaining - 2_000)
    const method = event.requestContext?.http?.method
    const path = event.rawPath
    if (method === 'POST' && path === '/runs') {
      const runId = event.headers?.['x-workflow-run-id']
      if (!validId(runId)) return response(400, { error: 'x-workflow-run-id must be a nonempty ID of at most 256 characters without control characters' })
      let input: Record<string, unknown>
      try { input = parseBody(event) } catch { return response(400, { error: 'Body must be a valid JSON object' }) }
      const result = await runtime.startRun({ workflowId: 'task', runId, input, leaseOwner: owner, deadline, includeEvents: false })
      return response(202, result)
    }
    const match = /^\/runs\/([^/]+)\/signals$/.exec(path || '')
    if (method === 'POST' && match) {
      let runId: string
      let input: Record<string, unknown>
      try { runId = decodeURIComponent(match[1]); input = parseBody(event) } catch { return response(400, { error: 'Invalid run ID encoding or JSON object' }) }
      const { signalId, message } = input
      if (!validId(runId) || !validId(signalId)) return response(400, { error: 'runId and signalId must be nonempty IDs of at most 256 characters without control characters' })
      if (typeof message !== 'string') return response(400, { error: 'message must be a string' })
      const result = await runtime.deliverSignal({ runId, signalId, name: 'complete', payload: { message }, leaseOwner: owner, deadline, includeEvents: false })
      return response(202, result)
    }
    return response(404, { error: 'Not found' })
  })
}
