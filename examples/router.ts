import { createWorkflowStreamRouter, dispatchKey, parseKey } from '../src/wakeups.js'
import { createApplicationQueuePublisher } from '../src/aws.js'
import { required, transport } from './transport.js'
import type { StreamRecord, WorkerContext } from '../src/wakeups.js'
const route = createWorkflowStreamRouter({ transport,
  publishApplicationEvent: createApplicationQueuePublisher({ queueUrl: required('APPLICATION_QUEUE_URL') }),
  onMetrics: metrics => console.log(JSON.stringify({ kind: 'workflow_router_metrics', ...metrics })),
})
export async function handler(event: { Records: StreamRecord[] } | { kind: 'reconcile'; keys: unknown[] }, context: WorkerContext) {
  if ('kind' in event && event.kind === 'reconcile') {
    if (!Array.isArray(event.keys) || event.keys.length > 25) throw new Error('Reconcile at most 25 keys')
    for (const key of event.keys) {
      if (context.getRemainingTimeInMillis() < 15_000) throw new Error('Insufficient reconciliation budget')
      await dispatchKey(parseKey(key), transport)
    }
    return { reconciled: event.keys.length }
  }
  if (!('Records' in event)) throw new Error('Expected stream records')
  return route(event, context)
}
