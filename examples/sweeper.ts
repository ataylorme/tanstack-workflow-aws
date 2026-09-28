import type { Context, SQSEvent, SQSBatchResponse } from 'aws-lambda'
import { parseWakeup, processWakeup, wakeupIO } from './wakeups.js'

export async function sweep(context: Context) {
  const { store, runtime } = await import('./runtime.js')
  const owner = `${process.env.AWS_REGION}:${context.awsRequestId}`
  const budget = context.getRemainingTimeInMillis() - 15_000
  if (budget <= 0) throw new Error('Insufficient sweep budget')
  const result = await store.withLeaseOwner(owner, () => runtime.sweep({
    leaseOwner: owner, maxDurationMs: budget, limit: 10, includeEvents: false,
  }))
  console.log(JSON.stringify({ region: process.env.AWS_REGION, summary: result.summary,
    diagnostics: [...result.recovered, ...result.scheduled, ...result.timers]
      .flatMap(run => run.events.filter(event => event.type === 'RUN_ERRORED')) }))
  return result
}

export async function handler(event: unknown, context: Context) {
  // Retained only for the migration/rollback rule and explicit operator sweeps.
  if (event && typeof event === 'object' && 'kind' in event && event.kind === 'sweep') return sweep(context)
  if (!event || typeof event !== 'object' || !('Records' in event) || !Array.isArray(event.Records)) {
    throw new Error('Expected an SQS wakeup batch')
  }
  const batchItemFailures: SQSBatchResponse['batchItemFailures'] = []
  for (const message of (event as SQSEvent).Records) {
    try {
      if (message.eventSource !== 'aws:sqs' || context.getRemainingTimeInMillis() < 20_000) throw new Error('Invalid message or insufficient budget')
      await processWakeup(parseWakeup(JSON.parse(message.body)), wakeupIO(), () => sweep(context))
    } catch (error) {
      console.error(JSON.stringify({ kind: 'workflow_wakeup_failed', error: error instanceof Error ? error.name : 'UnknownError' }))
      batchItemFailures.push({ itemIdentifier: message.messageId })
    }
  }
  return { batchItemFailures }
}
