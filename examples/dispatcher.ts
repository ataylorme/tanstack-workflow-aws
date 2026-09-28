import type { Context, DynamoDBStreamEvent, DynamoDBBatchResponse } from 'aws-lambda'
import { dispatchKey, parseKey, wakeupIO, type WakeupIO } from './wakeups.js'

export async function dispatchStream(event: DynamoDBStreamEvent, context: Pick<Context, 'getRemainingTimeInMillis'>,
  io: WakeupIO): Promise<DynamoDBBatchResponse> {
  const batchItemFailures: DynamoDBBatchResponse['batchItemFailures'] = []
  for (const record of event.Records) {
    try {
      if (context.getRemainingTimeInMillis() < 15_000) throw new Error('Insufficient dispatcher budget')
      if (record.eventName !== 'INSERT' && record.eventName !== 'MODIFY') continue
      const image = record.dynamodb?.NewImage
      if (!image?.duePK?.S || !image.dueSK?.N) continue
      await dispatchKey(parseKey({ PK: record.dynamodb?.Keys?.PK?.S, SK: record.dynamodb?.Keys?.SK?.S }), io)
    } catch (error) {
      console.error(JSON.stringify({ kind: 'workflow_wakeup_dispatch_failed', error: error instanceof Error ? error.name : 'UnknownError' }))
      const sequence = record.dynamodb?.SequenceNumber
      if (!sequence) throw new Error('Failed stream record has no sequence number')
      batchItemFailures.push({ itemIdentifier: sequence })
    }
  }
  return { batchItemFailures }
}

export async function handler(event: DynamoDBStreamEvent | { kind: 'reconcile'; keys: unknown[] }, context: Context) {
  const io = wakeupIO()
  if ('kind' in event && event.kind === 'reconcile') {
    if (!Array.isArray(event.keys) || event.keys.length > 25) throw new Error('Reconcile at most 25 keys per invocation')
    for (const key of event.keys) {
      if (context.getRemainingTimeInMillis() < 15_000) throw new Error('Insufficient reconciliation budget; retry batch')
      await dispatchKey(parseKey(key), io)
    }
    return { reconciled: event.keys.length }
  }
  if (!('Records' in event) || !Array.isArray(event.Records)) throw new Error('Invalid dispatcher event')
  return dispatchStream(event, context, io)
}
