import type { WorkflowWorkTarget, WorkflowRuntimeDefinition } from './runtime.js'
import type { FencedWorkflowExecutionStore } from './index.js'
import type { ApplicationEvent, ApplicationEventPublisher, ApplicationEventHandler } from './events.js'
import { decodeApplicationEvent } from './event-stream.js'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'
import { serializeApplicationEvent } from './event-validation.js'

export const dueKinds = ['RUNNING', 'TIMER_RUN', 'TIMER', 'SCHEDULE', 'OUTBOX', 'CLEANUP'] as const
type DueKind = typeof dueKinds[number]
export interface ItemKey { PK: string; SK: string }
export type DueWakeup = { version: 1; kind: 'due'; key: ItemKey; dueKind: DueKind; dueAt: number }
export type Wakeup = DueWakeup
export interface WakeupIO {
  read(key: ItemKey): Promise<Record<string, unknown> | undefined>
  enqueue(wakeup: Wakeup, delaySeconds?: number): Promise<void>
  schedule(wakeup: DueWakeup, at: number): Promise<void>
}
const record = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === 'object' ? value as Record<string, unknown> : {}
const timestamp = (value: unknown): number => typeof value === 'number' && Number.isFinite(value) ? value : 0

export function parseKey(value: unknown): ItemKey {
  const key = record(value)
  if (typeof key.PK !== 'string' || !/^(RUN|TIMER|SCHEDULE|EVENT)#/.test(key.PK) ||
      key.PK.length > 2048 || key.SK !== 'META') throw new Error('Invalid workflow metadata key')
  return { PK: key.PK, SK: key.SK }
}

export function parseWakeup(value: unknown): Wakeup {
  const input = record(value)
  if (input.version !== 1) throw new Error('Unsupported wakeup version')
  if (input.kind !== 'due' || !dueKinds.includes(input.dueKind as DueKind) ||
      typeof input.dueAt !== 'number' || !Number.isFinite(input.dueAt) || input.dueAt < 0) throw new Error('Invalid wakeup')
  return { version: 1, kind: 'due', key: parseKey(input.key), dueKind: input.dueKind as DueKind, dueAt: input.dueAt }
}

// The installed store's due fields are a version-pinned storage contract. In
// particular timerLease is NOT reflected in dueSK, so it must be considered here.
export function dueWakeup(item: Record<string, unknown> | undefined): DueWakeup | undefined {
  if (!item || item.deleted || !['RUNNING', 'TIMER_RUN', 'TIMER', 'SCHEDULE'].includes(item.duePK as string) ||
      typeof item.dueSK !== 'number' || !Number.isFinite(item.dueSK) || item.dueSK < 0) return undefined
  const run = record(item.run)
  if (['finished', 'errored', 'aborted'].includes(String(run.status))) return undefined
  const dueKind = item.duePK as DueKind
  const lease = dueKind === 'TIMER_RUN' ? item.timerLease
    : dueKind === 'TIMER' ? item.lease
    : dueKind === 'SCHEDULE' ? record(item.pendingBucket).lease : run.lease
  return { version: 1, kind: 'due', key: parseKey(item), dueKind,
    dueAt: Math.max(item.dueSK, timestamp(record(lease).expiresAt)) }
}

export async function dispatchKey(key: ItemKey, io: WakeupIO, now = Date.now()): Promise<void> {
  for (const wakeup of itemWakeups(await io.read(key))) {
    if (wakeup.dueAt <= now) await io.enqueue(wakeup)
    else await io.schedule(wakeup, wakeup.dueAt)
  }
}

/** Translate the authoritative item category, never the possibly stale message category. */
export function workTarget(wakeup: DueWakeup): WorkflowWorkTarget {
  const { PK } = wakeup.key
  if (wakeup.dueKind === 'RUNNING' && PK.startsWith('RUN#')) return { kind: 'run', runId: PK.slice(4) }
  if (wakeup.dueKind === 'TIMER_RUN' && PK.startsWith('RUN#')) return { kind: 'timer', runId: PK.slice(4) }
  if (wakeup.dueKind === 'SCHEDULE' && PK.startsWith('SCHEDULE#')) return { kind: 'schedule', scheduleId: PK.slice(9) }
  if (wakeup.dueKind === 'TIMER' && PK.startsWith('TIMER#')) {
    const parts = PK.slice(6).split('#')
    if (parts.length === 2) return { kind: 'timer', runId: decodeURIComponent(parts[0]!), signalId: decodeURIComponent(parts[1]!) }
  }
  throw new Error('Workflow due category does not match its item key')
}

export interface WakeupHandlers {
  processTarget(target: WorkflowWorkTarget): Promise<unknown>
  drainEffects(runId: string): Promise<unknown>
  cleanupItem(PK: string): Promise<unknown>
}

export function itemWakeups(item: Record<string, unknown> | undefined): DueWakeup[] {
  if (!item) return []
  if (item.schemaVersion !== 1) throw new Error('Unsupported workflow storage version')
  const key = parseKey(item)
  const result: DueWakeup[] = []
  const due = dueWakeup(item)
  if (due) result.push(due)
  if (!item.purging && item.purgedAt === undefined && key.PK.startsWith('RUN#') && timestamp(item.outboxEnd) > timestamp(item.outboxCursor)) result.push({ version: 1, kind: 'due', key, dueKind: 'OUTBOX', dueAt: 0 })
  if (typeof item.cleanupAt === 'number') result.push({ version: 1, kind: 'due', key, dueKind: 'CLEANUP', dueAt: item.cleanupAt })
  return result
}

export async function processWakeup(wakeup: Wakeup, io: WakeupIO,
  handlers: WakeupHandlers, now = Date.now): Promise<void> {
  const tasks = itemWakeups(await io.read(wakeup.key))
  const current = tasks.find(task => wakeup.dueKind === 'OUTBOX' || wakeup.dueKind === 'CLEANUP'
    ? task.dueKind === wakeup.dueKind : !['OUTBOX', 'CLEANUP'].includes(task.dueKind))
  if (!current) return
  if (current.dueAt > now()) { await io.schedule(current, current.dueAt); return }
  if (current.dueKind === 'OUTBOX') await handlers.drainEffects(current.key.PK.slice(4))
  else if (current.dueKind === 'CLEANUP') await handlers.cleanupItem(current.key.PK)
  else await handlers.processTarget(workTarget(current))
  for (const pending of itemWakeups(await io.read(wakeup.key))) {
    await io.schedule(pending, Math.max(pending.dueAt, now() + 1000))
  }
}

export interface WorkerContext { awsRequestId: string; getRemainingTimeInMillis(): number }
export interface QueueBatch { Records: { eventSource?: string | undefined; messageId: string; body: string }[] }
export interface StreamRecord {
  eventName?: string | undefined
  dynamodb?: { SequenceNumber?: string | undefined; NewImage?: Record<string, unknown> | undefined; OldImage?: Record<string, unknown> | undefined } | undefined
}
export interface RouterMetrics { records: number; unchanged: number; keys: number; applicationEvents: number }
export interface StreamRouterOptions {
  transport: WakeupIO
  publishApplicationEvent(event: ApplicationEvent): Promise<void>
  minRemainingTimeMs?: number
  onMetrics?(metrics: RouterMetrics): void
}
const image = (value?: Record<string, unknown>) => value ? unmarshall(value as Record<string, AttributeValue>) : undefined
function interesting(record: StreamRecord): boolean {
  const next = image(record.dynamodb?.NewImage)
  if (!next || next.SK !== 'META' || !/^(RUN|TIMER|SCHEDULE|EVENT)#/.test(next.PK)) return false
  const old = image(record.dynamodb?.OldImage)
  // A previously durable lease wakeup will reread and follow a heartbeat extension.
  const heartbeat = next.duePK === 'RUNNING' && old?.duePK === 'RUNNING' &&
    next.run?.status === old.run?.status && next.run?.lease?.owner === old.run?.lease?.owner &&
    next.run?.lease?.expiresAt > old.run?.lease?.expiresAt
  const signature = (value: Record<string, any> | undefined) => itemWakeups(value).map(task =>
    heartbeat && task.dueKind === 'RUNNING' ? { ...task, dueAt: 0 } : task)
  return JSON.stringify(signature(next)) !== JSON.stringify(signature(old))
}

/** One stream reader per replica; durable fan-out only, no business handlers. */
export function createWorkflowStreamRouter(options: StreamRouterOptions) {
  return async (batch: { Records: StreamRecord[] }, context?: Pick<WorkerContext, 'getRemainingTimeInMillis'>) => {
    const metrics: RouterMetrics = { records: batch.Records.length, unchanged: 0, keys: 0, applicationEvents: 0 }
    const keys = new Map<string, { key: ItemKey; sequence: string | undefined }>()
    let failedSequence: string | undefined
    try {
      for (const record of batch.Records) {
        failedSequence = record.dynamodb?.SequenceNumber
        if (context && context.getRemainingTimeInMillis() < (options.minRemainingTimeMs ?? 15_000)) throw new Error('Insufficient router budget')
        if (record.eventName === 'REMOVE') continue
        const changed = interesting(record)
        const event = decodeApplicationEvent(record)
        if (event) { await options.publishApplicationEvent(event); metrics.applicationEvents++ }
        if (!changed) { metrics.unchanged++; continue }
        const key = parseKey(image(record.dynamodb?.NewImage))
        if (!keys.has(key.PK)) keys.set(key.PK, { key, sequence: failedSequence })
      }
      for (const entry of keys.values()) {
        failedSequence = entry.sequence
        if (context && context.getRemainingTimeInMillis() < (options.minRemainingTimeMs ?? 15_000)) throw new Error('Insufficient router budget')
        await dispatchKey(entry.key, options.transport)
        metrics.keys++
      }
    } catch (error) {
      // Pending keys may precede a failed application record. Retry from the earliest
      // pending record so none of their scheduling obligations is acknowledged away.
      const pending = [...keys.values()].map(entry => entry.sequence).filter((s): s is string => !!s)
      const sequences = [...pending, ...(failedSequence ? [failedSequence] : [])]
      console.error(JSON.stringify({ kind: 'workflow_router_failed', error: error instanceof Error ? error.name : 'Error' }))
      if (!failedSequence || sequences.some(s => !/^\d+$/.test(s))) throw error
      const first = sequences.reduce((a, b) => BigInt(a) < BigInt(b) ? a : b)
      return { batchItemFailures: [{ itemIdentifier: first }] }
    }
    options.onMetrics?.(metrics)
    return { batchItemFailures: [] }
  }
}

export function createWorkflowWorker(options: {
  store: FencedWorkflowExecutionStore; runtime: WorkflowRuntimeDefinition; transport: WakeupIO;
  publisher: ApplicationEventPublisher; region: string; minRemainingTimeMs?: number
}) {
  return async (batch: QueueBatch, context: WorkerContext) => {
    if (!Array.isArray(batch.Records)) throw new Error('Expected an SQS wakeup batch')
    const batchItemFailures: { itemIdentifier: string }[] = []
    for (const message of batch.Records) {
      try {
        if (message.eventSource !== 'aws:sqs' || context.getRemainingTimeInMillis() < (options.minRemainingTimeMs ?? 20_000)) throw new Error('Invalid message or insufficient worker budget')
        const owner = `${options.region}:${context.awsRequestId}:${message.messageId}`
        await processWakeup(parseWakeup(JSON.parse(message.body)), options.transport, {
          async processTarget(target) {
            const result = await options.store.withLeaseOwner(owner, () => options.runtime.processTarget({ target,
              leaseOwner: owner, maxDurationMs: context.getRemainingTimeInMillis() - 15_000, includeEvents: false }))
            const deferred = [...result.recovered, ...result.timers, ...result.scheduled].flatMap(run => run.events.filter(event => event.type === 'RUN_ERRORED'))
            console.log(JSON.stringify({ kind: deferred.length ? 'workflow_deferred' : 'workflow_processed', target, owner, summary: result.summary, deferred }))
          },
          drainEffects: runId => options.store.drainEffects(runId, options.publisher, 10, Date.now() + context.getRemainingTimeInMillis() - 15_000),
          cleanupItem: PK => options.store.cleanupItem(PK, Date.now(), 25, Date.now() + context.getRemainingTimeInMillis() - 15_000),
        })
      } catch (error) {
        console.error(JSON.stringify({ kind: 'workflow_wakeup_failed', messageId: message.messageId, error: error instanceof Error ? error.name : 'Error' }))
        batchItemFailures.push({ itemIdentifier: message.messageId })
      }
    }
    return { batchItemFailures }
  }
}

/** Downstream business handlers receive at-least-once envelopes from their own queue. */
export function createApplicationQueueHandler(handler: ApplicationEventHandler) {
  return async (batch: QueueBatch, context?: Pick<WorkerContext, 'getRemainingTimeInMillis'>) => {
    const batchItemFailures: { itemIdentifier: string }[] = []
    for (const message of batch.Records) {
      try {
        if (message.eventSource !== 'aws:sqs' || (context && context.getRemainingTimeInMillis() < 15_000)) throw new Error('Invalid application message or insufficient budget')
        const event = JSON.parse(message.body)
        serializeApplicationEvent(event)
        await handler(event)
      } catch (error) {
        console.error(JSON.stringify({ kind: 'application_event_delivery_failed', messageId: message.messageId, error: error instanceof Error ? error.name : 'Error' }))
        batchItemFailures.push({ itemIdentifier: message.messageId })
      }
    }
    return { batchItemFailures }
  }
}
