import { createHash } from 'node:crypto'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb'
import { CreateScheduleCommand, GetScheduleCommand, SchedulerClient } from '@aws-sdk/client-scheduler'
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs'

export const dueKinds = ['RUNNING', 'TIMER_RUN', 'TIMER', 'SCHEDULE'] as const
type DueKind = typeof dueKinds[number]
export interface ItemKey { PK: string; SK: string }
export type DueWakeup = { version: 1; kind: 'due'; key: ItemKey; dueKind: DueKind; dueAt: number }
export type Wakeup = DueWakeup | { version: 1; kind: 'drain' }
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
  if (typeof key.PK !== 'string' || !/^(RUN|TIMER|SCHEDULE)#/.test(key.PK) ||
      key.PK.length > 2048 || key.SK !== 'META') throw new Error('Invalid workflow metadata key')
  return { PK: key.PK, SK: key.SK }
}

export function parseWakeup(value: unknown): Wakeup {
  const input = record(value)
  if (input.version !== 1) throw new Error('Unsupported wakeup version')
  if (input.kind === 'drain') return { version: 1, kind: 'drain' }
  if (input.kind !== 'due' || !dueKinds.includes(input.dueKind as DueKind) ||
      typeof input.dueAt !== 'number' || !Number.isFinite(input.dueAt) || input.dueAt < 0) throw new Error('Invalid wakeup')
  return { version: 1, kind: 'due', key: parseKey(input.key), dueKind: input.dueKind as DueKind, dueAt: input.dueAt }
}

// The installed store's due fields are a version-pinned storage contract. In
// particular timerLease is NOT reflected in dueSK, so it must be considered here.
export function dueWakeup(item: Record<string, unknown> | undefined): DueWakeup | undefined {
  if (!item || item.deleted || !dueKinds.includes(item.duePK as DueKind) ||
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
  const wakeup = dueWakeup(await io.read(key))
  if (!wakeup) return
  if (wakeup.dueAt <= now) await io.enqueue(wakeup)
  else await io.schedule(wakeup, wakeup.dueAt)
}

export async function processWakeup(wakeup: Wakeup, io: WakeupIO,
  sweep: () => Promise<{ remainingMayExist: boolean }>, now = Date.now): Promise<void> {
  if (wakeup.kind === 'due') {
    const current = dueWakeup(await io.read(wakeup.key))
    if (!current) return
    if (current.dueAt > now()) { await io.schedule(current, current.dueAt); return }
  }
  const result = await sweep()
  if (wakeup.kind === 'due') {
    const pending = dueWakeup(await io.read(wakeup.key))
    // An empty eventual GSI result is NOT evidence that this base item is done.
    // Persist a new, future successor before acknowledging the current message.
    if (pending) await io.schedule(pending, Math.max(pending.dueAt, now() + 60_000))
  }
  if (result.remainingMayExist) await io.enqueue({ version: 1, kind: 'drain' }, 1)
}

export interface WakeupConfig {
  tableName: string; queueUrl: string; queueArn: string; group: string; roleArn: string; dlqArn: string
}
export function scheduleInput(config: WakeupConfig, wakeup: DueWakeup, at: number) {
  const fireAt = Math.ceil(at / 1000) * 1000
  const input = JSON.stringify(wakeup)
  const name = `wake-${createHash('sha256').update(JSON.stringify([config.tableName, wakeup, fireAt])).digest('hex').slice(0, 56)}`
  return {
    Name: name, GroupName: config.group,
    ScheduleExpression: `at(${new Date(fireAt).toISOString().slice(0, 19)})`,
    ScheduleExpressionTimezone: 'UTC', FlexibleTimeWindow: { Mode: 'OFF' as const },
    ActionAfterCompletion: 'DELETE' as const, State: 'ENABLED' as const,
    Target: { Arn: config.queueArn, RoleArn: config.roleArn, Input: input,
      DeadLetterConfig: { Arn: config.dlqArn },
      RetryPolicy: { MaximumEventAgeInSeconds: 86400, MaximumRetryAttempts: 185 } },
  }
}

export function createWakeupIO(config: WakeupConfig): WakeupIO {
  const options = { maxAttempts: 3,
    requestHandler: { connectionTimeout: 1_000, requestTimeout: 4_000, throwOnRequestTimeout: true } }
  const db = DynamoDBDocumentClient.from(new DynamoDBClient(options))
  const scheduler = new SchedulerClient(options)
  const sqs = new SQSClient(options)
  return {
    async read(key) {
      return (await db.send(new GetCommand({ TableName: config.tableName, Key: key, ConsistentRead: true }))).Item
    },
    async enqueue(wakeup, delaySeconds = 0) {
      await sqs.send(new SendMessageCommand({ QueueUrl: config.queueUrl, MessageBody: JSON.stringify(wakeup), DelaySeconds: delaySeconds }))
    },
    async schedule(wakeup, at) {
      // Scheduling a deadline that elapsed during a read/API retry must not
      // create a one-time schedule in the past (which would never fire).
      // Very short deadlines use SQS's delay rather than racing Scheduler's
      // create request against a timestamp only a second or two in the future.
      if (at <= Date.now() + 15_000) {
        await sqs.send(new SendMessageCommand({ QueueUrl: config.queueUrl, MessageBody: JSON.stringify(wakeup),
          DelaySeconds: Math.max(0, Math.ceil((at - Date.now()) / 1000)) }))
        return
      }
      const input = scheduleInput(config, wakeup, at)
      try { await scheduler.send(new CreateScheduleCommand(input)) }
      catch (error) {
        if (!(error instanceof Error) || error.name !== 'ConflictException') throw error
        const existing = await scheduler.send(new GetScheduleCommand({ Name: input.Name, GroupName: input.GroupName }))
        if (existing.ScheduleExpression !== input.ScheduleExpression || existing.State !== 'ENABLED' ||
            existing.ActionAfterCompletion !== 'DELETE' || existing.Target?.Arn !== input.Target.Arn ||
            existing.Target?.RoleArn !== input.Target.RoleArn || existing.Target?.Input !== input.Target.Input ||
            existing.Target?.DeadLetterConfig?.Arn !== input.Target.DeadLetterConfig.Arn) {
          throw new Error('Conflicting wakeup schedule configuration')
        }
      }
      // SDK retries can finish AFTER the requested timestamp. Do not ACK a
      // schedule created in the past: persist a queue fallback. If Scheduler
      // also delivered, the runtime's lease/deduplication makes this harmless.
      if (Date.now() >= Math.ceil(at / 1000) * 1000) {
        await sqs.send(new SendMessageCommand({ QueueUrl: config.queueUrl, MessageBody: JSON.stringify(wakeup) }))
      }
    },
  }
}

let io: WakeupIO | undefined
export function wakeupIO(): WakeupIO {
  const required = (name: string) => {
    const value = process.env[name]
    if (!value) throw new Error(`${name} is required`)
    return value
  }
  return io ??= createWakeupIO({ tableName: required('TABLE_NAME'), queueUrl: required('WAKEUP_QUEUE_URL'),
    queueArn: required('WAKEUP_QUEUE_ARN'), group: required('SCHEDULE_GROUP'),
    roleArn: required('SCHEDULER_ROLE_ARN'), dlqArn: required('SCHEDULER_DLQ_ARN') })
}
