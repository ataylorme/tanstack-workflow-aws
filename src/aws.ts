import { createHash } from 'node:crypto'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb'
import { CreateScheduleCommand, GetScheduleCommand, SchedulerClient } from '@aws-sdk/client-scheduler'
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs'
import type { ApplicationEvent } from './events.js'
import { serializeApplicationEvent } from './event-validation.js'
import type { DueWakeup, WakeupIO } from './wakeups.js'
export interface WakeupConfig {
  client?: DynamoDBDocumentClient; scheduler?: SchedulerClient; sqs?: SQSClient;
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

export function createAwsWorkflowTransport(config: WakeupConfig): WakeupIO {
  const options = { maxAttempts: 3,
    requestHandler: { connectionTimeout: 1_000, requestTimeout: 4_000, throwOnRequestTimeout: true } }
  const db = config.client ?? DynamoDBDocumentClient.from(new DynamoDBClient(options))
  const scheduler = config.scheduler ?? new SchedulerClient(options)
  const sqs = config.sqs ?? new SQSClient(options)
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


export function createApplicationQueuePublisher(options: { queueUrl: string; client?: SQSClient }) {
  const client = options.client ?? new SQSClient({ maxAttempts: 3,
    requestHandler: { connectionTimeout: 1000, requestTimeout: 4000, throwOnRequestTimeout: true } })
  return async (event: ApplicationEvent) => {
    await client.send(new SendMessageCommand({ QueueUrl: options.queueUrl, MessageBody: serializeApplicationEvent(event) }))
  }
}
