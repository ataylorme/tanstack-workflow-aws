import { createAwsWorkflowTransport } from '../src/aws.js'
export const required = (name: string) => {
  const value = process.env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}
export const transport = createAwsWorkflowTransport({ tableName: required('TABLE_NAME'),
  queueUrl: required('WAKEUP_QUEUE_URL'), queueArn: required('WAKEUP_QUEUE_ARN'),
  group: required('SCHEDULE_GROUP'), roleArn: required('SCHEDULER_ROLE_ARN'), dlqArn: required('SCHEDULER_DLQ_ARN') })
