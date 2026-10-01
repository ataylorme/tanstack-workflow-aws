import { SNSClient } from '@aws-sdk/client-sns'
import { createApplicationQueueHandler } from '../src/wakeups.js'
import { createSnsBridge } from '../src/bridges-sns.js'
export const handler = createApplicationQueueHandler(createSnsBridge({ topicArn: process.env.APPLICATION_TOPIC_ARN!,
  client: new SNSClient({ maxAttempts: 2, requestHandler: { connectionTimeout: 1_000, requestTimeout: 4_000, throwOnRequestTimeout: true } }),
}))
