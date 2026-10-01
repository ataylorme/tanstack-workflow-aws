import { applicationEventGroupId } from '../src/ordered-events.js'
import { SNSClient } from '@aws-sdk/client-sns'
import { createApplicationQueueHandler } from '../src/wakeups.js'
import { createSnsBridge } from '../src/bridges-sns.js'
export const handler = createApplicationQueueHandler(createSnsBridge({ topicArn: process.env.APPLICATION_TOPIC_ARN!, messageGroupId: applicationEventGroupId,
  client: new SNSClient({ maxAttempts: 2, requestHandler: { connectionTimeout: 1_000, requestTimeout: 4_000, throwOnRequestTimeout: true } }),
}), { fifo: true })
