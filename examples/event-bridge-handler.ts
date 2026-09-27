import { createEventBridgeBridge } from '../src/bridges-eventbridge.js'
import { createSnsBridge } from '../src/bridges-sns.js'
import { createSqsBridge } from '../src/bridges-sqs.js'
import { createWebhookBridge } from '../src/bridges-webhook.js'
import { createApplicationStreamHandler } from '../src/event-stream.js'
import type { ApplicationEventHandler } from '../src/events.js'

// Deploy this bundle as a separate Lambda for each destination. Set BRIDGE_KIND
// and the corresponding target variable in that Lambda's environment.
const kind = process.env.BRIDGE_KIND
let bridge: ApplicationEventHandler
switch (kind) {
  case 'eventbridge':
    bridge = createEventBridgeBridge({ eventBusName: process.env.EVENT_BUS_NAME!, source: process.env.EVENT_SOURCE! })
    break
  case 'sns':
    bridge = createSnsBridge({ topicArn: process.env.TOPIC_ARN! })
    break
  case 'sqs':
    bridge = createSqsBridge({ queueUrl: process.env.QUEUE_URL! })
    break
  case 'webhook':
    bridge = createWebhookBridge({
      url: process.env.WEBHOOK_URL!,
      headers: process.env.WEBHOOK_TOKEN ? { authorization: `Bearer ${process.env.WEBHOOK_TOKEN}` } : undefined,
    })
    break
  default: throw new Error(`Unknown BRIDGE_KIND: ${kind}`)
}

export const handler = createApplicationStreamHandler(bridge)
