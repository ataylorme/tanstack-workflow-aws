import { serializeApplicationEvent } from './event-validation.js'
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge'
import type { ApplicationEventHandler } from './events.js'

export interface EventBridgeBridgeOptions {
  eventBusName: string
  source: string
  client?: EventBridgeClient
}

/** Forwards the complete application envelope. EventBridge assigns its own transport ID. */
export function createEventBridgeBridge(options: EventBridgeBridgeOptions): ApplicationEventHandler {
  if (!options.eventBusName || !options.source) throw new Error('eventBusName and source are required')
  const client = options.client ?? new EventBridgeClient({})
  return async event => {
    const result = await client.send(new PutEventsCommand({ Entries: [{
      EventBusName: options.eventBusName,
      Source: options.source,
      DetailType: `${event.type}.v${event.version}`,
      Time: new Date(event.timestamp),
      Detail: serializeApplicationEvent(event),
    }] }))
    if (result.FailedEntryCount || result.Entries?.[0]?.ErrorCode || !result.Entries?.[0]?.EventId) {
      throw new Error(`EventBridge publication failed: ${result.Entries?.[0]?.ErrorCode ?? 'missing event ID'}`)
    }
  }
}

