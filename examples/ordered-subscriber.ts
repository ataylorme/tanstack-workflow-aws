import { createDynamoOrderedSubscriber } from '../src/ordered-events.js'

// Both regional deployments of this logical subscriber MUST share SUBSCRIBER_ID.
const subscriber = createDynamoOrderedSubscriber({
  tableName: process.env.TABLE_NAME!, subscriberId: process.env.SUBSCRIBER_ID!,
  async handler(event, delivery) {
    // Replace with your awaited, idempotent business operation. Do not detach work.
    // Persist/use delivery.idempotencyKey at the destination for uncertain retries.
    console.log(JSON.stringify({ kind: 'ordered_event_observed', streamId: event.ordering.streamId,
      sequence: event.ordering.sequence, type: event.type, idempotencyKey: delivery.idempotencyKey }))
  },
})
export const handler = subscriber.handler
