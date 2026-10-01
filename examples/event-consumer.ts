import { createDynamoOrderedSubscriber } from '../src/ordered-events.js'
import { routeApplicationEvent } from '../src/events.js'
import type { ApplicationEvent } from '../src/events.js'

// Replace these with application-specific notification and projection services.
async function notify(event: ApplicationEvent) {
  console.log('notify', event.id, event.data)
}
async function project(event: ApplicationEvent) {
  console.log('project', event.id, event.data)
}

// Deploy each logical subscriber behind its own FIFO subscription queue.
// Use the same subscriberId in both Regions; each callback awaits its complete effect.
export const notificationHandler = createDynamoOrderedSubscriber({
  tableName: process.env.TABLE_NAME!, subscriberId: 'task-notifications',
  handler: event => routeApplicationEvent(event, [
    { type: 'task.requested', handler: notify },
    { type: 'task.approved', handler: notify },
    { type: 'task.started', handler: notify },
    { type: 'task.completed', handler: notify },
  ]).then(() => {}),
}).handler
export const projectionHandler = createDynamoOrderedSubscriber({
  tableName: process.env.TABLE_NAME!, subscriberId: 'task-projection',
  handler: event => routeApplicationEvent(event, [
    { type: 'task.requested', handler: project },
    { type: 'task.approved', handler: project },
    { type: 'task.started', handler: project },
    { type: 'task.completed', handler: project },
  ]).then(() => {}),
}).handler
