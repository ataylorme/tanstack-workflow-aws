import { createApplicationQueueHandler } from '../src/wakeups.js'
import { routeApplicationEvent } from '../src/events.js'
import type { ApplicationEvent } from '../src/events.js'

// Replace these with application-specific notification and projection services.
async function notify(event: ApplicationEvent) {
  console.log('notify', event.id, event.data)
}
async function project(event: ApplicationEvent) {
  console.log('project', event.id, event.data)
}

// Subscribe separate queues to the application topic for independent retries.
// Every real consumer must deduplicate by event.id.
export const notificationHandler = createApplicationQueueHandler(event =>
  routeApplicationEvent(event, [
    { type: 'task.requested', handler: notify },
    { type: 'task.completed', handler: notify },
  ]).then(() => {}),
)
export const projectionHandler = createApplicationQueueHandler(event =>
  routeApplicationEvent(event, [
    { type: 'task.approved', handler: project },
    { type: 'task.started', handler: project },
    { type: 'task.completed', handler: project },
  ]).then(() => {}),
)
