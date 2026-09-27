import { createApplicationStreamHandler } from '../src/event-stream.js'
import { routeApplicationEvent } from '../src/events.js'
import type { ApplicationEvent } from '../src/events.js'

// Replace these with application-specific notification and projection services.
async function notify(event: ApplicationEvent) {
  console.log('notify', event.id, event.data)
}
async function project(event: ApplicationEvent) {
  console.log('project', event.id, event.data)
}

// In production, give each side effect its own Lambda stream mapping so a
// notification outage cannot block a projection; deduplicate by event.id.
export const notificationHandler = createApplicationStreamHandler(event =>
  routeApplicationEvent(event, [
    { type: 'task.requested', handler: notify },
    { type: 'task.completed', handler: notify },
  ]).then(() => {}),
)
export const projectionHandler = createApplicationStreamHandler(event =>
  routeApplicationEvent(event, [
    { type: 'task.approved', handler: project },
    { type: 'task.started', handler: project },
    { type: 'task.completed', handler: project },
  ]).then(() => {}),
)
