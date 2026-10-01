import { createDynamoApplicationEventPublisher } from '../src/events.js'

// Use a unique task lifecycle ID, including tenant scope. Its four positions are immutable.
// Publish each transition only after its business change commits; retry gaps from a durable outbox.
const publisher = createDynamoApplicationEventPublisher({ tableName: process.env.TABLE_NAME!,
  orderedEventTypes: ['task.requested', 'task.approved', 'task.started', 'task.completed'] })

type TaskRequested = { taskId: string; requestedBy: string }
type TaskApproved = { taskId: string; approvedBy: string }
type TaskStarted = { taskId: string; runId: string }
type TaskCompleted = { taskId: string; resultLocation: string }

export async function taskRequested(data: TaskRequested) {
  return publisher.publish({ id: `${data.taskId}:requested`, type: 'task.requested', ordering: { streamId: data.taskId, sequence: 1 }, data })
}
export async function taskApproved(data: TaskApproved) {
  return publisher.publish({ id: `${data.taskId}:approved`, type: 'task.approved', ordering: { streamId: data.taskId, sequence: 2 }, data })
}
export async function taskStarted(data: TaskStarted) {
  return publisher.publish({ id: `${data.taskId}:started:${data.runId}`, type: 'task.started', ordering: { streamId: data.taskId, sequence: 3 }, data })
}
export async function taskCompleted(data: TaskCompleted, runId: string) {
  return publisher.publish({ id: `${data.taskId}:completed:${runId}`, type: 'task.completed', ordering: { streamId: data.taskId, sequence: 4 }, data })
}
