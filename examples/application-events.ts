import { createDynamoApplicationEventPublisher } from '../src/events.js'

// Supply stable event IDs per committed state transition to make producer retries safe.
const publisher = createDynamoApplicationEventPublisher({ tableName: process.env.TABLE_NAME! })

type TaskRequested = { taskId: string; requestedBy: string }
type TaskApproved = { taskId: string; approvedBy: string }
type TaskStarted = { taskId: string; runId: string }
type TaskCompleted = { taskId: string; resultLocation: string }

export async function taskRequested(data: TaskRequested) {
  return publisher.publish({ id: `${data.taskId}:requested`, type: 'task.requested', data })
}
export async function taskApproved(data: TaskApproved) {
  return publisher.publish({ id: `${data.taskId}:approved`, type: 'task.approved', data })
}
export async function taskStarted(data: TaskStarted) {
  return publisher.publish({ id: `${data.taskId}:started:${data.runId}`, type: 'task.started', data })
}
export async function taskCompleted(data: TaskCompleted, runId: string) {
  return publisher.publish({ id: `${data.taskId}:completed:${runId}`, type: 'task.completed', data })
}
