import { createWorkflow } from '../src/workflow.js'
import { defineWorkflowRuntime } from '../src/runtime.js'
import { createDynamoWorkflowExecutionStore } from '../src/index.js'

export const store = createDynamoWorkflowExecutionStore({ tableName: process.env.TABLE_NAME! })
const task = createWorkflow({ id: 'task' }).handler(async ctx => {
  // Replace with a real idempotent step. The workflow may resume in either Region.
  const signal = await ctx.waitForEvent<{ message: string }>('complete')
  return { message: signal.message }
})
export const runtime = defineWorkflowRuntime({ store, workflows: { task: { load: async () => task } } })
