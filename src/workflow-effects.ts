import { createHash } from 'node:crypto'
import type { Ctx, WorkflowEvent } from './workflow.js'
import type { ApplicationEvent, PublishApplicationEventOptions } from './events.js'
import { serializeApplicationEvent } from './event-validation.js'

const hash = (value: string) => createHash('sha256').update(value).digest('hex')
export interface PublicationIntent { $workflowEffect: 'publish'; event: ApplicationEvent }
export interface ContinuationIntent { $workflowEffect: 'continue'; runId: string; input: unknown }
export type WorkflowEffect = PublicationIntent | ContinuationIntent

/** The step checkpoint is the outbox commit; this function performs no external I/O. */
export async function publishWorkflowEvent<T>(ctx: Pick<Ctx<any, any>, 'step' | 'runId'>,
  name: string, input: PublishApplicationEventOptions<T>): Promise<ApplicationEvent<T>> {
  const intent = await ctx.step(name, step => {
    const event = JSON.parse(serializeApplicationEvent({ ...input,
      id: input.id ?? `workflow-${hash(step.id)}`, version: input.version ?? 1,
      timestamp: input.timestamp ?? new Date().toISOString(),
      correlationId: input.correlationId ?? ctx.runId,
    })) as ApplicationEvent<T>
    return { $workflowEffect: 'publish' as const, event }
  })
  return intent.event
}

/** Return this value from the workflow handler to durably start a fresh history. */
export async function continueAsNew(ctx: Pick<Ctx<any, any>, 'step'>, input: unknown): Promise<ContinuationIntent> {
  return ctx.step('continue-as-new', step => ({ $workflowEffect: 'continue' as const,
    runId: `continuation-${hash(step.id)}`, input }))
}

export function committedEffect(event: WorkflowEvent): WorkflowEffect | undefined {
  const value = event.type === 'STEP_FINISHED' ? event.result : event.type === 'RUN_FINISHED' ? event.output : undefined
  if (!value || typeof value !== 'object') return undefined
  const intent = value as WorkflowEffect
  if (event.type === 'STEP_FINISHED' && intent.$workflowEffect === 'publish') {
    serializeApplicationEvent(intent.event)
    return intent
  }
  if (event.type === 'RUN_FINISHED' && intent.$workflowEffect === 'continue') {
    if (typeof intent.runId !== 'string' || !intent.runId.startsWith('continuation-')) throw new Error('Invalid continuation')
    return intent
  }
  return undefined
}
