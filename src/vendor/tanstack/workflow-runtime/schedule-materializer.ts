import { nextScheduleTime } from '../../../schedules.js'
import type { WorkflowRegistrationMap, WorkflowRuntimeDefinition } from './types.js'

export interface MaterializeWorkflowSchedulesOptions { now?: number }
export interface MaterializedWorkflowSchedule { workflowId: string; scheduleId: string; fireAt: number; kind: 'materialized' | 'disabled' }

/** Seed future deadlines on deployment/configuration changes; workers advance each tick. */
export async function materializeWorkflowSchedules<T extends WorkflowRegistrationMap>(runtime: WorkflowRuntimeDefinition<T>,
  options: MaterializeWorkflowSchedulesOptions = {}): Promise<MaterializedWorkflowSchedule[]> {
  const now = options.now ?? Date.now()
  const result: MaterializedWorkflowSchedule[] = []
  for (const [workflowId, registration] of Object.entries(runtime.workflows)) {
    for (const [index, definition] of (registration.schedules ?? []).entries()) {
      const scheduleId = definition.id ?? `${workflowId}:${index}`
      const fireAt = nextScheduleTime(definition.schedule, now)
      await runtime.store.upsertSchedule({ scheduleId, workflowId, workflowVersion: registration.version,
        schedule: definition.schedule, overlapPolicy: definition.overlapPolicy ?? 'skip',
        missedTickPolicy: definition.missedTickPolicy, maxCatchUp: definition.maxCatchUp,
        input: typeof definition.input === 'function' ? await definition.input() : definition.input,
        nextFireAt: fireAt, enabled: definition.enabled !== false, now })
      result.push({ workflowId, scheduleId, fireAt, kind: definition.enabled === false ? 'disabled' : 'materialized' })
    }
  }
  return result
}
