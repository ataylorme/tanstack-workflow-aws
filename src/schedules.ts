import { CronExpressionParser } from 'cron-parser'
import type { WorkflowScheduleSpec } from './runtime.js'

export type MissedTickPolicy = 'skip' | 'run-once' | 'catch-up'

/** Strictly later than the supplied instant; interval ticks are UTC epoch aligned. */
export function nextScheduleTime(spec: WorkflowScheduleSpec, after: number): number {
  if (!Number.isFinite(after)) throw new TypeError('Invalid schedule timestamp')
  if (spec.timezone) new Intl.DateTimeFormat('en', { timeZone: spec.timezone }).format(0)
  if (spec.kind === 'interval') {
    if (!Number.isSafeInteger(spec.everyMs) || spec.everyMs <= 0) throw new TypeError('everyMs must be a positive safe integer')
    const next = (Math.floor(after / spec.everyMs) + 1) * spec.everyMs
    if (!Number.isSafeInteger(next)) throw new TypeError('Schedule timestamp exceeds safe range')
    return next
  }
  if (spec.expression.trim().split(/\s+/).length !== 5 || /\bH\b/i.test(spec.expression)) throw new TypeError('Use deterministic five-field cron expressions')
  return CronExpressionParser.parse(spec.expression, { currentDate: after, tz: spec.timezone ?? 'UTC' }).next().getTime()
}

export function advanceSchedule(schedule: Record<string, any>, fireAt: number, now: number) {
  let nextFireAt = nextScheduleTime(schedule.spec, fireAt)
  let catchUpRemaining: number | undefined
  if (nextFireAt <= now) {
    if (schedule.missedTickPolicy === 'catch-up') {
      const left = (schedule.catchUpRemaining ?? schedule.maxCatchUp) - 1
      if (left > 0) catchUpRemaining = left
      else nextFireAt = nextScheduleTime(schedule.spec, now)
    } else nextFireAt = nextScheduleTime(schedule.spec, now)
  }
  return { nextFireAt, catchUpRemaining }
}
