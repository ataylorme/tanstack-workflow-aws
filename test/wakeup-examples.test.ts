import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

const sweeper = readFileSync('cloudformation/sweeper.yaml', 'utf8')
const regional = readFileSync('cloudformation/regional.yaml', 'utf8')
describe('demand-driven deployment examples', () => {
  it('does not enable recurring polling by default in either template', () => {
    for (const template of [sweeper, regional]) {
      expect(template).toContain('Default: removed')
      expect(template).toMatch(/SweepRule:\s+Condition: KeepLegacySchedule/)
      expect(template).toContain('State: !If [EnableLegacySchedule, ENABLED, DISABLED]')
    }
    expect(regional).toMatch(/SweepWorker:\s+Condition: KeepLegacySchedule/)
  })
  it('wires the bundled dispatcher and worker to regional streams, queues and schedules', () => {
    expect(sweeper).toContain('Handler: dispatcher.handler')
    expect(sweeper).toContain('Handler: sweeper.handler')
    expect(sweeper).toContain('EventSourceArn: !Ref StreamArn')
    expect(sweeper).toContain('EventSourceArn: !GetAtt WakeupQueue.Arn')
    expect(sweeper).toContain('VisibilityTimeout: 360')
    expect(sweeper).toContain('ScalingConfig: { MaximumConcurrency: 2 }')
    expect(sweeper).toContain('"RUNNING","TIMER_RUN","TIMER","SCHEDULE"')
    expect(sweeper.match(/FunctionResponseTypes: \[ReportBatchItemFailures\]/g)).toHaveLength(2)
    expect(sweeper.match(/Type: AWS::CloudWatch::Alarm/g)).toHaveLength(10)
    expect(sweeper).toContain("'iam:PassedToService': scheduler.amazonaws.com")
    for (const name of ['TABLE_NAME','WAKEUP_QUEUE_URL','WAKEUP_QUEUE_ARN','SCHEDULE_GROUP','SCHEDULER_ROLE_ARN','SCHEDULER_DLQ_ARN']) {
      expect(sweeper.match(new RegExp(`\\b${name}:`, 'g'))).toHaveLength(2)
    }
  })
  it('keeps application events on their separate INSERT-only bridge', () => {
    const bridge = readFileSync('cloudformation/event-bridge-consumer.yaml', 'utf8')
    expect(bridge).toContain('APPLICATION_EVENT')
    expect(bridge).not.toContain('ScheduleExpression')
    expect(sweeper).not.toContain('APPLICATION_EVENT')
  })
  it('plans reconciliation offline and rejects missing execution guards before AWS access', () => {
    const run = (args: string[]) => execFileSync(process.execPath, ['examples/reconcile-wakeups.mjs', ...args], { env: { PATH: '' }, encoding: 'utf8', stdio: 'pipe' })
    expect(run([])).toContain('Plan only')
    expect(() => run(['--execute'])).toThrow('Explicit AWS_PROFILE required')
    expect(() => run(['--unexpected'])).toThrow('Only --execute is supported')
  })
})
