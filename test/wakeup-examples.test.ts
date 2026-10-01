import { execFileSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
const workers = readFileSync('cloudformation/workers.yaml', 'utf8')
it('installs one stream reader and two independent queue consumers per replica', () => {
  expect(workers.match(/EventSourceArn: !Ref StreamArn/g)).toHaveLength(1)
  expect(workers).toContain('EventSourceArn: !GetAtt WakeupQueue.Arn')
  expect(workers).toContain('EventSourceArn: !GetAtt ApplicationQueue.Arn')
  expect(workers.match(/FunctionResponseTypes: \[ReportBatchItemFailures\]/g)).toHaveLength(3)
  expect(workers).not.toContain('ScheduleExpression: rate')
  expect(readFileSync('cloudformation/global-table.yaml', 'utf8')).toContain('NEW_AND_OLD_IMAGES')
  for (const name of ['worker', 'router', 'application-consumer']) expect(workers).toContain(`Handler: ${name}.handler`)
})
it('plans reconciliation without accessing AWS', () => {
  const run = (args: string[]) => execFileSync(process.execPath, ['examples/reconcile-wakeups.mjs', ...args], { env: { PATH: '' }, encoding: 'utf8', stdio: 'pipe' })
  expect(run([])).toContain('Plan only')
  expect(() => run(['--execute'])).toThrow('Explicit AWS_PROFILE required')
})
