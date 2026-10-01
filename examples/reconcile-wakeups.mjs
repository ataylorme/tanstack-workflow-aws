import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

assert.ok(process.argv.slice(2).every(arg => arg === '--execute'), 'Only --execute is supported')
if (!process.argv.includes('--execute')) {
  console.log('Plan only: scan workflow, outbox and retention metadata and invoke the regional router to seed missing wakeups. No data deletion. Set AWS_PROFILE, AWS_REGION, EXPECTED_AWS_ACCOUNT_ID, TABLE_NAME, ROUTER_FUNCTION_NAME, then pass --execute. Run independently in both application Regions.')
  process.exit(0)
}
const { AWS_PROFILE: profile, AWS_REGION: region, EXPECTED_AWS_ACCOUNT_ID: account,
  TABLE_NAME: tableName, ROUTER_FUNCTION_NAME: router } = process.env
assert.ok(profile, 'Explicit AWS_PROFILE required')
assert.ok(['us-east-1', 'us-west-2'].includes(region), 'Choose an application Region from the example topology')
assert.match(account ?? '', /^\d{12}$/, 'EXPECTED_AWS_ACCOUNT_ID required')
assert.match(tableName ?? '', /^[a-zA-Z0-9_.-]{3,255}$/, 'TABLE_NAME required')
assert.match(router ?? '', /^[a-zA-Z0-9_-]{1,64}$/, 'ROUTER_FUNCTION_NAME required')
function aws(args) {
  return JSON.parse(execFileSync('aws', ['--profile', profile, '--region', region, ...args, '--output', 'json'], {
    encoding: 'utf8', env: { ...process.env, AWS_PAGER: '' }, maxBuffer: 8 * 1024 * 1024,
  }))
}
assert.equal(aws(['sts', 'get-caller-identity']).Account, account, 'AWS account guard failed')
const table = aws(['dynamodb', 'describe-table', '--table-name', tableName]).Table
assert.ok(table.TableStatus === 'ACTIVE' && table.MultiRegionConsistency === 'STRONG', 'Expected ACTIVE MRSC table')
const fn = aws(['lambda', 'get-function-configuration', '--function-name', router])
assert.equal(fn.Environment?.Variables?.TABLE_NAME, tableName, 'Router/table mismatch')
assert.equal(fn.Handler, 'router.handler', 'Expected the workflow wakeup router')
// Never write private deployment evidence into a directory Git would publish.
execFileSync('git', ['check-ignore', '.deploy/workflow-wakeups/reconcile.json'], { stdio: 'pipe' })
const dir = resolve('.deploy/workflow-wakeups', `reconcile-${Date.now()}`)
mkdirSync(dir, { recursive: true, mode: 0o700 })
let cursor
let reconciled = 0
let pages = 0
const save = extra => writeFileSync(resolve(dir, 'result.json'), JSON.stringify({ region, pages, reconciled, ...extra }, null, 2), { mode: 0o600 })
try {
  do {
    const page = aws(['dynamodb', 'scan', '--table-name', tableName, '--consistent-read',
      '--projection-expression', 'PK, SK', '--filter-expression', 'SK = :meta',
      '--expression-attribute-values', JSON.stringify({ ':meta': { S: 'META' } }), '--limit', '100', '--no-paginate', ...(cursor ? ['--exclusive-start-key', JSON.stringify(cursor)] : [])])
    const keys = page.Items.map(item => ({ PK: item.PK.S, SK: item.SK.S }))
    for (let i = 0; i < keys.length; i += 10) {
      const batch = keys.slice(i, i + 10)
      const responseFile = resolve(dir, 'response.json')
      writeFileSync(responseFile, '', { mode: 0o600 })
      const result = aws(['lambda', 'invoke', '--function-name', router, '--cli-binary-format', 'raw-in-base64-out',
        '--payload', JSON.stringify({ kind: 'reconcile', keys: batch }), responseFile])
      assert.equal(result.FunctionError, undefined, 'Router failed; safely rerun reconciliation')
      assert.equal(JSON.parse(readFileSync(responseFile, 'utf8')).reconciled, batch.length)
      reconciled += batch.length
      save({ complete: false })
    }
    pages++
    cursor = page.LastEvaluatedKey && Object.keys(page.LastEvaluatedKey).length ? page.LastEvaluatedKey : undefined
  } while (cursor)
  save({ complete: true, completedAt: new Date().toISOString() })
  console.log(`Reconciled ${reconciled} metadata items across ${pages} pages; private evidence: ${dir}`)
} catch (error) {
  save({ complete: false, error: error instanceof Error ? error.message : String(error) })
  throw error
}
