import { execFileSync } from 'node:child_process'
import { mkdtempSync, writeFileSync, rmSync, readFileSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

// Install the actual tarball into an unrelated project: repository-relative imports
// and a pre-existing dist directory must not make a broken package appear usable.
const root = fileURLToPath(new URL('../', import.meta.url))
const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'))
const tarballName = `${manifest.name.replace(/^@/, '').replace('/', '-')}-${manifest.version}.tgz`
const temporary = mkdtempSync(join(tmpdir(), 'workflow-package-'))
try {
  execFileSync('npm', ['pack', '--loglevel', 'error', '--pack-destination', temporary], { cwd: root, stdio: 'inherit' })
  writeFileSync(join(temporary, 'package.json'), JSON.stringify({ private: true, type: 'module' }))
  execFileSync('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', join(temporary, tarballName)], {
    cwd: temporary, stdio: 'inherit',
  })
  for (const service of ['eventbridge', 'sns', 'sqs', 'scheduler']) {
    if (existsSync(join(temporary, 'node_modules/@aws-sdk/client-' + service))) throw new Error('Optional bridge/example SDK unexpectedly installed: ' + service)
  }
  writeFileSync(join(temporary, 'consumer.mjs'), `
import { createDynamoWorkflowExecutionStore } from '@ataylorme/tanstack-workflow-aws'
const { GetCommand } = await import('@aws-sdk/lib-dynamodb')
const { createWorkflow } = await import('@ataylorme/tanstack-workflow-aws/workflow')
const { defineWorkflowRuntime } = await import('@ataylorme/tanstack-workflow-aws/runtime')
if (typeof createWorkflow !== 'function' || typeof defineWorkflowRuntime !== 'function') throw new Error('Missing pinned runtime exports')
const store = createDynamoWorkflowExecutionStore({ tableName: 'consumer-smoke' })
if (typeof store.withLeaseOwner !== 'function' || typeof store.appendEvents !== 'function') {
  throw new Error('Published entry point does not expose the store')
}
const targetedStore = createDynamoWorkflowExecutionStore({ tableName: 'consumer-targeted', client: {
  send: async command => {
    if (!(command instanceof GetCommand) || command.input.ConsistentRead !== true) throw new Error('Targeted processing attempted discovery')
    return {}
  },
} })
const runtime = defineWorkflowRuntime({ store: targetedStore, workflows: {} })
for (const target of [{ kind: 'run', runId: 'missing' }, { kind: 'timer', runId: 'missing' }, { kind: 'timer', runId: 'missing', signalId: 'timer' }, { kind: 'schedule', scheduleId: 'missing' }]) {
  const result = await runtime.processTarget({ target })
  if (result.recovered.length + result.timers.length + result.scheduled.length !== 0 || 'remainingMayExist' in result) throw new Error('Invalid targeted result')
}
const { createDynamoApplicationEventPublisher } = await import('@ataylorme/tanstack-workflow-aws/events')
const { createApplicationStreamHandler } = await import('@ataylorme/tanstack-workflow-aws/event-stream')
const { createWebhookBridge } = await import('@ataylorme/tanstack-workflow-aws/bridges/webhook')
const event = await createDynamoApplicationEventPublisher({ tableName: 'test', client: { send: async () => ({}) } }).publish({ id: 'test', type: 'test', data: { works: true } })
if (event.data.works !== true) throw new Error('Invalid event package export')
if ((await createApplicationStreamHandler(() => {})({ Records: [] })).batchItemFailures.length !== 0) throw new Error('Invalid stream package export')
createWebhookBridge({ url: 'https://example.test' })
console.log('Packed core, events, stream, and webhook imports passed without optional AWS clients')
`)
  execFileSync(process.execPath, ['consumer.mjs'], { cwd: temporary, stdio: 'inherit' })
  // Install each optional service independently; unrelated clients must not be
  // needed just to load that adapter's subpath.
  for (const [service, factory, options] of [
    ['eventbridge', 'createEventBridgeBridge', { eventBusName: 'test', source: 'test' }],
    ['sns', 'createSnsBridge', { topicArn: 'test' }],
    ['sqs', 'createSqsBridge', { queueUrl: 'test' }],
  ]) {
    execFileSync('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', '@aws-sdk/client-' + service + '@' + manifest.peerDependencies['@aws-sdk/client-' + service]], { cwd: temporary, stdio: 'inherit' })
    writeFileSync(join(temporary, 'bridge.mjs'), `import { ${factory} } from '@ataylorme/tanstack-workflow-aws/bridges/${service}'; ${factory}(${JSON.stringify(options)});`)
    execFileSync(process.execPath, ['bridge.mjs'], { cwd: temporary, stdio: 'inherit' })
  }
  execFileSync('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', '@types/aws-lambda@' + manifest.devDependencies['@types/aws-lambda']], { cwd: temporary, stdio: 'inherit' })
  writeFileSync(join(temporary, 'consumer.mts'), `
import { createDynamoWorkflowExecutionStore } from '@ataylorme/tanstack-workflow-aws'
import { defineWorkflowRuntime, type WorkflowExecutionStore, type WorkflowWorkTarget, type WorkflowRuntimeProcessTargetResult } from '@ataylorme/tanstack-workflow-aws/runtime'
const store: WorkflowExecutionStore = createDynamoWorkflowExecutionStore({ tableName: 'consumer-smoke' })
import { createDynamoApplicationEventPublisher } from '@ataylorme/tanstack-workflow-aws/events'
import { createApplicationStreamHandler } from '@ataylorme/tanstack-workflow-aws/event-stream'
import type { DynamoDBStreamHandler } from 'aws-lambda'
const handler: DynamoDBStreamHandler = createApplicationStreamHandler(async event => { void event.data })
const publisher = createDynamoApplicationEventPublisher({ tableName: 'test' })
async function typedPayload() {
  const result = await publisher.publish<{ value: number }>({ type: 'test', data: { value: 1 } })
  const value: number = result.data.value
  return value
}
const runtime = defineWorkflowRuntime({ store, workflows: {} })
async function typedTarget(target: WorkflowWorkTarget): Promise<WorkflowRuntimeProcessTargetResult> {
  return runtime.processTarget({ target, leaseOwner: 'consumer', maxDurationMs: 1000 })
}
void store; void handler; void typedPayload; void typedTarget
`)
  execFileSync(join(root, 'node_modules/.bin/tsc'), ['--noEmit', '--strict', '--exactOptionalPropertyTypes', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--target', 'ES2022', '--skipLibCheck', 'consumer.mts'], {
    cwd: temporary, stdio: 'inherit',
  })
} finally {
  rmSync(temporary, { recursive: true, force: true })
}
