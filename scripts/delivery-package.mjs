import { execFileSync } from 'node:child_process'
import { mkdtempSync, writeFileSync, rmSync, readFileSync } from 'node:fs'
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
  writeFileSync(join(temporary, 'consumer.mjs'), `
import { createDynamoWorkflowExecutionStore } from '@ataylorme/tanstack-workflow-aws'
const { createWorkflow } = await import('@ataylorme/tanstack-workflow-aws/workflow')
const { defineWorkflowRuntime } = await import('@ataylorme/tanstack-workflow-aws/runtime')
if (typeof createWorkflow !== 'function' || typeof defineWorkflowRuntime !== 'function') throw new Error('Missing pinned runtime exports')
const store = createDynamoWorkflowExecutionStore({ tableName: 'consumer-smoke' })
if (typeof store.withLeaseOwner !== 'function' || typeof store.appendEvents !== 'function') {
  throw new Error('Published entry point does not expose the store')
}
console.log('Packed package import and store construction passed')
`)
  execFileSync(process.execPath, ['consumer.mjs'], { cwd: temporary, stdio: 'inherit' })
  writeFileSync(join(temporary, 'consumer.mts'), `
import { createDynamoWorkflowExecutionStore } from '@ataylorme/tanstack-workflow-aws'
import type { WorkflowExecutionStore } from '@ataylorme/tanstack-workflow-aws/runtime'
const store: WorkflowExecutionStore = createDynamoWorkflowExecutionStore({ tableName: 'consumer-smoke' })
void store
`)
  execFileSync(join(root, 'node_modules/.bin/tsc'), ['--noEmit', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--target', 'ES2022', '--skipLibCheck', 'consumer.mts'], {
    cwd: temporary, stdio: 'inherit',
  })
} finally {
  rmSync(temporary, { recursive: true, force: true })
}
