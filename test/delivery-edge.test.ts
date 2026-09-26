import { readFileSync } from 'node:fs'
import vm from 'node:vm'
import { describe, expect, it } from 'vitest'

// Execute the actual inline edge handler from the deployment template so the
// deployed code, rather than a second implementation, is exercised.
const template = readFileSync(new URL('../cloudformation/edge.yaml', import.meta.url), 'utf8')
const inline = template.match(/ZipFile: !Sub \|\n([\s\S]*?)\n  RouterVersion:/)?.[1]
if (!inline) throw new Error('Missing inline routing function')
const code = inline.split('\n').map(line => line.slice(10)).join('\n')
  .replaceAll('${WestApiDomain}', 'west.example.test')
  .replaceAll('${EastApiDomain}', 'east.example.test')
const exports: { handler?: (event: unknown) => Promise<any> } = {}
vm.runInNewContext(code, { exports })
const route = async (id: string, uri = '/runs') => {
  const request = { uri, method: 'POST', headers: { 'x-workflow-run-id': [{ key: 'x-workflow-run-id', value: id }] }, body: { data: 'unchanged' } }
  return exports.handler!({ Records: [{ cf: { request } }] })
}

describe('deployed Lambda@Edge router', () => {
  it('routes to both regions and rewrites the host to the selected TLS origin', async () => {
    const origins = new Set<string>()
    for (const id of ['a', 'b']) {
      const result = await route(id)
      origins.add(result.origin.custom.domainName)
      expect(result.headers.host[0].value).toBe(result.origin.custom.domainName)
      expect(result.origin.custom.protocol).toBe('https')
      expect(result.body.data).toBe('unchanged')
    }
    expect(origins).toEqual(new Set(['east.example.test', 'west.example.test']))
  })

  it('keeps explicit run affinity across request paths', async () => {
    const start = await route('stable-run')
    const signal = await route('stable-run', '/runs/stable-run/signals')
    expect(start.origin.custom.domainName).toBe(signal.origin.custom.domainName)
  })
})
