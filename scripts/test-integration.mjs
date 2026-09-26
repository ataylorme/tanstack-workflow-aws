import { spawn } from 'node:child_process'
import { createServer } from 'node:net'
import { dirname, resolve } from 'node:path'

let server
const run = () => new Promise(resolveRun => {
  const test = spawn(process.execPath, ['node_modules/vitest/vitest.mjs', 'run'], { stdio: 'inherit', env: process.env })
  test.on('exit', code => resolveRun(code ?? 1))
  test.on('error', () => resolveRun(1))
})
try {
  if (!process.env.DYNAMODB_ENDPOINT) {
    const jar = process.env.DYNAMODB_LOCAL_JAR
    if (!jar) throw new Error('Set DYNAMODB_LOCAL_JAR to DynamoDBLocal.jar or DYNAMODB_ENDPOINT to an existing local emulator')
    const port = await new Promise((resolvePort, reject) => {
      const socket = createServer(); socket.on('error', reject)
      socket.listen(0, '127.0.0.1', () => { const value = socket.address().port; socket.close(() => resolvePort(value)) })
    })
    process.env.DYNAMODB_ENDPOINT = `http://127.0.0.1:${port}`
    server = spawn('java', [`-Djava.library.path=${dirname(resolve(jar))}/DynamoDBLocal_lib`, '-jar', resolve(jar), '-inMemory', '-sharedDb', '-port', String(port)], { stdio: ['ignore', 'pipe', 'pipe'] })
    let failure
    server.on('error', error => { failure = error })
    server.stdout.resume()
    server.stderr.on('data', chunk => process.stderr.write(chunk))
    let ready = false
    for (let attempt = 0; attempt < 300; attempt++) {
      if (failure || server.exitCode !== null) throw failure ?? new Error('DynamoDB Local exited before becoming ready')
      try { await fetch(process.env.DYNAMODB_ENDPOINT); ready = true; break } catch { await new Promise(r => setTimeout(r, 100)) }
    }
    if (!ready) throw new Error('DynamoDB Local did not start within thirty seconds')
  }
  process.exitCode = await run()
} catch (error) { console.error(error.message); process.exitCode = 1 }
finally { server?.kill() }
