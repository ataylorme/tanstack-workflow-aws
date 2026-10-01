import { serializeApplicationEvent } from './event-validation.js'
import type { ApplicationEventHandler } from './events.js'

export interface WebhookBridgeOptions {
  url: string
  fetch?: typeof globalThis.fetch
  headers?: Record<string, string>
  timeoutMs?: number
}

export function createWebhookBridge(options: WebhookBridgeOptions): ApplicationEventHandler {
  const url = new URL(options.url)
  if (url.protocol !== 'https:') throw new Error('webhook url must use HTTPS')
  if (url.username || url.password) throw new Error('webhook URL must not contain credentials')
  const send = options.fetch ?? globalThis.fetch
  const timeoutMs = options.timeoutMs ?? 10_000
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647) throw new Error('timeoutMs must be positive')
  return async event => {
    const body = serializeApplicationEvent(event)
    const headers = new Headers(options.headers)
    headers.set('content-type', 'application/json')
    headers.set('x-event-id', encodeURIComponent(event.id))
    const response = await send(url, {
      method: 'POST',
      headers,
      body,
      signal: AbortSignal.timeout(timeoutMs),
      redirect: 'error',
    })
    // Release the connection without buffering an untrusted response body.
    await response.body?.cancel()
    if (!response.ok) throw new Error(`Webhook returned HTTP ${response.status}`)
  }
}
