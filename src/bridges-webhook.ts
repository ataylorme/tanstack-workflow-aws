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
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error('timeoutMs must be positive')
  return async event => {
    const response = await send(url, {
      method: 'POST',
      headers: { ...options.headers, 'content-type': 'application/json', 'x-event-id': event.id },
      body: JSON.stringify(event),
      signal: AbortSignal.timeout(timeoutMs),
      redirect: 'error',
    })
    if (!response.ok) throw new Error(`Webhook returned HTTP ${response.status}`)
  }
}
