import type { ApplicationEvent } from './events.js'

// Leave room for the DynamoDB item wrapper and transport attributes.
export const MAX_APPLICATION_EVENT_BYTES = 240 * 1024

function text(value: unknown, name: string, max = 256): asserts value is string {
  if (typeof value !== 'string' || !value.trim() || value.length > max || /[\u0000-\u001f\u007f]/.test(value)) {
    throw new TypeError(`${name} must be a nonempty string of at most ${max} characters without control characters`)
  }
}
function json(value: unknown, depth: number, seen: Set<object>): void {
  if (depth > 24) throw new TypeError('Event data exceeds maximum JSON nesting depth (24)')
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return
  if (typeof value === 'number' && Number.isFinite(value) && (!Number.isInteger(value) || Number.isSafeInteger(value))) return
  if (typeof value !== 'object' || seen.has(value)) throw new TypeError('Event data must be finite, acyclic JSON')
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    throw new TypeError('Event data must contain only plain JSON objects and arrays')
  }
  seen.add(value)
  if (Array.isArray(value)) {
    for (const item of value) json(item, depth + 1, seen)
  } else {
    for (const item of Object.values(value)) json(item, depth + 1, seen)
  }
  seen.delete(value)
}

export function serializeApplicationEvent(event: ApplicationEvent): string {
  if (!event || typeof event !== 'object') throw new TypeError('Expected an application event object')
  text(event.id, 'id')
  text(event.type, 'type', 100)
  if (!Number.isSafeInteger(event.version) || event.version < 1) throw new TypeError('version must be a positive safe integer')
  if (typeof event.timestamp !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/.test(event.timestamp) || !Number.isFinite(Date.parse(event.timestamp))) {
    throw new TypeError('timestamp must be an ISO 8601 timestamp')
  }
  if (event.ordering !== undefined) {
    if (!event.ordering || typeof event.ordering !== 'object') throw new TypeError('Invalid ordering')
    text(event.ordering.streamId, 'ordering.streamId')
    if (!Number.isSafeInteger(event.ordering.sequence) || event.ordering.sequence < 1) throw new TypeError('ordering.sequence must be a positive safe integer')
  }
  if (event.correlationId !== undefined) text(event.correlationId, 'correlationId')
  if (event.causationId !== undefined) text(event.causationId, 'causationId')
  if (event.metadata !== undefined) {
    if (!event.metadata || typeof event.metadata !== 'object' || Array.isArray(event.metadata)) throw new TypeError('metadata must be a string map')
    json(event.metadata, 0, new Set())
    for (const [key, value] of Object.entries(event.metadata)) {
      text(key, 'metadata key')
      if (typeof value !== 'string') throw new TypeError('metadata values must be strings')
    }
  }
  json(event.data, 0, new Set())
  // Undefined optional envelope fields are intentionally omitted; undefined data is rejected above.
  const encoded = JSON.stringify({
    id: event.id, type: event.type, version: event.version, timestamp: event.timestamp, data: event.data,
    ordering: event.ordering ? { streamId: event.ordering.streamId, sequence: event.ordering.sequence } : undefined, correlationId: event.correlationId, causationId: event.causationId, metadata: event.metadata,
  })
  if (Buffer.byteLength(encoded, 'utf8') > MAX_APPLICATION_EVENT_BYTES) throw new TypeError('Application event exceeds 240 KiB; store large payloads externally')
  return encoded
}
