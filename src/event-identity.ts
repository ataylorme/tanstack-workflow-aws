import { createHash } from 'node:crypto'
import type { ApplicationEvent } from './events.js'
const hash = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex')
export const applicationEventGroupId = (event: ApplicationEvent) => hash(event.ordering ? ['stream', event.ordering.streamId] : ['event', event.id])
export const applicationEventDeduplicationId = (event: ApplicationEvent) => event.ordering ? hash(['stream', event.ordering.streamId, event.ordering.sequence]) : createHash('sha256').update(event.id).digest('hex')
