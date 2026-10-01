import { createWorkflowWorker } from '../src/wakeups.js'
import { createDynamoApplicationEventPublisher } from '../src/events.js'
import { runtime, store } from './runtime.js'
import { required, transport } from './transport.js'
export const handler = createWorkflowWorker({ runtime, store, transport,
  publisher: createDynamoApplicationEventPublisher({ tableName: required('TABLE_NAME') }),
  region: required('AWS_REGION'),
})
