import { store, runtime } from './runtime.js'

type LambdaContext = {
  awsRequestId: string
  getRemainingTimeInMillis(): number
}

/** Native Lambda handler. It imports the same workflow definitions as the web app. */
export async function handler(_event: unknown, context: LambdaContext) {
  const owner = `${process.env.AWS_REGION}:${context.awsRequestId}`
  const remaining = context.getRemainingTimeInMillis()
  if (remaining <= 2_000) throw new Error('Insufficient time to start a workflow sweep')

  return store.withLeaseOwner(owner, () => runtime.sweep({
    leaseOwner: owner,
    deadline: Date.now() + remaining - 2_000,
    limit: 20,
    includeEvents: false,
  }))
}
