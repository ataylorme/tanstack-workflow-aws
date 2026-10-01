# TanStack Start, Lambda Web Adapter and Aurora DSQL

This guide targets an existing Start application deployed active/active in `us-east-1` and `us-west-2`, with CloudFront/Lambda@Edge routing and Aurora DSQL as its primary application database. The package remains a DynamoDB workflow store. Adding it does not require replacing the application's DSQL data access, web server, or edge routing.

## Topology

| Component | us-east-1 | us-west-2 | us-east-2 |
| --- | --- | --- | --- |
| Start + Lambda Web Adapter | Active web Lambda | Active web Lambda | — |
| Workflow runtime | In server code and targeted worker Lambda | In server code and targeted worker Lambda | — |
| DynamoDB MRSC | Read/write replica | Read/write replica | Witness |
| Aurora DSQL | Local endpoint of your peered database | Local endpoint of your peered database | Witness if configured for this topology |
| Workflow wakeups | Stream → router → one-time Scheduler/SQS → targeted worker | Same independent regional path | — |

Each service maintains its own replication and quorum. Selecting Ohio for both witnesses does not make DSQL and DynamoDB one database or one transaction domain. The example templates provision the workflow infrastructure only; your application owns its DSQL clusters and permissions. Use each Region's local endpoints.

A recommended deployment uses the Start web function, a unified stream router, a native targeted workflow function and an application queue consumer per application Region. Web and worker functions bundle the same workflow registry and matching workflow versions and use the same table name. This gives background work its own concurrency allocation and execution budget. Neither Region permanently owns a run. An expired execution lease can be reclaimed by the other Region when the DynamoDB quorum is available.

## Call workflows from Start server code

Keep the store, workflow registry and DSQL client in server-only code. Import them from a Start server function or server route. Use your existing authentication and authorization before accepting workflow IDs, business identifiers or signals. Run IDs should include the authenticated tenant and a stable business operation ID, so a retry in the other Region resolves to the same operation.

The following is an application integration sketch. `requireUser` is your app's authentication function; `workflow-runtime` is your shared server module, based on [`examples/runtime.ts`](../examples/runtime.ts), exporting `store` and `runtime`. The example registry contains the `task` workflow, which pauses for a signal. Adapt the import paths and registry to your app.

```ts
import { createServerFn } from '@tanstack/react-start'

export const startTask = createServerFn({ method: 'POST' })
  .inputValidator((value: unknown) => {
    const id = (value as { operationId?: unknown } | null)?.operationId
    if (typeof id !== 'string' || !/^[a-zA-Z0-9_-]{1,100}$/.test(id)) {
      throw new Error('Invalid operation ID')
    }
    return { operationId: id }
  })
  .handler(async ({ data }) => {
    const { randomUUID } = await import('node:crypto')
    const { requireUser } = await import('./auth.server')
    const { store, runtime } = await import('./workflow-runtime.server')
    const user = await requireUser()
    // Generate this for every request. Never reuse a module-level owner.
    const owner = `${process.env.AWS_REGION}:${randomUUID()}`
    const runId = JSON.stringify([user.tenantId, 'task', data.operationId])
    const result = await store.withLeaseOwner(owner, () => runtime.startRun({
      workflowId: 'task',
      runId,
      input: { tenantId: user.tenantId, operationId: data.operationId },
      leaseOwner: owner,
      maxDurationMs: 5_000,
      includeEvents: false,
    }))
    return { runId, kind: result.kind }
  })
```

Use the same `withLeaseOwner` binding and runtime `leaseOwner` for signal/approval delivery. A Web Adapter handler is an HTTP request handler; it need not receive the native Lambda `context` argument. A per-request UUID avoids depending on a forwarded request ID. Set the workflow budget below the remaining Lambda and HTTP timeout budgets; the five-second value above is illustrative. Durable execution yields at runtime boundaries, so individual steps also need their own network timeouts and a bounded duration. A runtime budget cannot forcibly stop an arbitrary long-running step.

Await the runtime call before returning the HTTP response. Do not detach it with `void runtime.startRun(...)`, start a process-local interval, or rely on post-response background work. Committed workflow metadata drives stream wakeups, one-time Scheduler deadlines, and an SQS-backed targeted worker for subsequent bounded execution. Each keyed wakeup calls `runtime.processTarget` and directly claims its run, timer, or schedule without a `DueIndex` query. The worker persists a keyed continuation before acknowledgement when needed. No recurring rule is required; the store alone does not provision this path. The package is experimental; validate the durable handoff and recovery behavior below against your deployed infrastructure.

## Deploy the dedicated worker

1. Deploy [`global-table.yaml`](../cloudformation/global-table.yaml) once from `us-west-2`. It creates the east/west replicas and an Ohio witness. Wait until the table and replicas are ACTIVE. See [MRSC provisioning](mrsc-deployment.md) for the explicit CloudFormation service-role option and safe handling of a retained table after partial creation.
2. Follow [the wakeup build and deployment commands](workflow-wakeups.md#build-and-deploy) to bundle `worker.js`, `router.js` and `application-consumer.js` (the standalone `handler.js` is unused here). Install the Scheduler, SQS and SNS SDKs in your application's build dependencies when copying the examples. Keep the worker's `./runtime.js` imports wired to the same workflow registry as the web application.
3. Deploy [`workers.yaml`](../cloudformation/workers.yaml) in each Region with the local table stream ARN, local artifact bucket and immutable ZIP key. The stack creates the unified stream router, targeted workflow worker and independent application delivery worker.
4. Give both Start web functions the DynamoDB workflow data-plane permissions shown in the worker role and set `TABLE_NAME`. Do not copy the router's Scheduler, SQS, or `iam:PassRole` permissions into the web role. Add your workflows' DSQL settings and permissions to both web and worker functions; the example itself does not connect to DSQL.
5. Reuse existing CloudFront/Lambda@Edge ingress. The standalone `regional.yaml` and `edge.yaml` are unnecessary for an embedded integration.

Neither router nor worker needs Lambda Web Adapter or a public HTTP endpoint. Retain workflow loaders in both Regions for every in-flight version. A single HTTP `/events` pass-through handler is not a drop-in replacement: it would also need the SQS partial-batch failure and durable-continuation protocol. Prefer the dedicated private functions rather than exposing administrative recovery endpoints.

## DSQL writes and workflow handoff

A business transaction committed to DSQL and a workflow write to DynamoDB are separate operations. Without durable handoff, a crash after the DSQL commit but before `startRun()` can leave the business operation without a workflow.

For workflows triggered by business mutations, implement a transactional outbox in your application's DSQL schema:

1. Commit the business change and an outbox row with a stable operation/run ID in the **same DSQL transaction**.
2. Regional routers read pending rows and submit the same operation ID to the workflow runtime. If using a DSQL claim, use conditional updates and transaction retries; do not assume PostgreSQL row locks are available. Keep external service calls outside the DSQL transaction.
3. Acknowledge the outbox row only once the application has a durable recovery path for the submitted operation. A worker dying after submission but before acknowledgement must be harmless to retry.

The store indexes queued submissions and expired or released running runs for recovery. Signal and approval acceptance also retain their payload until the resolution event commits. DynamoDB Local regression tests cover crashes at these boundaries. A durable `createRun` record is therefore recoverable by targeted workers, provided the matching workflow remains registered in both Regions. Keep outbox reconciliation for failures before submission, unavailable services, incompatible workflow deployments, and operator intervention; validate the complete DSQL-to-DynamoDB handoff on AWS. This package does not implement the DSQL outbox itself.

Within a workflow step that mutates DSQL, use a stable per-step operation key. Record its completion and the business mutation in the same DSQL transaction, so replay can return the recorded result instead of applying the mutation twice. Retry the whole transaction on DSQL serialization conflicts (`40001`) with bounded backoff. An event commit in DynamoDB cannot atomically commit the DSQL mutation. External effects such as email or payments need their own idempotent delivery mechanism.

## Ingress and recovery

Lambda@Edge placement and regional health routing are independent of workflow execution. Existing HTTP traffic routing does not assign durable run ownership. A signal can arrive in the opposite Region from the original request; the shared MRSC store arbitrates the claim. Continue using stable operation IDs for client retries, and verify how your edge layer reroutes mutation requests during an outage. The standalone edge sample is deterministic placement, not automatic POST failover.

Configure the history and retention limits documented in [lifecycle](lifecycle.md). Test with real MRSC replicas, the application's actual Start build and Lambda Web Adapter version, DSQL transaction retries, and both regional stacks before relying on cross-Region recovery.

## References

- [TanStack Start server functions](https://tanstack.com/start/latest/docs/framework/react/guide/server-functions)
- [AWS Lambda Web Adapter configuration](https://github.com/aws/aws-lambda-web-adapter)
- [DynamoDB Global Tables region sets and consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-global-table-design.html)
- [Aurora DSQL multi-Region configuration](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/configuring-multi-region-clusters.html)
- [Aurora DSQL concurrency control](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html)
- [Transactional outbox pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)
