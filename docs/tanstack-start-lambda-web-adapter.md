# TanStack Start, Lambda Web Adapter and Aurora DSQL

This guide targets an existing Start application deployed active/active in `us-east-1` and `us-west-2`, with CloudFront/Lambda@Edge routing and Aurora DSQL as its primary application database. The package remains a DynamoDB workflow store. Adding it does not require replacing the application's DSQL data access, web server, or edge routing.

## Topology

| Component | us-east-1 | us-west-2 | us-east-2 |
| --- | --- | --- | --- |
| Start + Lambda Web Adapter | Active web Lambda | Active web Lambda | — |
| Workflow runtime | In server code and sweep Lambda | In server code and sweep Lambda | — |
| DynamoDB MRSC | Read/write replica | Read/write replica | Witness |
| Aurora DSQL | Local endpoint of your peered database | Local endpoint of your peered database | Witness if configured for this topology |
| EventBridge | Local sweep trigger | Local sweep trigger | — |

Each service maintains its own replication and quorum. Selecting Ohio for both witnesses does not make DSQL and DynamoDB one database or one transaction domain. The example templates provision the workflow infrastructure only; your application owns its DSQL clusters and permissions. Use each Region's local endpoints.

A recommended deployment uses two Lambda functions per application Region: the Start web function and a native workflow sweep function. Both bundle the same workflow registry and compatible workflow versions and use the same table name. This gives background work its own concurrency allocation and execution budget. Neither Region permanently owns a run. An expired execution lease can be reclaimed by the other Region when the DynamoDB quorum is available.

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

Await the runtime call before returning the HTTP response. Do not detach it with `void runtime.startRun(...)`, start a process-local interval, or rely on post-response background work. EventBridge invokes the durable sweeper for subsequent bounded execution. The package is currently a prototype: the additional dispatcher recovery requirements below must be validated before claiming reliable background submission.

## Deploy the dedicated sweeper

1. Deploy [`global-table.yaml`](../cloudformation/global-table.yaml) once from `us-west-2`. It creates the east/west replicas and an Ohio witness. This is a fresh-table example; changing an existing MRSC witness/replica topology is not an in-place migration procedure. Wait until the table and replicas are ACTIVE.
2. Bundle the shared workflows into the sweeper. From this repository:

   ```sh
   npm ci
   mkdir -p build/sweeper
   npx esbuild examples/sweeper.ts --bundle --platform=node --target=node22 --format=cjs --outfile=build/sweeper/sweeper.js
   (cd build/sweeper && zip ../workflow-sweeper.zip sweeper.js)
   ```

3. Upload that ZIP into an S3 bucket in each application Region. Deploy [`sweeper.yaml`](../cloudformation/sweeper.yaml) in each Region with `TableName`, its local `CodeBucket`, and `CodeKey`, plus `--capabilities CAPABILITY_IAM`.
4. Give both Start web functions the same DynamoDB data-plane permissions shown in the sweeper role and set their `TABLE_NAME` to the shared table name. The default SDK client selects the current Lambda Region. Add any DSQL IAM permissions and connection settings needed by your own workflows to both function roles. The sample sweeper does not connect to DSQL.
5. Reuse your existing CloudFront/Lambda@Edge distribution. The standalone `regional.yaml` and `edge.yaml` templates demonstrate a separate API; they are not required when embedding workflows in your app.

The native sweeper needs neither Lambda Web Adapter nor a public HTTP endpoint. Deploy compatible workflow code to both Regions and both function roles before submitting new workflow versions; retain loaders for in-flight older versions. Both EventBridge rules stay enabled during ordinary operation.

### If one web Lambda per Region is preferred

Lambda Web Adapter also supports non-HTTP triggers through `AWS_LWA_PASS_THROUGH_PATH`, defaulting to `/events`. A private, authenticated control route can receive the EventBridge payload and await `runtime.sweep()`. Setting a path alone is not an authorization mechanism; prevent public callers from invoking the sweep through every ingress path, including the regional endpoint. Configure `AWS_LWA_ERROR_STATUS_CODES` for the error statuses that should fail an asynchronous Lambda invocation and trigger retries. The dedicated sweeper avoids this HTTP control route and is the recommended example here.

## DSQL writes and workflow handoff

A business transaction committed to DSQL and a workflow write to DynamoDB are separate operations. Without durable handoff, a crash after the DSQL commit but before `startRun()` can leave the business operation without a workflow.

For workflows triggered by business mutations, implement a transactional outbox in your application's DSQL schema:

1. Commit the business change and an outbox row with a stable operation/run ID in the **same DSQL transaction**.
2. Regional dispatchers read pending rows and submit the same operation ID to the workflow runtime. If using a DSQL claim, use conditional updates and transaction retries; do not assume PostgreSQL row locks are available. Keep external service calls outside the DSQL transaction.
3. Acknowledge the outbox row only once the application has a durable recovery path for the submitted operation. A worker dying after submission but before acknowledgement must be harmless to retry.

**Current package limitation:** `createRun()` followed by a crash before `claimRun()` can leave a `queued` run that `claimStaleRuns()` does not find. Likewise, signal acceptance and runtime execution are separate phases. Do not treat “the run row exists,” `duplicate`, or a `running` response as proof that the operation can no longer stall. A production dispatcher needs reconciliation for unfinished outbox rows and these handoff states. This guide describes the boundary; it does not implement a DSQL outbox or claim complete handoff recovery.

Within a workflow step that mutates DSQL, use a stable per-step operation key. Record its completion and the business mutation in the same DSQL transaction, so replay can return the recorded result instead of applying the mutation twice. Retry the whole transaction on DSQL serialization conflicts (`40001`) with bounded backoff. An event commit in DynamoDB cannot atomically commit the DSQL mutation. External effects such as email or payments need their own idempotent delivery mechanism.

## Ingress and recovery

Lambda@Edge placement and regional health routing are independent of workflow execution. Existing HTTP traffic routing does not assign durable run ownership. A signal can arrive in the opposite Region from the original request; the shared MRSC store arbitrates the claim. Continue using stable operation IDs for client retries, and verify how your edge layer reroutes mutation requests during an outage. The standalone edge sample is deterministic placement, not automatic POST failover.

This integration does not remove the prototype limits documented in the main README. Test with real MRSC replicas, the application's actual Start build and Lambda Web Adapter version, DSQL transaction retries, and both regional stacks before relying on cross-Region recovery.

## References

- [TanStack Start server functions](https://tanstack.com/start/latest/docs/framework/react/guide/server-functions)
- [AWS Lambda Web Adapter configuration](https://github.com/aws/aws-lambda-web-adapter)
- [DynamoDB Global Tables region sets and consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-global-table-design.html)
- [Aurora DSQL multi-Region configuration](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/configuring-multi-region-clusters.html)
- [Aurora DSQL concurrency control](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html)
- [Transactional outbox pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html)
