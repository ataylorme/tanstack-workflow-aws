# tanstack-workflow-aws

An experimental `WorkflowExecutionStore` package for **TanStack Workflow** on AWS. It provides a DynamoDB execution store and a runnable, two-Region Lambda/EventBridge/CloudFront example. It does not use Vercel Workflow.

The package implements the current `@tanstack/workflow-runtime` 0.0.3 store interface. Both `us-west-2` and `us-east-2` may accept requests, execute workflows and sweep due work independently. They use their local replica of one **MRSC** DynamoDB Global Table; a witness in `us-east-1` forms the third quorum member. The same run can resume in either application Region.

> **Status:** prototype. Unit and runtime integration tests use a deterministic simulated document client. The CloudFormation templates pass `cfn-lint`; live cross-Region MRSC failure injection, load testing, security hardening and the full upstream store contract suite remain required before production deployment. No AWS resources are created by this repository.

## Install and use

```sh
npm install tanstack-workflow-aws @tanstack/workflow-runtime @tanstack/workflow-core
```

The package is prepared for publication; the example above will resolve after it is published. Until then, use `npm install` from a checkout or a Git reference. The caller needs a table with string `PK`/`SK` keys and `DueIndex` on string `duePK` and number `dueSK` (see `cloudformation/global-table.yaml`).

```ts
import { randomUUID } from 'node:crypto'
import { createDynamoWorkflowExecutionStore } from 'tanstack-workflow-aws'
import { defineWorkflowRuntime } from '@tanstack/workflow-runtime'

const store = createDynamoWorkflowExecutionStore({ tableName: process.env.TABLE_NAME! })
const runtime = defineWorkflowRuntime({ store, workflows: {
  example: { load: async () => (await import('./workflow.js')).workflow },
} })

// Bind the same owner to the store context and runtime invocation. Use a unique
// owner per Lambda invocation, including Region and request ID.
const owner = `${process.env.AWS_REGION}:${randomUUID()}`
const result = await store.withLeaseOwner(owner, () => runtime.startRun({
  workflowId: 'example', runId: 'stable-request-id', input: {}, leaseOwner: owner,
  deadline: Date.now() + 50_000,
}))
```

Pass `leaseOwner: owner` to `runtime.deliverSignal`, `runtime.deliverApproval` and `runtime.sweep` as well. Keep each call within `withLeaseOwner`. This binds state writes and event commits to the current lease; a resumed worker cannot silently accept a previous worker's write after reclaim. The store can be used directly for inspection without owner context; **do not execute workflows without the context in a multi-worker deployment**. If supplying a custom `DynamoDBDocumentClient`, configure `marshallOptions.removeUndefinedValues: true`.

## Design

| Resource | Item(s) | Coordination |
| --- | --- | --- |
| Run and core `RunState` | `RUN#id / META` | Conditional version replacement, lease owner and expiry |
| Event log | `RUN#id / SEG#uuid` | Stage immutable batch, then publish `head` and `nextIndex` on run item with one CAS |
| Timer | `TIMER#run#signal / META` | DueIndex candidate, strongly consistent base read and conditional claim |
| Schedule | `SCHEDULE#id / META` and `BUCKET#id#fireAt / META` | Conditional bucket create/claim; started buckets are not reclaimed |
| Due work | Sparse `DueIndex` | GSI is eventual; authoritative decisions always use base table conditional writes |

`appendEvents` stages all events of a batch in one item. A single conditional update to `RUN#id / META` publishes the batch. A reader starts at that pointer and follows `previous` links, so it sees the whole batch or none of it. A failed conditional commit can leave an unreachable segment; it is safe for replay, but requires offline garbage collection. Never delete a staged segment solely because the client timed out: the commit might have succeeded. The event chain is retained after a run is deleted, and `deleteRun` leaves a tombstone to prevent inadvertent ID reuse.

This relies on **MRSC** conditional writes and strongly consistent reads. Ordinary MREC Global Tables can accept the same local conditional claim in two Regions and are unsafe for this active/active design. MRSC has no DynamoDB transactions or TTL; explicit retention and orphan cleanup are application responsibilities. The run item also stores delivered signal IDs for atomic deduplication. Large run state, a high number of signal IDs or an event batch above DynamoDB's 400 KB item limit will fail; this prototype does not spill payloads to external storage. Event reads walk the entire chain and administrative `listRuns` uses a table Scan. `DueIndex` currently uses three unsharded partition values (`RUNNING`, `TIMER`, `SCHEDULE`), so it needs a shard design and load testing at high throughput.

A GSI is eventually consistent even on an MRSC table. A new timer or newly expired lease may be discovered on a later sweep. Duplicate EventBridge invocations and candidate reads are harmless to claims, but **external side effects still require idempotency keys** because a worker can die after a side effect and before its completion event commits. Keep clock skew below the lease margin; use a lease longer than the maximum expected store write and heartbeat interval. A lost MRSC quorum prevents writes; no store can continue safely in a single isolated Region under this consistency model.

## AWS example

Deploy the three templates in this order. This example deploys real, billable resources; inspect and adapt its public API, IAM, retention and monitoring settings before running it. CloudFormation controls the Global Table **from one stack in one Region**.

1. Build the example Lambda bundle and upload the ZIP to an S3 bucket in each application Region:

   ```sh
   npm ci
   npm run build
   mkdir -p build/lambda
   npx esbuild examples/handler.ts --bundle --platform=node --target=node22 --format=cjs --outfile=build/lambda/handler.js
   (cd build/lambda && zip ../workflow-handler.zip handler.js)
   aws s3 cp build/workflow-handler.zip s3://YOUR-WEST-BUCKET/workflow-handler.zip --region us-west-2
   aws s3 cp build/workflow-handler.zip s3://YOUR-EAST-BUCKET/workflow-handler.zip --region us-east-2
   ```

2. Deploy `cloudformation/global-table.yaml` **once**, from `us-west-2`. Wait until both replicas are ACTIVE. It creates replicas in `us-west-2` and `us-east-2` and a witness in `us-east-1`. Do not deploy the same table template again in Ohio.

   ```sh
   aws cloudformation deploy --region us-west-2 --stack-name workflow-table --template-file cloudformation/global-table.yaml
   ```

3. Deploy `cloudformation/regional.yaml` in **each** application Region with its local S3 bucket and `CodeKey=workflow-handler.zip`. Pass `--capabilities CAPABILITY_IAM`. Read the `ApiDomain` output of both stacks.

4. Deploy `cloudformation/edge.yaml` **only in `us-east-1`** with `WestApiDomain` and `EastApiDomain` from step 3 and `--capabilities CAPABILITY_IAM`. CloudFront uses a versioned Lambda@Edge function at the origin-request event. The edge function hashes `x-workflow-run-id` (or URL path) to distribute requests across the two APIs. The two regional EventBridge rules independently invoke their local sweep Lambda every minute.

The example accepts `POST /runs` with required `x-workflow-run-id` and JSON input, and `POST /runs/{id}/signals` with JSON `{ "signalId": "stable-id", "message": "done" }`. The workflow pauses for the `complete` signal and may resume in either Region. Invoke the CloudFront `Endpoint` output. Supply stable run and signal IDs on retries. The example does not configure an API authorizer; add authentication, access controls, alarms and throttling before exposing it to untrusted users.

CloudFront native origin-group failover covers GET/HEAD/OPTIONS only. The included Lambda@Edge router gives active/active placement, **not automatic failover for POST**. A regional outage requires an external health-based routing mechanism or a client retry against the other regional `ApiUrl` with the same idempotency key. Because every region uses the same MRSC store, such a retry can resume safely when quorum is available. Lambda@Edge only routes HTTP; it does not run the scheduler.

## Development

```sh
npm ci
npm run typecheck
npm run build
npm test
cfn-lint cloudformation/*.yaml
```

Tests cover event batch publication, conflicting regional writers, lease fencing and expiry, timers and signals, schedule buckets, and a real TanStack runtime start in one region followed by resume in the other. Run an integration suite against a real MRSC Global Table in all three Regions before treating the package as production ready. Fault inject a stage/commit crash, write timeouts, an expired lease during a step, a regional outage, and delayed GSI propagation. The simulated client cannot reproduce MRSC replication or quorum failures.
