# tanstack-workflow-aws

An experimental `WorkflowExecutionStore` package for **TanStack Workflow** on AWS. It provides a DynamoDB execution store and a runnable, two-Region Lambda/EventBridge/CloudFront example. It does not use Vercel Workflow.

The package implements the included TanStack Workflow snapshot’s store interface. Import the matching engine from this package’s `/workflow` and `/runtime` entry points; see [snapshot provenance and packaging](docs/packaging.md). Both `us-west-2` and `us-east-1` may accept requests, execute workflows and sweep due work independently. They use their local replica of one **MRSC** DynamoDB Global Table; a witness in `us-east-2` forms the third quorum member. The same run can resume in either application Region.

> **Status:** experimental, prepared for controlled AWS integration testing. Regression tests exercise real DynamoDB expressions against DynamoDB Local, upstream store contracts, interrupted runtime recovery, and the installed package. Live MRSC replication, regional failure injection and load testing remain required before production use. See [review evidence and the AWS test plan](docs/testing.md). No AWS deployment has been performed during this review.

## Install and use

```sh
npm install tanstack-workflow-aws
```

The package is prepared for publication; the example above will resolve after it is published. Until then, use `npm install` from a checkout or a Git reference. The caller needs a table with string `PK`/`SK` keys and `DueIndex` on string `duePK` and number `dueSK` (see `cloudformation/global-table.yaml`).

```ts
import { randomUUID } from 'node:crypto'
import { createDynamoWorkflowExecutionStore } from 'tanstack-workflow-aws'
import { defineWorkflowRuntime } from 'tanstack-workflow-aws/runtime'

const store = createDynamoWorkflowExecutionStore({ tableName: process.env.TABLE_NAME! })
const runtime = defineWorkflowRuntime({ store, workflows: {
  example: { load: async () => (await import('./workflow.js')).workflow },
} })

// Bind the same owner to the store context and runtime invocation. Use a unique
// owner per Lambda invocation, including Region and request ID.
const owner = `${process.env.AWS_REGION}:${randomUUID()}`
const result = await store.withLeaseOwner(owner, () => runtime.startRun({
  workflowId: 'example', runId: 'stable-request-id', input: {}, leaseOwner: owner,
  maxDurationMs: 5_000,
}))
```

Pass `leaseOwner: owner` to `runtime.deliverSignal`, `runtime.deliverApproval` and `runtime.sweep` as well. Keep each call within `withLeaseOwner`. This binds state writes and event commits to the current lease; a resumed worker cannot silently accept a previous worker's write after reclaim. The store can be used directly for inspection without owner context; **do not execute workflows without the context in a multi-worker deployment**. If supplying a custom `DynamoDBDocumentClient`, configure `marshallOptions.removeUndefinedValues: true`.

## Design

| Resource | Item(s) | Coordination |
| --- | --- | --- |
| Run and core `RunState` | `RUN#id / META` | Conditional version replacement, lease owner and expiry |
| Event log | `RUN#id / SEG#uuid` | Stage immutable batch, then publish `head` and `nextIndex` on run item with one CAS |
| Timer | Paused run metadata; standalone timer rows for direct store clients | Persisted waits remain discoverable even if timer registration is interrupted |
| Schedule | `SCHEDULE#id / META`, including its pending bucket | One-item conditional claim preserves unfinished buckets when the next tick advances |
| Due work | Sparse `DueIndex` | GSI is eventual; authoritative decisions always use base table conditional writes |

`appendEvents` stages all events of a batch in one item. A single conditional update to `RUN#id / META` publishes the batch. A reader starts at that pointer and follows `previous` links, so it sees the whole batch or none of it. A failed conditional commit can leave an unreachable segment; it is safe for replay, but requires offline garbage collection. Never delete a staged segment solely because the client timed out: the commit might have succeeded. The event chain is retained after a run is deleted, and `deleteRun` leaves a tombstone to prevent inadvertent ID reuse.

This relies on **MRSC** conditional writes and strongly consistent reads. Ordinary MREC Global Tables can accept the same local conditional claim in two Regions and are unsafe for this active/active design. MRSC has no DynamoDB transactions or TTL; explicit retention and orphan cleanup are application responsibilities. The run item also stores delivered signal IDs for atomic deduplication. Large run state, a high number of signal IDs or an event batch above DynamoDB's 400 KB item limit will fail; this prototype does not spill payloads to external storage. Event reads walk the entire chain and administrative `listRuns` uses a table Scan. `DueIndex` currently uses four unsharded partition values (`RUNNING`, `TIMER_RUN`, `TIMER`, `SCHEDULE`), so it needs a shard design and load testing at high throughput.

A GSI is eventually consistent even on an MRSC table. A new timer or newly expired lease may be discovered on a later sweep. Duplicate EventBridge invocations and candidate reads are harmless to claims, but **external side effects still require idempotency keys** because a worker can die after a side effect and before its completion event commits. Keep clock skew below the lease margin; use a lease longer than the maximum expected store write and heartbeat interval. A lost MRSC quorum prevents writes; no store can continue safely in a single isolated Region under this consistency model.

Queued runs and expired or released running leases are sweep candidates. Accepted signals and approvals persist their payload with the deduplication ID; recovery publishes the resolution event and clears that payload atomically. Schedule policies `allow` and `skip` are supported; `buffer-one`, `cancel-previous` and `terminate-previous` are rejected. Schedule metadata changed during this pre-release review: use a fresh test table rather than mixing old prototype workers or records with this implementation.

## Framework-independent package

The library implements TanStack Workflow's store contract for a Node.js runtime. It has no dependency on TanStack Start, Lambda Web Adapter, an application's database, or an HTTP routing framework. Applications supply their workflow definitions and may use any business datastore. The Lambda handlers and CloudFormation templates are optional deployment examples; separate regional sweep Lambdas can share any application's workflow registry.

## TanStack Start with Lambda Web Adapter and Aurora DSQL

For an existing Start application in `us-east-1` and `us-west-2`, use this package in server functions/routes alongside your DSQL client. DSQL remains the application database; DynamoDB MRSC holds workflow coordination and history. The recommended deployment adds a dedicated EventBridge sweep Lambda in each application Region, sharing the same workflow definitions as the web application. Reuse the application's existing CloudFront/Lambda@Edge ingress.

See [the integration guide](docs/tanstack-start-lambda-web-adapter.md) for request ownership, bounded execution, the DSQL outbox boundary, and the dedicated [sweeper template](cloudformation/sweeper.yaml). These integration files do not provision or modify DSQL clusters or your Start application.

## Standalone AWS example

Deploy the three templates in this order. This example deploys real, billable resources; inspect and adapt its public API, IAM, retention and monitoring settings before running it. CloudFormation controls the Global Table **from one stack in one Region**.

1. Build the example Lambda bundle and upload the ZIP to an S3 bucket in each application Region:

   ```sh
   npm ci
   npm run build
   mkdir -p build/lambda
   npx esbuild examples/handler.ts --bundle --platform=node --target=node22 --format=cjs --outfile=build/lambda/handler.js
   (cd build/lambda && zip ../workflow-handler.zip handler.js)
   aws s3 cp build/workflow-handler.zip s3://YOUR-WEST-BUCKET/workflow-handler.zip --region us-west-2
   aws s3 cp build/workflow-handler.zip s3://YOUR-EAST-BUCKET/workflow-handler.zip --region us-east-1
   ```

2. Deploy `cloudformation/global-table.yaml` **once**, from `us-west-2`. Wait until both replicas are ACTIVE. It creates replicas in `us-west-2` and `us-east-1` and a witness in `us-east-2`. Do not deploy the same table template again in N. Virginia.

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
npm run test:package
DYNAMODB_LOCAL_JAR=/path/to/DynamoDBLocal.jar npm run test:integration
cfn-lint cloudformation/*.yaml
```

`npm test` runs unit checks and skips emulator suites unless `DYNAMODB_ENDPOINT` is configured. `test:integration` starts DynamoDB Local when given its JAR and runs all suites. Download DynamoDB Local from [AWS](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html); Java 17 or later is required. Tests create and delete isolated tables and accept only localhost endpoints. They are not a live AWS test runner. See [the testing guide](docs/testing.md) for reproducible checks and the separate MRSC test plan.
