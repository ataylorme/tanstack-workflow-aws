# Package installation and deployment artifacts

Until an npm release exists, install from a checkout or an explicit Git revision:

```sh
npm install github:ataylorme/tanstack-workflow-aws#YOUR_COMMIT_OR_BRANCH
```

The Git installation runs the package's `prepare` lifecycle script to build its ESM JavaScript and TypeScript declarations. Node.js 20 or later is required. Do not pass `--ignore-scripts` for a source/Git install unless you build the package yourself first. A published tarball already contains the compiled `dist` files.

From the repository, verify the actual distributable with:

```sh
npm ci
npm run test:package
```

This packs the package, installs the tarball into an unrelated temporary project, imports the public entry point, constructs the store without contacting AWS, and typechecks a consumer against the upstream store interface. It needs npm registry access (or a populated npm cache) for the consumer's production dependencies. The docs, deployment templates, example sources and store source are included alongside the compiled library.

## Updating the AWS examples

Upload each new regional Lambda bundle under a new, immutable S3 `CodeKey`, or enable S3 versioning and pass the new `CodeObjectVersion`. CloudFormation does not detect overwritten bytes at the same S3 bucket/key. Both the standalone API and its separate sweep Lambda use the supplied bundle; the sweeper-only template supplies a function for an existing application.

The edge template's `RouterRevision` must change whenever its inline routing code changes. Its published version description also includes both API domains, so a changed endpoint creates a new version and updates the CloudFront association. Old edge versions are retained because CloudFront replicas cannot be deleted immediately after association changes; remove unused versions manually after replication has completed and AWS allows deletion.

These checks establish that the artifact can be consumed and the templates are syntactically valid. They do not establish live MRSC replication or regional failover behavior.

## Pinned workflow engine

This package includes the TanStack Workflow core/runtime source snapshot at commit
`b9287174b44424059895a0ed834b18ce50e0484a`. Its MIT license and provenance notice
are distributed under `src/vendor/tanstack`. ESM imports are rewritten for packaging. Small recovery patches persist terminal
validation/version errors and isolate failed sweep candidates with retry backoff;
the provenance notice enumerates these changes. The snapshot includes interrupted-run
recovery absent from the published core/runtime packages at review time.

Use the supported matching engine exports:

```ts
import { createWorkflow } from 'tanstack-workflow-aws/workflow'
import { defineWorkflowRuntime } from 'tanstack-workflow-aws/runtime'
import { createDynamoWorkflowExecutionStore } from 'tanstack-workflow-aws'
```

Do not combine this store with separately installed `@tanstack/workflow-core` or
`@tanstack/workflow-runtime` engines. The supported and tested execution contract
is the included snapshot; similarly named npm releases may lack its recovery
behavior. This approach does not patch or replace dependencies in the consuming
application. Upgrade the snapshot and rerun recovery tests together when adopting
future upstream changes.
