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
import { createWorkflow } from '@ataylorme/tanstack-workflow-aws/workflow'
import { defineWorkflowRuntime } from '@ataylorme/tanstack-workflow-aws/runtime'
import { createDynamoWorkflowExecutionStore } from '@ataylorme/tanstack-workflow-aws'
```

Do not combine this store with separately installed `@tanstack/workflow-core` or
`@tanstack/workflow-runtime` engines. The supported and tested execution contract
is the included snapshot; similarly named npm releases may lack its recovery
behavior. This approach does not patch or replace dependencies in the consuming
application. Upgrade the snapshot and rerun recovery tests together when adopting
future upstream changes.

## GitHub Packages releases and CI

The npm package name is `@ataylorme/tanstack-workflow-aws`. Configure the scope in your consuming project's `.npmrc`:

```ini
@ataylorme:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${NODE_AUTH_TOKEN}
```

Set `NODE_AUTH_TOKEN` locally to a GitHub personal access token (classic) with `read:packages`; never commit the token. Then install with `npm install @ataylorme/tanstack-workflow-aws`. GitHub Actions consumers can use their `GITHUB_TOKEN` when granted access to the package. Package visibility and consumer access are managed in GitHub Packages settings.

To release, update `version` in both package manifests, merge the change, and publish a GitHub release tagged `v<VERSION>` (for example `v0.1.0`) at that commit. The release workflow verifies the tag matches the manifest and runs the reusable test workflow before publishing using its built-in `GITHUB_TOKEN` with `packages: write`. No separate publishing secret is required. Prereleases use the `next` npm dist-tag; regular releases use `latest`. Each version can be published only once. This change configures publishing but does not create a release.

PRs and pushes to `main` run typecheck, build, all tests against DynamoDB Local, an installed-package smoke test and Lambda bundle checks on Node 20, 22 and 24. The Node 22 job also lints CloudFormation. Tests use read-only repository permissions and no AWS credentials. The same workflow runs on the release commit before publication. Repository administrators can require the three `Test Node` checks through branch protection.
