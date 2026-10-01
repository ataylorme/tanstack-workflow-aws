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

## Regional deployment artifacts

Bundle `handler.js`, `worker.js`, `router.js` and `application-consumer.js` and `ordered-subscriber.js` using the commands in [demand-driven wakeups](workflow-wakeups.md). Deploy `workers.yaml` per replica. `regional.yaml` supplies the optional standalone HTTP API. The AWS transport subpath requires Scheduler and SQS peers; the example application consumer also requires SNS.

Use immutable S3 artifact keys or explicit object versions. CloudFormation does not detect overwritten bytes at the same bucket/key. Web and worker bundles must contain the same workflow registry and matching engine. The edge template's `RouterRevision` identifies its inline routing code; published versions are retained until CloudFront replication permits their removal.

The installed-package check verifies all public subpaths, injectable handler types, targeted processing and independent optional SDK dependencies. Template and bundle checks establish artifact integrity; live MRSC and regional failure behavior require AWS acceptance tests.

## Pinned workflow engine

The package includes TanStack Workflow core/runtime at commit `b9287174b44424059895a0ed834b18ce50e0484a`. The MIT license and provenance notice are distributed under `src/vendor/tanstack`. Host extensions provide fenced recovery, direct targeted claims and self-advancing schedule integration. Use the matching engine exports:

```ts
import { createWorkflow } from '@ataylorme/tanstack-workflow-aws/workflow'
import { defineWorkflowRuntime } from '@ataylorme/tanstack-workflow-aws/runtime'
import { createDynamoWorkflowExecutionStore } from '@ataylorme/tanstack-workflow-aws'
```

The supported execution contract is the included engine and its recovery tests. Keep the source notice and rerun the full suite when changing the engine snapshot. `cron-parser` is pinned to make cron evaluation reproducible across deployed bundles.

## GitHub Packages releases and CI

The npm package name is `@ataylorme/tanstack-workflow-aws`. Configure the scope in your consuming project's `.npmrc`:

```ini
@ataylorme:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${NODE_AUTH_TOKEN}
```

Set `NODE_AUTH_TOKEN` locally to a GitHub personal access token (classic) with `read:packages`; never commit the token. Then install with `npm install @ataylorme/tanstack-workflow-aws`. GitHub Actions consumers can use their `GITHUB_TOKEN` when granted access to the package. Package visibility and consumer access are managed in GitHub Packages settings.

To release, update `version` in both package manifests, merge the change, and publish a GitHub release tagged `v<VERSION>` (for example `v0.2.0-rc.0`) at that commit. The release workflow verifies the tag matches the manifest and runs the reusable test workflow before publishing using its built-in `GITHUB_TOKEN` with `packages: write`. No separate publishing secret is required. Prereleases use the `next` npm dist-tag; regular releases use `latest`. Each version can be published only once. Updating a branch does not publish a package release.

PRs and pushes to `main` run typecheck, build, all tests against DynamoDB Local, an installed-package smoke test and Lambda bundle checks on Node 20, 22 and 24. The Node 22 job also lints CloudFormation. Tests use read-only repository permissions and no AWS credentials. The same workflow runs on the release commit before publication. Repository administrators can require the three `Test Node` checks through branch protection.
