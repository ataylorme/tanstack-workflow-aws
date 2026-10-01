# Vendored TanStack Workflow snapshot

Source: https://github.com/TanStack/workflow
Commit: b9287174b44424059895a0ed834b18ce50e0484a
Directories: packages/workflow-core/src and packages/workflow-runtime/src
License: MIT, Copyright (c) 2025 Tanner Linsley (see LICENSE in this directory).

These files are an upstream source snapshot with host extensions. Relative
imports/exports include `.js` (directory imports include `/index.js`), and
`@tanstack/workflow-core` resolves to the vendored core. Host safety patches
persist terminal validation/version errors, isolate per-run execution failures,
and use `deferRunRecovery` for bounded retry backoff. The `processTarget` API and
optional `claimStaleRun`, `claimTimer` and `claimScheduleBucket` store methods
reuse the execution/recovery driver for one named item. Administrative `sweep`
uses the same driver with discovery. Schedule definitions include missed-tick
policy and catch-up bounds; the materializer seeds future deadlines through the
host cron evaluator, while the execution store advances accepted buckets.
Terminal checkpoints commit before terminal state; recovery finalizes committed
terminal records without rerunning the handler. Unhandled persistence errors
propagate to the runtime. Recovery, scheduling and installed-package tests cover
these extensions.
Preserve this notice and the license when updating the snapshot.
