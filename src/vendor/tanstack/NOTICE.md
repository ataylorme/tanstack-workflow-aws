# Vendored TanStack Workflow snapshot

Source: https://github.com/TanStack/workflow
Commit: b9287174b44424059895a0ed834b18ce50e0484a
Directories: packages/workflow-core/src and packages/workflow-runtime/src
License: MIT, Copyright (c) 2025 Tanner Linsley (see LICENSE in this directory).

These files are an upstream source snapshot. Module
specifier rewrites: relative imports/exports include `.js` (directory imports
include `/index.js`), and `@tanstack/workflow-core` resolves to the vendored core.
Host safety patches persist terminal input-validation/version-mismatch states,
isolate per-run sweep failures, and add an optional `deferRunRecovery` store hook
for bounded retry backoff. These changes are covered by runtime recovery tests.
Preserve this notice and the license
when updating the snapshot. This snapshot is used because the published packages
at review time did not provide the interrupted-run recovery required by this store.
