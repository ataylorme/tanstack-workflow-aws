import { AsyncLocalStorage } from 'node:async_hooks'
import { randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { buildPendingResolution } from './recovery.js'
import type { PendingDelivery } from './recovery.js'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, DeleteCommand, GetCommand, PutCommand, QueryCommand, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb'
import { LogConflictError } from './workflow.js'
import type { DeleteReason, RunState, WorkflowEvent } from './workflow.js'
import type {
  WorkflowExecutionStore, WorkflowExecution, StoredWorkflowEvent, WorkflowLease,
  AppendEventsArgs, ReadEventsArgs, ClaimRunArgs, ClaimRunResult, DeliverSignalArgs,
  DeliverSignalResult, DeliverApprovalArgs, DeliverApprovalResult, ScheduleBucket,
  RunSummary, UpsertScheduleArgs, ScheduleTimerArgs,
} from './runtime.js'

export interface DynamoWorkflowStoreOptions {
  /** An MRSC Global Table with PK/SK, and DueIndex (duePK/dueSK). */
  tableName: string
  client?: DynamoDBDocumentClient
}

/** Bind every runtime call to the lease owner passed to startRun/sweep. Required for fencing state/event writes. */
export type FencedWorkflowExecutionStore = WorkflowExecutionStore & {
  withLeaseOwner<T>(owner: string, run: () => Promise<T>): Promise<T>
}

type Item = Record<string, any>
const ownerContext = new AsyncLocalStorage<string>()
const copy = <T>(value: T): T => structuredClone(value)
const isReplicationConflict = (error: unknown) => (error as { name?: string })?.name === 'ReplicatedWriteConflictException'
const backoff = (attempt: number) => new Promise<void>(resolve => setTimeout(resolve, Math.min(100, 5 * 2 ** attempt) * (0.5 + Math.random())))
const isConflict = (error: unknown) => ['ConditionalCheckFailedException', 'ReplicatedWriteConflictException'].includes((error as { name?: string })?.name ?? '')
const lease = (owner: string, ms: number, now: number): WorkflowLease => ({ owner, expiresAt: now + ms })
const terminal = (status: string) => ['finished', 'errored', 'aborted'].includes(status)
const runKey = (id: string) => `RUN#${id}`
const timerKey = (runId: string, signalId: string) => `TIMER#${encodeURIComponent(runId)}#${encodeURIComponent(signalId)}`
const scheduleKey = (id: string) => `SCHEDULE#${id}`
const expressionNames = { '#v': 'version' }

export function createDynamoWorkflowExecutionStore(options: DynamoWorkflowStoreOptions): FencedWorkflowExecutionStore {
  const client = options.client ?? DynamoDBDocumentClient.from(new DynamoDBClient({}), { marshallOptions: { removeUndefinedValues: true } })
  const TableName = options.tableName
  if (!TableName) throw new Error('tableName is required')
  const key = (PK: string, SK = 'META') => ({ PK, SK })
  const get = async (PK: string, SK = 'META') => (await client.send(new GetCommand({ TableName, Key: key(PK, SK), ConsistentRead: true }))).Item as Item | undefined
  const put = async (item: Item, condition = 'attribute_not_exists(PK)') => {
    for (let attempt = 0; ; attempt++) {
      try { return await client.send(new PutCommand({ TableName, Item: item, ConditionExpression: condition })) }
      catch (error) { if (!isReplicationConflict(error) || attempt >= 7) throw error; await backoff(attempt) }
    }
  }
  const remove = async (PK: string, SK = 'META') => client.send(new DeleteCommand({ TableName, Key: key(PK, SK) }))
  const owner = () => ownerContext.getStore()
  const fence = (item: Item) => {
    if (item.deleted) throw new Error(`Run was deleted and cannot be reused: ${item.PK}`)
    const current = owner()
    if ((current && item.run?.lease?.owner !== current) || (!current && item.run?.lease)) {
      throw new Error(`Lost workflow lease or missing withLeaseOwner context: ${item.PK}`)
    }
  }
  const runOf = (item: Item): WorkflowExecution => copy(item.run)
  const dueFields = (run: WorkflowExecution) => {
    if (run.status === 'queued' || run.status === 'running') {
      return { duePK: 'RUNNING', dueSK: run.lease?.expiresAt ?? run.updatedAt }
    }
    if (run.status === 'paused' && run.waitingFor?.signalName === '__timer' && typeof run.waitingFor.deadline === 'number') {
      return { duePK: 'TIMER_RUN', dueSK: run.waitingFor.deadline }
    }
    return { duePK: undefined, dueSK: undefined }
  }
  const scheduleDueFields = (schedule: Item) => {
    // An accepted bucket remains discoverable even when the definition advances or is disabled.
    if (schedule.pendingBucket) return { duePK: 'SCHEDULE', dueSK: schedule.pendingBucket.lease.expiresAt }
    if (schedule.enabled && typeof schedule.nextFireAt === 'number' &&
        (schedule.lastStartedAt === undefined || schedule.nextFireAt > schedule.lastStartedAt)) {
      return { duePK: 'SCHEDULE', dueSK: schedule.nextFireAt }
    }
    return { duePK: undefined, dueSK: undefined }
  }
  const resolvesPending = (events: readonly WorkflowEvent[], pending: PendingDelivery | undefined) => pending && events.some(event =>
    pending.kind === 'signal'
      ? event.type === 'SIGNAL_RESOLVED' && event.signalId === pending.delivery.signalId && event.name === pending.delivery.name
      : event.type === 'APPROVAL_RESOLVED' && event.approvalId === pending.approval.approvalId)
  async function replace(current: Item, changes: Item): Promise<Item> {
    const next = { ...current, ...changes, version: current.version + 1 }
    // PutItem replaces the entire item with a version guard; no two readers can silently overwrite each other.
    await client.send(new PutCommand({ TableName, Item: next, ConditionExpression: '#v = :v', ExpressionAttributeNames: expressionNames, ExpressionAttributeValues: { ':v': current.version } }))
    return next
  }
  async function mutate(PK: string, change: (item: Item) => Item | undefined, attempts = 8): Promise<Item | undefined> {
    for (let i = 0; i < attempts; i++) {
      const current = await get(PK)
      if (!current) return undefined
      const changes = change(current)
      if (!changes) return current
      try { return await replace(current, changes) } catch (error) { if (!isConflict(error)) throw error; await backoff(i) }
    }
    throw new Error(`Contention retry limit exceeded: ${PK}`)
  }
  async function candidates(duePK: string, now: number, limit: number, claim: (item: Item) => Promise<any>): Promise<any[]> {
    if (limit <= 0) return []
    const found: any[] = []
    let ExclusiveStartKey: Item | undefined
    do {
      const page = await client.send(new QueryCommand({ TableName, IndexName: 'DueIndex', KeyConditionExpression: 'duePK = :p AND dueSK <= :n', ExpressionAttributeValues: { ':p': duePK, ':n': now }, ExclusiveStartKey, Limit: 100 }))
      for (const candidate of page.Items ?? []) {
        // A GSI is eventual even on MRSC. It only supplies candidates; the authoritative read and claim are on the base item.
        const item = await get(candidate.PK)
        if (!item) continue
        const claimed = await claim(item)
        if (claimed) found.push(claimed)
        if (found.length >= limit) break
      }
      if (found.length >= limit) break
      ExclusiveStartKey = page.LastEvaluatedKey
    } while (ExclusiveStartKey)
    return found
  }
  const stored = (runId: string, start: number, events: readonly WorkflowEvent[]): StoredWorkflowEvent[] => events.map((event, i) => ({ runId, eventIndex: start + i, eventType: event.type, stepId: 'stepId' in event ? event.stepId : undefined, event, createdAt: event.ts }))
  async function readEvents(args: ReadEventsArgs): Promise<readonly StoredWorkflowEvent[]> {
    const meta = await get(runKey(args.runId))
    if (!meta || meta.deleted) return []
    const parts: StoredWorkflowEvent[][] = []
    let pointer: string | undefined = meta.head
    let expected = meta.nextIndex as number
    while (pointer) {
      const segment = await get(runKey(args.runId), pointer)
      if (!segment || segment.events.length + segment.start !== expected) throw new Error(`Corrupt event chain for ${args.runId}`)
      parts.push(segment.events)
      expected = segment.start
      pointer = segment.previous
    }
    if (expected !== 0) throw new Error(`Incomplete event chain for ${args.runId}`)
    return parts.reverse().flat().filter(event => event.eventIndex >= (args.fromIndex ?? 0)).map(copy)
  }
  async function deliver(runId: string, id: string, now: number, pending: PendingDelivery, waiting: (run: WorkflowExecution) => boolean): Promise<{ kind: 'delivered' | 'duplicate' | 'not-waiting' | 'not-found', run?: WorkflowExecution }> {
    const PK = runKey(runId)
    for (let i = 0; i < 8; i++) {
      const current = await get(PK)
      if (!current || current.deleted) return { kind: 'not-found' }
      const run = runOf(current)
      if ((current.deliveredIds ?? []).includes(id)) return { kind: 'duplicate', run }
      if (!waiting(run)) return { kind: 'not-waiting', run }
      const next: WorkflowExecution = { ...run, status: 'queued', awaiting: undefined, waitingFor: undefined, pendingApproval: undefined, wakeAt: undefined, updatedAt: now }
      try {
        const result = await replace(current, { run: next, pending, deliveredIds: [...(current.deliveredIds ?? []), id], ...dueFields(next) })
        return { kind: 'delivered', run: runOf(result) }
      } catch (error) { if (!isConflict(error)) throw error; await backoff(i) }
    }
    throw new Error(`Delivery contention retry limit: ${runId}`)
  }
  const store: FencedWorkflowExecutionStore = {
    withLeaseOwner<T>(leaseOwner: string, run: () => Promise<T>) { return ownerContext.run(leaseOwner, run) },
    async createRun(args) {
      const PK = runKey(args.runId)
      const run: WorkflowExecution = { runId: args.runId, workflowId: args.workflowId, workflowVersion: args.workflowVersion, status: 'queued', input: args.input, createdAt: args.now, updatedAt: args.now }
      try { await put({ ...key(PK), version: 0, nextIndex: 0, run, ...dueFields(run) }); return { kind: 'created', run } as const }
      catch (error) { if (!isConflict(error)) throw error; const existing = await get(PK); if (!existing) throw error; if (existing.deleted) throw new Error('Deleted run IDs cannot be reused'); return { kind: 'existing', run: runOf(existing) } as const }
    },
    async loadRun(runId) { const item = await get(runKey(runId)); return item && !item.deleted ? runOf(item) : undefined },
    async loadExecution(runId) { const run = await store.loadRun(runId); return run ? { run, events: await readEvents({ runId }) } : undefined },
    async loadRunState(runId) { const item = await get(runKey(runId)); return item?.deleted ? undefined : copy(item?.state as RunState | undefined) },
    async saveRunState({ state }) {
      const PK = runKey(state.runId)
      for (let i = 0; i < 8; i++) {
        const current = await get(PK)
        if (current) fence(current)
        const previous = current?.run as WorkflowExecution | undefined
        const run: WorkflowExecution = { runId: state.runId, workflowId: state.workflowId, workflowVersion: state.workflowVersion, status: state.status, input: state.input, output: state.output, error: state.error, awaiting: state.awaiting, waitingFor: state.waitingFor, pendingApproval: state.pendingApproval, wakeAt: state.waitingFor?.signalName === '__timer' ? state.waitingFor.deadline : undefined, lease: previous?.lease, createdAt: state.createdAt, updatedAt: state.updatedAt }
        try {
          if (current) await replace(current, { state: copy(state), run, ...dueFields(run) })
          else await put({ ...key(PK), version: 0, nextIndex: 0, state: copy(state), run, ...dueFields(run) })
          return
        } catch (error) { if (!isConflict(error)) throw error; await backoff(i) }
      }
      throw new Error(`State contention retry limit: ${state.runId}`)
    },
    async deleteRun(runId: string, reason: DeleteReason) {
      const PK = runKey(runId)
      const item = await get(PK)
      if (!item || item.deleted) return
      fence(item)
      // Tombstone blocks accidental run resurrection. Event segments are retained for audit/explicit retention policy.
      await mutate(PK, current => { fence(current); return { deleted: true, state: undefined, pending: undefined, run: { ...current.run, status: reason, lease: undefined }, duePK: undefined, dueSK: undefined } })
    },
    async appendEvents(args: AppendEventsArgs) {
      const PK = runKey(args.runId)
      const meta = await get(PK)
      if (!meta || meta.deleted || meta.nextIndex !== args.expectedNextIndex) throw new LogConflictError(args.runId, args.expectedNextIndex)
      fence(meta)
      if (!args.events.length) return { nextIndex: args.expectedNextIndex }
      const SK = `SEG#${randomUUID()}`
      // Stage immutable batch first, then publish it with one conditional update to the run item.
      // Orphan segments from losing writers are invisible to readers and may be removed offline.
      const segment = { ...key(PK, SK), start: args.expectedNextIndex, events: stored(args.runId, args.expectedNextIndex, args.events), previous: meta.head }
      try { await put(segment) } catch (error) {
        // Retried immutable staging writes can report a conditional failure after success.
        const existing = await get(PK, SK)
        if (!existing || existing.start !== segment.start || existing.previous !== segment.previous || !isDeepStrictEqual(JSON.parse(JSON.stringify(existing.events)), JSON.parse(JSON.stringify(segment.events)))) throw error
      }
      for (let attempt = 0; attempt < 8; attempt++) {
        const current = attempt === 0 ? meta : await get(PK)
        if (!current || current.nextIndex !== args.expectedNextIndex || current.head !== meta.head) {
          throw new LogConflictError(args.runId, args.expectedNextIndex)
        }
        fence(current)
        try {
          // The version fences concurrent state changes; the lease owner rejects stale workers.
          await client.send(new UpdateCommand({ TableName, Key: key(PK), UpdateExpression: 'SET head = :head, nextIndex = :next, #v = #v + :one' + (resolvesPending(args.events, current.pending) ? ' REMOVE #pending, #timerLease' : ''), ConditionExpression: owner() ? 'nextIndex = :index AND #v = :version AND #run.#lease.#owner = :owner' : 'nextIndex = :index AND #v = :version', ExpressionAttributeNames: { '#v': 'version', ...(resolvesPending(args.events, current.pending) ? { '#pending': 'pending', '#timerLease': 'timerLease' } : {}), ...(owner() ? { '#run': 'run', '#lease': 'lease', '#owner': 'owner' } : {}) }, ExpressionAttributeValues: { ':head': SK, ':next': args.expectedNextIndex + args.events.length, ':one': 1, ':index': args.expectedNextIndex, ':version': current.version, ...(owner() ? { ':owner': owner() } : {}) } }))
          return { nextIndex: args.expectedNextIndex + args.events.length }
        } catch (error) {
          // A timeout (or an SDK retry whose first attempt committed) is ambiguous.
          // Only our immutable segment identity proves this exact append succeeded.
          let confirmed: Item | undefined
          try { confirmed = await get(PK) } catch { throw error }
          if (confirmed?.head === SK && confirmed.nextIndex === args.expectedNextIndex + args.events.length) {
            return { nextIndex: confirmed.nextIndex }
          }
          if (!isConflict(error)) throw error
          await backoff(attempt)
        }
      }
      throw new LogConflictError(args.runId, args.expectedNextIndex)
    },
    readEvents,
    async claimRun(args: ClaimRunArgs): Promise<ClaimRunResult> {
      const PK = runKey(args.runId)
      for (let i = 0; i < 8; i++) {
        const current = await get(PK)
        if (!current || current.deleted) return { kind: 'not-found' }
        const run = runOf(current)
        if (terminal(run.status) || (run.lease && run.lease.owner !== args.leaseOwner && run.lease.expiresAt > args.now)) return { kind: 'not-claimable', run }
        const claimed = { ...run, status: 'running' as const, lease: lease(args.leaseOwner, args.leaseMs, args.now), updatedAt: args.now }
        try { await replace(current, { run: claimed, ...dueFields(claimed) }); return { kind: 'claimed', run: claimed } }
        catch (error) { if (!isConflict(error)) throw error; await backoff(i) }
      }
      throw new Error(`Run claim contention: ${args.runId}`)
    },
    async heartbeatRunLease(args) { await mutate(runKey(args.runId), current => (() => { if (current.run.lease?.owner !== args.leaseOwner || current.run.lease.expiresAt <= args.now) throw new Error(`Lost workflow lease: ${current.PK}`); return { run: { ...current.run, lease: lease(args.leaseOwner, args.leaseMs, args.now), updatedAt: args.now }, ...dueFields({ ...current.run, lease: lease(args.leaseOwner, args.leaseMs, args.now) }) } })()) },
    async releaseRunLease(args) { await mutate(runKey(args.runId), current => current.run.lease?.owner === args.leaseOwner ? { run: { ...current.run, lease: undefined }, ...dueFields({ ...current.run, lease: undefined }) } : undefined) },
    async markRunPaused(args) { await mutate(runKey(args.runId), current => { fence(current); const run = { ...current.run, status: 'paused', awaiting: args.awaiting, waitingFor: args.waitingFor, pendingApproval: args.pendingApproval, wakeAt: args.wakeAt, lease: undefined, updatedAt: args.now }; return { run, ...dueFields(run) } }) },
    async markRunFinished(args) { await mutate(runKey(args.runId), current => { fence(current); const run = { ...current.run, status: 'finished', output: args.output, awaiting: undefined, waitingFor: undefined, pendingApproval: undefined, wakeAt: undefined, lease: undefined, updatedAt: args.now }; return { run, ...dueFields(run) } }) },
    async markRunErrored(args) { await mutate(runKey(args.runId), current => { fence(current); const run = { ...current.run, status: 'errored', error: args.error, awaiting: undefined, waitingFor: undefined, pendingApproval: undefined, wakeAt: undefined, lease: undefined, updatedAt: args.now }; return { run, ...dueFields(run) } }) },
    async scheduleTimer(args: ScheduleTimerArgs) {
      const PK = timerKey(args.runId, args.signalId)
      const item = { ...key(PK), version: 0, runId: args.runId, workflowId: args.workflowId, workflowVersion: args.workflowVersion, wakeAt: args.wakeAt, signalId: args.signalId, duePK: 'TIMER', dueSK: args.wakeAt }
      try { await put(item) } catch (error) { if ((error as { name?: string })?.name !== 'ConditionalCheckFailedException') throw error }
      await mutate(runKey(args.runId), current => { if (current.deleted || terminal(current.run.status) || current.pending) return undefined; fence(current); const run = { ...current.run, wakeAt: args.wakeAt, updatedAt: args.now }; return { run, ...dueFields(run) } })
    },
    async claimDueTimers(args) {
      // Paused run state is itself a durable timer index; no save-state/scheduleTimer crash gap.
      const repaired = await candidates('TIMER_RUN', args.now, args.limit, async item => {
        const run = item.run as WorkflowExecution
        const wait = run?.waitingFor
        if (item.deleted || run?.status !== 'paused' || wait?.signalName !== '__timer' || typeof wait.deadline !== 'number' || wait.deadline > args.now) return undefined
        if (item.timerLease?.expiresAt > args.now) return undefined
        const signalId = `timer:${run.runId}:${wait.stepId}:${wait.deadline}`
        try {
          await replace(item, { timerLease: lease(args.leaseOwner, args.leaseMs, args.now) })
          return { runId: run.runId, workflowId: run.workflowId, workflowVersion: run.workflowVersion, wakeAt: wait.deadline, signalId }
        } catch (error) { if (isConflict(error)) return undefined; throw error }
      })
      if (repaired.length >= args.limit) return repaired
      const scheduled = await candidates('TIMER', args.now, args.limit - repaired.length, async item => {
        if (typeof item.wakeAt !== 'number' || item.wakeAt > args.now || !item.duePK) return undefined
        const current = await get(runKey(item.runId))
        // Real runtime waits are covered by TIMER_RUN. Remove orphan/completed/delivered timers.
        if (!current || current.deleted || terminal(current.run.status) || current.pending || current.state) {
          await remove(item.PK)
          return undefined
        }
        if (item.lease?.expiresAt > args.now) return undefined
        try { await replace(item, { lease: lease(args.leaseOwner, args.leaseMs, args.now) }); return { runId: item.runId, workflowId: item.workflowId, workflowVersion: item.workflowVersion, wakeAt: item.wakeAt, signalId: item.signalId } }
        catch (error) { if (isConflict(error)) return undefined; throw error }
      })
      return [...repaired, ...scheduled]
    },
    async deliverSignal<TPayload>(args: DeliverSignalArgs<TPayload>): Promise<DeliverSignalResult> {
      const result = await deliver(args.runId, `signal:${args.delivery.signalId}`, args.now, { kind: 'signal', delivery: args.delivery, acceptedAt: args.now }, run => run.status === 'paused' && ([run.waitingFor, ...(run.awaiting ?? []).filter(x => x.type === 'signal')].some(x => x?.signalName === args.delivery.name && (!args.delivery.stepId || !x.stepId || x.stepId === args.delivery.stepId))))
      if (result.kind === 'delivered' || result.kind === 'duplicate') await remove(timerKey(args.runId, args.delivery.signalId))
      return result as DeliverSignalResult
    },
    async deliverApproval(args: DeliverApprovalArgs): Promise<DeliverApprovalResult> {
      return await deliver(args.runId, `approval:${args.approval.approvalId}`, args.now, { kind: 'approval', approval: args.approval, acceptedAt: args.now }, run => run.status === 'paused' && (run.pendingApproval?.approvalId === args.approval.approvalId || run.awaiting?.some(x => x.type === 'approval' && x.approvalId === args.approval.approvalId) === true)) as DeliverApprovalResult
    },
    async upsertSchedule(args: UpsertScheduleArgs) {
      if (!['skip', 'allow'].includes(args.overlapPolicy)) {
        throw new Error(`Unsupported schedule overlap policy: ${args.overlapPolicy}. Use skip or allow.`)
      }
      if (!Number.isFinite(args.now) || (args.nextFireAt !== undefined && !Number.isFinite(args.nextFireAt))) {
        throw new Error('Schedule timestamps must be finite')
      }
      const PK = scheduleKey(args.scheduleId)
      const fields = { scheduleId: args.scheduleId, workflowId: args.workflowId, workflowVersion: args.workflowVersion, spec: args.schedule, overlapPolicy: args.overlapPolicy, input: args.input, nextFireAt: args.nextFireAt, enabled: args.enabled, definitionUpdatedAt: args.now }
      const existing = await get(PK)
      if (!existing) {
        try { await put({ ...key(PK), version: 0, ...fields, ...scheduleDueFields(fields) }); return }
        catch (error) { if (!isConflict(error)) throw error }
      }
      await mutate(PK, current => {
        // A delayed materializer must not roll the schedule back or replace a newer definition.
        if (current.definitionUpdatedAt > args.now) return undefined
        if (current.workflowId !== args.workflowId || current.overlapPolicy !== args.overlapPolicy) {
          throw new Error('Schedule workflowId and overlapPolicy are immutable; use a new scheduleId')
        }
        if (args.enabled && current.enabled && args.nextFireAt !== undefined && current.nextFireAt !== undefined && args.nextFireAt < current.nextFireAt) return undefined
        const nextFireAt = args.nextFireAt
        const next = { ...current, ...fields, nextFireAt }
        return { ...fields, nextFireAt, ...scheduleDueFields(next) }
      })
    },
    async claimDueScheduleBuckets(args) { return candidates('SCHEDULE', args.now, args.limit, async schedule => {
      const pending = schedule.pendingBucket
      if (pending) {
        if (pending.lease.expiresAt > args.now) return undefined
        const nextBucket = { ...pending, lease: lease(args.leaseOwner, args.leaseMs, args.now) }
        try {
          await replace(schedule, { pendingBucket: nextBucket, ...scheduleDueFields({ ...schedule, pendingBucket: nextBucket }) })
          return nextBucket as ScheduleBucket
        } catch (error) { if (isConflict(error)) return undefined; throw error }
      }
      if (!schedule.enabled || typeof schedule.nextFireAt !== 'number' || !Number.isFinite(schedule.nextFireAt) || schedule.nextFireAt > args.now ||
          (schedule.lastStartedAt !== undefined && schedule.nextFireAt <= schedule.lastStartedAt)) return undefined
      if (!['skip', 'allow'].includes(schedule.overlapPolicy)) throw new Error(`Unsupported schedule overlap policy: ${schedule.overlapPolicy}`)
      if (schedule.overlapPolicy === 'skip' && schedule.activeRunId) {
        const active = await store.loadRun(schedule.activeRunId)
        if (active && !terminal(active.status)) {
          // Consume this tick atomically. A concurrent schedule update forces fresh evaluation.
          const next = { ...schedule, lastStartedAt: schedule.nextFireAt }
          try { await replace(schedule, { lastStartedAt: next.lastStartedAt, ...scheduleDueFields(next) }) }
          catch (error) { if (!isConflict(error)) throw error }
          return undefined
        }
      }
      const bucketId = String(schedule.nextFireAt)
      const bucket = { scheduleId: schedule.scheduleId, bucketId, workflowId: schedule.workflowId, workflowVersion: schedule.workflowVersion,
        runId: `${schedule.workflowId}:${schedule.scheduleId}:${bucketId}`, fireAt: schedule.nextFireAt, input: schedule.input,
        overlapPolicy: schedule.overlapPolicy, lease: lease(args.leaseOwner, args.leaseMs, args.now) }
      try {
        await replace(schedule, { pendingBucket: bucket, ...scheduleDueFields({ ...schedule, pendingBucket: bucket }) })
        return bucket as ScheduleBucket
      } catch (error) { if (isConflict(error)) return undefined; throw error }
    }) },
    async markScheduleBucketStarted(args) {
      const leaseOwner = owner()
      if (!leaseOwner) throw new Error('withLeaseOwner is required to mark a schedule bucket started')
      await mutate(scheduleKey(args.scheduleId), schedule => {
        const pending = schedule.pendingBucket
        if (!pending && String(schedule.lastStartedAt) === args.bucketId && schedule.activeRunId === args.runId) return undefined
        if (!pending || pending.bucketId !== args.bucketId || pending.runId !== args.runId || pending.lease.owner !== leaseOwner) {
          throw new Error('Lost schedule bucket lease or mismatched bucket')
        }
        const next = { ...schedule, pendingBucket: undefined, lastStartedAt: pending.fireAt, activeRunId: args.runId }
        return { pendingBucket: undefined, lastStartedAt: pending.fireAt, activeRunId: args.runId, ...scheduleDueFields(next) }
      })
    },
    async deferRunRecovery(args) {
      await mutate(runKey(args.runId), current => {
        if (current.deleted || terminal(current.run.status) || (current.run.lease && current.run.lease.owner !== args.leaseOwner)) return undefined
        const run = { ...current.run, lease: lease(args.leaseOwner, 60_000, Math.max(args.now, Date.now())) }
        return { run, recoveryError: String(args.error), ...dueFields(run) }
      })
    },
    async claimStaleRuns(args) { return candidates('RUNNING', args.now, args.limit, async item => {
      if (item.deleted || !['queued', 'running'].includes(item.run.status) || (item.run.lease && item.run.lease.expiresAt > args.now)) return undefined
      const next = { ...item.run, status: 'running', lease: lease(args.leaseOwner, args.leaseMs, args.now), updatedAt: args.now }
      let claimed: Item
      try { claimed = await replace(item, { run: next, ...dueFields(next) }) }
      catch (error) { if (isConflict(error)) return undefined; throw error }
      try { if (claimed.pending) {
        await store.withLeaseOwner(args.leaseOwner, async () => {
          if (!claimed.state) throw new Error('Pending delivery has no durable run state')
          const history = await readEvents({ runId: next.runId })
          const seed = buildPendingResolution(claimed.pending, claimed.state, history.map(event => event.event))
          if (seed) await store.appendEvents({ runId: next.runId, expectedNextIndex: history.length, events: [seed] })
          else await mutate(item.PK, current => { fence(current); return { pending: undefined } })
        })
      }
      } catch (error) {
        await store.deferRunRecovery!({ runId: next.runId, leaseOwner: args.leaseOwner, now: args.now, error })
        return undefined
      }
      return { run: next, lease: next.lease }
    }) },
    async listRuns(args) {
      // Administrative listing; Scan is intentionally explicit and is not used by scheduler/claims.
      const runs: RunSummary[] = []
      let ExclusiveStartKey: Item | undefined
      do {
        const page = await client.send(new ScanCommand({ TableName, ExclusiveStartKey, FilterExpression: 'begins_with(PK, :prefix) AND SK = :meta', ExpressionAttributeValues: { ':prefix': 'RUN#', ':meta': 'META' } }))
        for (const item of page.Items ?? []) { const run = item.run as WorkflowExecution; if (!item.deleted && run && (!args.workflowId || run.workflowId === args.workflowId) && (!args.status || run.status === args.status)) runs.push(run) }
        ExclusiveStartKey = page.LastEvaluatedKey
      } while (ExclusiveStartKey)
      runs.sort((a, b) => b.updatedAt - a.updatedAt)
      const offset = args.cursor ? Number(args.cursor) || 0 : 0
      return runs.slice(offset, offset + args.limit).map(({ runId, workflowId, workflowVersion, status, awaiting, waitingFor, pendingApproval, wakeAt, createdAt, updatedAt }) => ({ runId, workflowId, workflowVersion, status, awaiting, waitingFor, pendingApproval, wakeAt, createdAt, updatedAt }))
    },
    async getRunTimeline(runId) { return store.loadExecution(runId) },
  }
  return store
}
