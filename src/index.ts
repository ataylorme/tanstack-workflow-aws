import { AsyncLocalStorage } from 'node:async_hooks'
import { randomUUID } from 'node:crypto'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient, DeleteCommand, GetCommand, PutCommand, QueryCommand, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb'
import { LogConflictError } from '@tanstack/workflow-core'
import type { DeleteReason, RunState, WorkflowEvent } from '@tanstack/workflow-core'
import type {
  WorkflowExecutionStore, WorkflowExecution, StoredWorkflowEvent, WorkflowLease,
  AppendEventsArgs, ReadEventsArgs, ClaimRunArgs, ClaimRunResult, DeliverSignalArgs,
  DeliverSignalResult, DeliverApprovalArgs, DeliverApprovalResult, ScheduleBucket,
  RunSummary, UpsertScheduleArgs, ScheduleTimerArgs,
} from '@tanstack/workflow-runtime'

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
const isConflict = (error: unknown) => ['ConditionalCheckFailedException', 'ReplicatedWriteConflictException'].includes((error as { name?: string })?.name ?? '')
const lease = (owner: string, ms: number, now: number): WorkflowLease => ({ owner, expiresAt: now + ms })
const terminal = (status: string) => ['finished', 'errored', 'aborted'].includes(status)
const runKey = (id: string) => `RUN#${id}`
const timerKey = (runId: string, signalId: string) => `TIMER#${encodeURIComponent(runId)}#${encodeURIComponent(signalId)}`
const scheduleKey = (id: string) => `SCHEDULE#${id}`
const bucketKey = (scheduleId: string, bucketId: string) => `BUCKET#${encodeURIComponent(scheduleId)}#${encodeURIComponent(bucketId)}`
const expressionNames = { '#v': 'version' }

export function createDynamoWorkflowExecutionStore(options: DynamoWorkflowStoreOptions): FencedWorkflowExecutionStore {
  const client = options.client ?? DynamoDBDocumentClient.from(new DynamoDBClient({}), { marshallOptions: { removeUndefinedValues: true } })
  const TableName = options.tableName
  if (!TableName) throw new Error('tableName is required')
  const key = (PK: string, SK = 'META') => ({ PK, SK })
  const get = async (PK: string, SK = 'META') => (await client.send(new GetCommand({ TableName, Key: key(PK, SK), ConsistentRead: true }))).Item as Item | undefined
  const put = async (item: Item, condition = 'attribute_not_exists(PK)') => client.send(new PutCommand({ TableName, Item: item, ConditionExpression: condition }))
  const remove = async (PK: string, SK = 'META') => client.send(new DeleteCommand({ TableName, Key: key(PK, SK) }))
  const owner = () => ownerContext.getStore()
  const fence = (item: Item) => { const current = owner(); if (current && item.run?.lease?.owner !== current) throw new Error(`Lost workflow lease: ${item.PK}`) }
  const runOf = (item: Item): WorkflowExecution => copy(item.run)
  const dueFields = (run: WorkflowExecution) => run.status === 'running' && run.lease
    ? { duePK: 'RUNNING', dueSK: run.lease.expiresAt }
    : { duePK: undefined, dueSK: undefined }
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
      try { return await replace(current, changes) } catch (error) { if (!isConflict(error)) throw error }
    }
    throw new Error(`Contention retry limit exceeded: ${PK}`)
  }
  async function candidates(duePK: string, now: number, limit: number, claim: (item: Item) => Promise<any>): Promise<any[]> {
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
    if (!meta) return []
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
  async function deliver(runId: string, id: string, now: number, waiting: (run: WorkflowExecution) => boolean): Promise<{ kind: 'delivered' | 'duplicate' | 'not-waiting' | 'not-found', run?: WorkflowExecution }> {
    const PK = runKey(runId)
    for (let i = 0; i < 8; i++) {
      const current = await get(PK)
      if (!current) return { kind: 'not-found' }
      const run = runOf(current)
      if ((current.deliveredIds ?? []).includes(id)) return { kind: 'duplicate', run }
      if (!waiting(run)) return { kind: 'not-waiting', run }
      const next: WorkflowExecution = { ...run, status: 'queued', awaiting: undefined, waitingFor: undefined, pendingApproval: undefined, wakeAt: undefined, updatedAt: now }
      try {
        const result = await replace(current, { run: next, deliveredIds: [...(current.deliveredIds ?? []), id], ...dueFields(next) })
        return { kind: 'delivered', run: runOf(result) }
      } catch (error) { if (!isConflict(error)) throw error }
    }
    throw new Error(`Delivery contention retry limit: ${runId}`)
  }
  const store: FencedWorkflowExecutionStore = {
    withLeaseOwner<T>(leaseOwner: string, run: () => Promise<T>) { return ownerContext.run(leaseOwner, run) },
    async createRun(args) {
      const PK = runKey(args.runId)
      const run: WorkflowExecution = { runId: args.runId, workflowId: args.workflowId, workflowVersion: args.workflowVersion, status: 'queued', input: args.input, createdAt: args.now, updatedAt: args.now }
      try { await put({ ...key(PK), version: 0, nextIndex: 0, run }); return { kind: 'created', run } as const }
      catch (error) { if (!isConflict(error)) throw error; const existing = await get(PK); if (!existing) throw error; return { kind: 'existing', run: runOf(existing) } as const }
    },
    async loadRun(runId) { const item = await get(runKey(runId)); return item ? runOf(item) : undefined },
    async loadExecution(runId) { const run = await store.loadRun(runId); return run ? { run, events: await readEvents({ runId }) } : undefined },
    async loadRunState(runId) { return copy((await get(runKey(runId)))?.state as RunState | undefined) },
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
        } catch (error) { if (!isConflict(error)) throw error }
      }
      throw new Error(`State contention retry limit: ${state.runId}`)
    },
    async deleteRun(runId: string, _reason: DeleteReason) {
      const PK = runKey(runId)
      const item = await get(PK)
      if (!item) return
      fence(item)
      // Tombstone blocks accidental run resurrection. Event segments are retained for audit/explicit retention policy.
      await mutate(PK, current => { fence(current); return { state: undefined, run: { ...current.run, status: 'aborted' }, duePK: undefined, dueSK: undefined } })
    },
    async appendEvents(args: AppendEventsArgs) {
      const PK = runKey(args.runId)
      const meta = await get(PK)
      if (!meta || meta.nextIndex !== args.expectedNextIndex) throw new LogConflictError(args.runId, args.expectedNextIndex)
      fence(meta)
      if (!args.events.length) return { nextIndex: args.expectedNextIndex }
      const SK = `SEG#${randomUUID()}`
      // Stage immutable batch first, then publish it with one conditional update to the run item.
      // Orphan segments from losing writers are invisible to readers and may be removed offline.
      await put({ ...key(PK, SK), start: args.expectedNextIndex, events: stored(args.runId, args.expectedNextIndex, args.events), previous: meta.head })
      for (let attempt = 0; attempt < 8; attempt++) {
        const current = attempt === 0 ? meta : await get(PK)
        if (!current || current.nextIndex !== args.expectedNextIndex || current.head !== meta.head) {
          throw new LogConflictError(args.runId, args.expectedNextIndex)
        }
        fence(current)
        try {
          // The version fences concurrent state changes; the lease owner rejects stale workers.
          await client.send(new UpdateCommand({ TableName, Key: key(PK), UpdateExpression: 'SET head = :head, nextIndex = :next, #v = #v + :one', ConditionExpression: owner() ? 'nextIndex = :index AND #v = :version AND #run.#lease.#owner = :owner' : 'nextIndex = :index AND #v = :version', ExpressionAttributeNames: { '#v': 'version', '#run': 'run', '#lease': 'lease', '#owner': 'owner' }, ExpressionAttributeValues: { ':head': SK, ':next': args.expectedNextIndex + args.events.length, ':one': 1, ':index': args.expectedNextIndex, ':version': current.version, ...(owner() ? { ':owner': owner() } : {}) } }))
          return { nextIndex: args.expectedNextIndex + args.events.length }
        } catch (error) {
          if (!isConflict(error)) throw error
        }
      }
      throw new LogConflictError(args.runId, args.expectedNextIndex)
    },
    readEvents,
    async claimRun(args: ClaimRunArgs): Promise<ClaimRunResult> {
      const PK = runKey(args.runId)
      for (let i = 0; i < 8; i++) {
        const current = await get(PK)
        if (!current) return { kind: 'not-found' }
        const run = runOf(current)
        if (terminal(run.status) || (run.lease && run.lease.owner !== args.leaseOwner && run.lease.expiresAt > args.now)) return { kind: 'not-claimable', run }
        const claimed = { ...run, status: 'running' as const, lease: lease(args.leaseOwner, args.leaseMs, args.now), updatedAt: args.now }
        try { await replace(current, { run: claimed, ...dueFields(claimed) }); return { kind: 'claimed', run: claimed } }
        catch (error) { if (!isConflict(error)) throw error }
      }
      throw new Error(`Run claim contention: ${args.runId}`)
    },
    async heartbeatRunLease(args) { await mutate(runKey(args.runId), current => current.run.lease?.owner === args.leaseOwner ? { run: { ...current.run, lease: lease(args.leaseOwner, args.leaseMs, args.now), updatedAt: args.now }, duePK: 'RUNNING', dueSK: args.now + args.leaseMs } : undefined) },
    async releaseRunLease(args) { await mutate(runKey(args.runId), current => current.run.lease?.owner === args.leaseOwner ? { run: { ...current.run, lease: undefined }, duePK: undefined, dueSK: undefined } : undefined) },
    async markRunPaused(args) { await mutate(runKey(args.runId), current => { fence(current); const run = { ...current.run, status: 'paused', awaiting: args.awaiting, waitingFor: args.waitingFor, pendingApproval: args.pendingApproval, wakeAt: args.wakeAt, lease: undefined, updatedAt: args.now }; return { run, ...dueFields(run) } }) },
    async markRunFinished(args) { await mutate(runKey(args.runId), current => { fence(current); const run = { ...current.run, status: 'finished', output: args.output, awaiting: undefined, waitingFor: undefined, pendingApproval: undefined, wakeAt: undefined, lease: undefined, updatedAt: args.now }; return { run, ...dueFields(run) } }) },
    async markRunErrored(args) { await mutate(runKey(args.runId), current => { fence(current); const run = { ...current.run, status: 'errored', error: args.error, awaiting: undefined, waitingFor: undefined, pendingApproval: undefined, wakeAt: undefined, lease: undefined, updatedAt: args.now }; return { run, ...dueFields(run) } }) },
    async scheduleTimer(args: ScheduleTimerArgs) {
      const PK = timerKey(args.runId, args.signalId)
      const item = { ...key(PK), version: 0, runId: args.runId, workflowId: args.workflowId, workflowVersion: args.workflowVersion, wakeAt: args.wakeAt, signalId: args.signalId, duePK: 'TIMER', dueSK: args.wakeAt }
      try { await put(item) } catch (error) { if (!isConflict(error)) throw error }
      await mutate(runKey(args.runId), current => { fence(current); return { run: { ...current.run, wakeAt: args.wakeAt, updatedAt: args.now } } })
    },
    async claimDueTimers(args) { return candidates('TIMER', args.now, args.limit, async item => {
      if (item.wakeAt > args.now || !item.duePK || (item.lease && item.lease.owner !== args.leaseOwner && item.lease.expiresAt > args.now)) return undefined
      try { await replace(item, { lease: lease(args.leaseOwner, args.leaseMs, args.now) }); return { runId: item.runId, workflowId: item.workflowId, workflowVersion: item.workflowVersion, wakeAt: item.wakeAt, signalId: item.signalId } }
      catch (error) { if (isConflict(error)) return undefined; throw error }
    }) },
    async deliverSignal<TPayload>(args: DeliverSignalArgs<TPayload>): Promise<DeliverSignalResult> {
      const result = await deliver(args.runId, args.delivery.signalId, args.now, run => run.status === 'paused' && ([run.waitingFor, ...(run.awaiting ?? []).filter(x => x.type === 'signal')].some(x => x?.signalName === args.delivery.name && (!args.delivery.stepId || !x.stepId || x.stepId === args.delivery.stepId))))
      if (result.kind === 'delivered' || result.kind === 'duplicate') await remove(timerKey(args.runId, args.delivery.signalId))
      return result as DeliverSignalResult
    },
    async deliverApproval(args: DeliverApprovalArgs): Promise<DeliverApprovalResult> {
      return await deliver(args.runId, `approval:${args.approval.approvalId}`, args.now, run => run.status === 'paused' && (run.pendingApproval?.approvalId === args.approval.approvalId || run.awaiting?.some(x => x.type === 'approval' && x.approvalId === args.approval.approvalId) === true)) as DeliverApprovalResult
    },
    async upsertSchedule(args: UpsertScheduleArgs) {
      const PK = scheduleKey(args.scheduleId)
      const fields = { scheduleId: args.scheduleId, workflowId: args.workflowId, workflowVersion: args.workflowVersion, spec: args.schedule, overlapPolicy: args.overlapPolicy, input: args.input, nextFireAt: args.nextFireAt, enabled: args.enabled, duePK: args.enabled && args.nextFireAt !== undefined ? 'SCHEDULE' : undefined, dueSK: args.enabled ? args.nextFireAt : undefined }
      const existing = await get(PK)
      if (!existing) { try { await put({ ...key(PK), version: 0, ...fields }); return } catch (error) { if (!isConflict(error)) throw error } }
      await mutate(PK, () => fields)
    },
    async claimDueScheduleBuckets(args) { return candidates('SCHEDULE', args.now, args.limit, async schedule => {
      if (!schedule.enabled || schedule.nextFireAt > args.now) return undefined
      const bucketId = String(schedule.nextFireAt)
      const PK = bucketKey(schedule.scheduleId, bucketId)
      let bucket = await get(PK)
      if (!bucket) {
        const runId = `${schedule.workflowId}:${schedule.scheduleId}:${bucketId}`
        const value = { ...key(PK), version: 0, scheduleId: schedule.scheduleId, bucketId, workflowId: schedule.workflowId, workflowVersion: schedule.workflowVersion, runId, fireAt: schedule.nextFireAt, input: schedule.input, overlapPolicy: schedule.overlapPolicy, status: 'claimed', lease: lease(args.leaseOwner, args.leaseMs, args.now) }
        try { await put(value); return value as ScheduleBucket } catch (error) { if (!isConflict(error)) throw error; bucket = await get(PK) }
      }
      if (!bucket || bucket.status === 'started' || (bucket.lease && bucket.lease.owner !== args.leaseOwner && bucket.lease.expiresAt > args.now)) return undefined
      try { return await replace(bucket, { lease: lease(args.leaseOwner, args.leaseMs, args.now) }) as ScheduleBucket }
      catch (error) { if (isConflict(error)) return undefined; throw error }
    }) },
    async markScheduleBucketStarted(args) { await mutate(bucketKey(args.scheduleId, args.bucketId), item => { if (owner() && item.lease?.owner !== owner()) throw new Error('Lost schedule bucket lease'); return { runId: args.runId, status: 'started', lease: undefined } }) },
    async claimStaleRuns(args) { return candidates('RUNNING', args.now, args.limit, async item => {
      if (item.run.status !== 'running' || !item.run.lease || item.run.lease.expiresAt > args.now) return undefined
      const next = { ...item.run, lease: lease(args.leaseOwner, args.leaseMs, args.now), updatedAt: args.now }
      try { await replace(item, { run: next, ...dueFields(next) }); return { run: next, lease: next.lease } }
      catch (error) { if (isConflict(error)) return undefined; throw error }
    }) },
    async listRuns(args) {
      // Administrative listing; Scan is intentionally explicit and is not used by scheduler/claims.
      const runs: RunSummary[] = []
      let ExclusiveStartKey: Item | undefined
      do {
        const page = await client.send(new ScanCommand({ TableName, ExclusiveStartKey, FilterExpression: 'begins_with(PK, :prefix) AND SK = :meta', ExpressionAttributeValues: { ':prefix': 'RUN#', ':meta': 'META' } }))
        for (const item of page.Items ?? []) { const run = item.run as WorkflowExecution; if (run && (!args.workflowId || run.workflowId === args.workflowId) && (!args.status || run.status === args.status)) runs.push(run) }
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
