import type { ApprovalResult, RunState, SignalDelivery, WorkflowEvent } from './workflow.js'

/** Persisted with acceptance before the runtime is allowed to acknowledge delivery. */
export type PendingDelivery =
  | { kind: 'signal'; delivery: SignalDelivery; acceptedAt: number }
  | { kind: 'approval'; approval: ApprovalResult; acceptedAt: number }

/** Reconstruct the seed the pinned TanStack engine would append on resume.
 * Undefined means the same seed is already durable. Never replace a competing seed.
 */
export function buildPendingResolution(
  pending: PendingDelivery,
  state: RunState,
  history: readonly WorkflowEvent[],
): WorkflowEvent | undefined {
  if (pending.kind === 'signal') {
    const delivery = pending.delivery
    const waiting = state.waitingFor
    if (waiting?.signalName !== delivery.name ||
      (delivery.stepId !== undefined && waiting.stepId !== undefined && delivery.stepId !== waiting.stepId)) {
      throw new Error('Pending signal does not match the persisted wait')
    }
    const targetStepId = delivery.stepId ?? waiting.stepId
    let stepId = targetStepId
    let awaitedIndex = -1
    for (let index = history.length - 1; index >= 0; index--) {
      const event = history[index]!
      if (event.type === 'SIGNAL_AWAITED' && event.name === delivery.name && (!targetStepId || event.stepId === targetStepId)) {
        awaitedIndex = index
        stepId = event.stepId
        break
      }
    }
    if (awaitedIndex >= 0) {
      for (const event of history.slice(awaitedIndex + 1)) {
        if (event.type === 'SIGNAL_RESOLVED' && event.name === delivery.name && (!stepId || event.stepId === stepId)) {
          if (event.signalId === delivery.signalId) return undefined
          throw new Error('A competing signal resolution is already durable')
        }
      }
    }
    return {
      type: 'SIGNAL_RESOLVED', ts: pending.acceptedAt,
      stepId: stepId ?? `__resolve-${delivery.name}`, name: delivery.name,
      signalId: delivery.signalId, payload: delivery.payload, meta: delivery.meta,
    }
  }
  const approval = pending.approval
  const waiting = state.pendingApproval
  if (waiting?.approvalId !== approval.approvalId) throw new Error('Pending approval does not match the persisted wait')
  let stepId = waiting.stepId
  let requestedIndex = -1
  for (let index = history.length - 1; index >= 0; index--) {
    const event = history[index]!
    if (event.type === 'APPROVAL_REQUESTED' && event.approvalId === approval.approvalId) {
      requestedIndex = index
      stepId ??= event.stepId
      break
    }
  }
  for (const event of history.slice(requestedIndex + 1)) {
    if (event.type === 'APPROVAL_RESOLVED' && event.approvalId === approval.approvalId && (!stepId || event.stepId === stepId)) {
      if (event.approved === approval.approved && event.feedback === approval.feedback) return undefined
      throw new Error('A competing approval resolution is already durable')
    }
  }
  return {
    type: 'APPROVAL_RESOLVED', ts: pending.acceptedAt, stepId: stepId ?? '__resolve-approval',
    approvalId: approval.approvalId, approved: approval.approved, feedback: approval.feedback, meta: approval.meta,
  }
}
