import type { PipelinePairInput } from '@/api/pipelinePairs'

export function pipelinePairDraftKey(userId: string): string {
  return `xdts.bidirectional.pending.${userId}`
}

export function readPipelinePairDraft(userId: string): PipelinePairInput | undefined {
  try {
    const raw = sessionStorage.getItem(pipelinePairDraftKey(userId))
    if (!raw) return undefined
    const value: unknown = JSON.parse(raw)
    if (typeof value !== 'object' || value === null) return undefined
    const input = value as Partial<PipelinePairInput>
    if (typeof input.requestId !== 'string' || typeof input.name !== 'string') return undefined
    for (const side of [input.a, input.b]) {
      if (
        !side ||
        typeof side.endpointId !== 'string' ||
        typeof side.nodeId !== 'string' ||
        (!side.rules &&
          (typeof side.strategyId !== 'string' ||
            !side.strategyVersion ||
            !Number.isInteger(side.strategyVersion)))
      )
        return undefined
    }
    return input as PipelinePairInput
  } catch {
    return undefined
  }
}

export function preparePipelinePairDraft(
  values: Omit<PipelinePairInput, 'requestId'>,
  previous: PipelinePairInput | undefined,
  rulesEdited: boolean,
): PipelinePairInput {
  const sameEndpoints =
    previous &&
    previous.name === values.name &&
    previous.a.endpointId === values.a.endpointId &&
    previous.a.nodeId === values.a.nodeId &&
    previous.b.endpointId === values.b.endpointId &&
    previous.b.nodeId === values.b.nodeId
  if (sameEndpoints && !rulesEdited) return previous
  const same =
    sameEndpoints &&
    JSON.stringify(previous.a.rules) === JSON.stringify(values.a.rules) &&
    JSON.stringify(previous.b.rules) === JSON.stringify(values.b.rules)
  return { ...values, requestId: same ? previous.requestId : crypto.randomUUID() }
}

export function storePipelinePairDraft(
  userId: string,
  input: PipelinePairInput | undefined,
): void {
  try {
    if (input) sessionStorage.setItem(pipelinePairDraftKey(userId), JSON.stringify(input))
    else sessionStorage.removeItem(pipelinePairDraftKey(userId))
  } catch {
    // The caller retains the request for retries when storage is unavailable.
  }
}
