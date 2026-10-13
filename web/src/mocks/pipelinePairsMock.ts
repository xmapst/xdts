import { ApiError } from '@/api/client'
import type { PipelinePairInput, PipelinePairResult } from '@/api/pipelinePairs'
import { fetchMe } from './authMock'
import { getEndpoint } from './endpointsMock'
import { getNode } from './nodesMock'
import { prepareLocalStrategyCopy } from './strategiesMock'
import { preparePipelinePair } from './pipelinesMock'

export const backendWriteParity = {
  liveRepoWrites: [
    'CreatePipelinePairRequest',
    'CreatePipeline',
    'CreateStrategy',
    'CreateStrategyVersion',
  ],
  note: 'HTTP pipeline_pair.go owns the atomic transaction.',
} as const

const requests = new Map<
  string,
  { userId: string; digest: string; result: PipelinePairResult }
>()

export async function createPipelinePair(
  input: PipelinePairInput,
): Promise<PipelinePairResult> {
  const me = await fetchMe()
  if (!me.user || me.user.role !== 'admin')
    throw new ApiError('没有权限执行此操作', 403, 'forbidden')
  const canonical = {
    requestId: input.requestId,
    topologyId: input.topologyId,
    name: input.name.trim(),
    a: {
      endpointId: input.a.endpointId,
      nodeId: input.a.nodeId,
      strategyId: input.a.strategyId,
      strategyVersion: input.a.strategyVersion,
      rules: input.a.rules,
    },
    b: {
      endpointId: input.b.endpointId,
      nodeId: input.b.nodeId,
      strategyId: input.b.strategyId,
      strategyVersion: input.b.strategyVersion,
      rules: input.b.rules,
    },
  }
  const digest = JSON.stringify(canonical)
  const [a, b, aNode, bNode] = await Promise.all([
    getEndpoint(input.a.endpointId),
    getEndpoint(input.b.endpointId),
    getNode(input.a.nodeId),
    getNode(input.b.nodeId),
  ])
  const prior = requests.get(input.requestId)
  if (prior) {
    if (prior.userId !== me.user.id || prior.digest !== digest)
      throw new ApiError('该 requestId 已用于其他双向创建请求', 409, 'request_conflict')
    return structuredClone(prior.result)
  }
  if (a.centerId === b.centerId)
    throw new ApiError('双向快捷创建需要两个不同中心', 400, 'same_center')
  if (a.centerId !== aNode.centerId || b.centerId !== bNode.centerId)
    throw new ApiError('节点与端点不在同一中心', 400, 'center_mismatch')
  if (!canonical.name || [...canonical.name].length > 110)
    throw new ApiError('双向线路名称不合法', 400, 'invalid_name')
  const first = input.a.rules
    ? { id: `pair-${input.requestId}-A`, commit: () => {} }
    : prepareLocalStrategyCopy(
        input.a.strategyId ?? '',
        input.a.strategyVersion ?? 0,
        `bidirectional-${input.requestId}-A`,
      )
  const second = input.b.rules
    ? { id: `pair-${input.requestId}-B`, commit: () => {} }
    : prepareLocalStrategyCopy(
        input.b.strategyId ?? '',
        input.b.strategyVersion ?? 0,
        `bidirectional-${input.requestId}-B`,
      )
  const pair = preparePipelinePair(
    {
      name: canonical.name + ' A→B',
      topologyId: input.topologyId ?? '00000000-0000-4000-8000-000000000001',
      sourceEndpointId: a.id,
      sourceNodeId: aNode.id,
      targetEndpointId: b.id,
      targetNodeId: bNode.id,
      strategyId: first.id,
      rules: input.a.rules
        ? {
            ...input.a.rules,
            source: {
              ...input.a.rules.source,
              origin_policy: { mode: 'ORIGIN_POLICY_MODE_LOCAL_ONLY' },
            },
          }
        : undefined,
      strategyVersion: 1,
      initMode: 'full_and_incremental',
    },
    {
      name: canonical.name + ' B→A',
      topologyId: input.topologyId ?? '00000000-0000-4000-8000-000000000001',
      sourceEndpointId: b.id,
      sourceNodeId: bNode.id,
      targetEndpointId: a.id,
      targetNodeId: aNode.id,
      strategyId: second.id,
      rules: input.b.rules
        ? {
            ...input.b.rules,
            source: {
              ...input.b.rules.source,
              origin_policy: { mode: 'ORIGIN_POLICY_MODE_LOCAL_ONLY' },
            },
          }
        : undefined,
      strategyVersion: 1,
      initMode: 'incremental_only_latest',
    },
  )
  // All validation is complete. No asynchronous work splits these writes.
  first.commit()
  second.commit()
  const result = pair.commit()
  requests.set(input.requestId, { userId: me.user.id, digest, result: structuredClone(result) })
  return result
}
