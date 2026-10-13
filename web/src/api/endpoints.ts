import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import type { Paginated } from './types'
import { isApiMockEnabled } from '@/mocks/config'
import * as endpointsMock from '@/mocks/endpointsMock'

/**
 * 探测结果：连通性测试 + 参数探测合并为一次「测试连接」（MGR-DS-02 把两者归为
 * 一个界面动作）。字段命名对齐 表格；`reachable=false` 时其余字段可能
 * 为空，`error` 携带连接失败原因。
 */
export type ProbeResult = generated.GeneratedEndpointProbeResult

export interface EndpointSummary {
  id: string
  name: string
  centerId: string
  host: string
  port: number
  username: string
  tls: Record<string, unknown> | null
  params: Record<string, unknown> | null
  version: string
  serverUuid: string | null

  lowerCaseTableNames: string
  probeNodeId: string | null
  lastProbeAt: string | null
  lastProbeResult: ProbeResult | null
  createdAt: string
}

export interface EndpointListQuery {
  page: number
  pageSize: number
  centerId?: string
  keyword?: string
}

export type EndpointInput = generated.GeneratedEndpointInput

export type EndpointUpdateInput = Partial<Omit<EndpointInput, 'centerId'>>

export type CatalogEntry = generated.GeneratedCatalogEntry

export function listEndpoints(query: EndpointListQuery): Promise<Paginated<EndpointSummary>> {
  if (isApiMockEnabled()) {
    return endpointsMock.listEndpoints(query)
  }
  return generated.listEndpoints(query)
}

export function getEndpoint(id: string): Promise<EndpointSummary> {
  if (isApiMockEnabled()) {
    return endpointsMock.getEndpoint(id)
  }
  return generated.getEndpoint(id)
}

export function createEndpoint(input: EndpointInput): Promise<EndpointSummary> {
  if (isApiMockEnabled()) {
    return endpointsMock.createEndpoint(input)
  }
  return generated.createEndpoint(input)
}

/* Endpoint update responses report source and target states independently. */
export type AffectedPipeline = generated.GeneratedAffectedPipeline

export type EndpointUpdateResult = EndpointSummary & {
  affectedPipelines: AffectedPipeline[]
  affectedUnknown?: boolean
}

export function updateEndpoint(
  id: string,
  input: EndpointUpdateInput,
): Promise<EndpointUpdateResult> {
  if (isApiMockEnabled()) {
    return endpointsMock.updateEndpoint(id, input)
  }
  return generated.updateEndpoint(id, input)
}

/** 被任一未删除的 pipeline 引用时后端拒绝删除（MGR-DS-05），错误信息原样展示。 */
export function deleteEndpoint(id: string): Promise<void> {
  if (isApiMockEnabled()) {
    return endpointsMock.deleteEndpoint(id)
  }
  return generated.deleteEndpoint(id).then(() => undefined)
}

/**
 * 派发探测任务（连通性 + 参数，MGR-DS-02）。`nodeId` 必须是与该数据端点同中心的
 * 在线节点——前端负责在候选下拉里只列出同中心在线节点，但这里仍然把后端的
 * 拒绝原因（跨中心 / 节点离线等）原样抛给调用方展示，不吞掉、不在前端伪造
 * 「已探测」的假象。返回 taskId，调用方轮询 getEndpoint(id) 等待同一任务结果。
 */
export function probeEndpoint(id: string, nodeId: string): Promise<{ taskId: string }> {
  if (isApiMockEnabled()) {
    return endpointsMock.probeEndpoint(id, nodeId)
  }
  return generated.probeEndpoint(id, { nodeId })
}

/** 浏览库表元数据（MGR-DS-04，P1），供策略配置与全量规划选择库表。 */
export function getEndpointCatalog(id: string): Promise<CatalogEntry[]> {
  if (isApiMockEnabled()) {
    return endpointsMock.getEndpointCatalog(id)
  }
  return generated.getEndpointCatalog(id)
}

export const endpointsListQueryKey = (query: EndpointListQuery) =>
  ['endpoints', 'list', query] as const
export const endpointDetailQueryKey = (id: string) => ['endpoints', 'detail', id] as const
export const endpointCatalogQueryKey = (id: string) => ['endpoints', 'catalog', id] as const

export function useEndpointsQuery(query: EndpointListQuery, enabled = true) {
  return useQuery({
    queryKey: endpointsListQueryKey(query),
    queryFn: () => listEndpoints(query),
    enabled,
  })
}

/**
 * `pollIntervalMs` 用于探测进行中的轮询展示：传入一个毫秒数时，query 会按该
 * 间隔重新拉取，调用方（探测弹窗）负责在探测结束（成功 / 超时）后把它改回
 * `false` 停止轮询——TanStack Query 的 refetchInterval 本身不知道「探测已完成」
 * 这件业务语义。
 */
export function useEndpointQuery(id: string | undefined, pollIntervalMs: number | false = false) {
  return useQuery({
    queryKey: endpointDetailQueryKey(id ?? ''),
    queryFn: () => getEndpoint(id ?? ''),
    enabled: Boolean(id),
    refetchInterval: pollIntervalMs,
  })
}

export function useEndpointCatalogQuery(id: string | undefined, enabled = true) {
  return useQuery({
    queryKey: endpointCatalogQueryKey(id ?? ''),
    queryFn: () => getEndpointCatalog(id ?? ''),
    enabled: Boolean(id) && enabled,
  })
}

function useInvalidateEndpoints() {
  const queryClient = useQueryClient()
  return (id?: string) => {
    void queryClient.invalidateQueries({ queryKey: ['endpoints', 'list'] })
    if (id) {
      void queryClient.invalidateQueries({ queryKey: endpointDetailQueryKey(id) })
    }
  }
}

export function useCreateEndpointMutation() {
  const invalidate = useInvalidateEndpoints()
  return useMutation({
    mutationFn: createEndpoint,
    meta: { suppressGlobalError: true },
    onSuccess: () => invalidate(),
  })
}

export function useUpdateEndpointMutation() {
  const invalidate = useInvalidateEndpoints()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: EndpointUpdateInput }) =>
      updateEndpoint(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, variables) => invalidate(variables.id),
  })
}

export function useDeleteEndpointMutation() {
  const invalidate = useInvalidateEndpoints()
  return useMutation({
    mutationFn: (id: string) => deleteEndpoint(id),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, id) => invalidate(id),
  })
}

export function useProbeEndpointMutation() {
  return useMutation({
    mutationFn: ({ id, nodeId }: { id: string; nodeId: string }) => probeEndpoint(id, nodeId),
    meta: { suppressGlobalError: true },
  })
}
