// 注册码安全约束：注册码明文只在生成的那一次由后端返回
// （`RegisterCodeCreated.code`），前端只允许把它短暂放在组件内存态（React state）
// 用于一次性展示 + 复制到剪贴板，绝不写 localStorage、不拼进 URL 查询串、
// 不经过 console.*／日志。`RegisterCode`（列表 / 详情用的类型）本身不含明文字段，
// 从类型层面防止意外回显。
import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import type { Paginated } from './types'
import { isApiMockEnabled } from '@/mocks/config'
import * as nodesMock from '@/mocks/nodesMock'

export type NodeStatus = generated.GeneratedNodeStatus
export type LogLevel = generated.GeneratedLogLevel
export type PipelineRole = 'source' | 'target'

export type NodeRuntimeInfo = generated.GeneratedNodeRuntimeInfo

export type NodeSummary = generated.GeneratedNodeSummary

export type NodePipelineRef = generated.GeneratedNodePipelineRef

export type NodeActualLogLevel = generated.GeneratedNodeActualLogLevel

export type NodeDetail = generated.GeneratedNodeDetail

export interface NodeListQuery {
  page: number
  pageSize: number
  centerId?: string
  status?: NodeStatus
  keyword?: string
}

export type NodeUpdateInput = generated.GeneratedNodeUpdateInput

export type NodeLogLevelInput = generated.GeneratedNodeLogLevelInput

/**
 * `POST /nodes/{id}/log-level` 的响应体：回显提交后的期望值 + 当前的生效
 * 状态（恒为 `'unknown'`，见 `NodeDetail` 上方的注释）。用这份响应直接回填
 * 详情页缓存，比提交后再发一次 `GET` 更准，也更能在交互上坐实"你刚提交的
 * 是期望值，不是已确认生效的值"这件事。
 */
export type NodeLogLevelResult = generated.GeneratedNodeLogLevelResult

/** 注册码列表 / 详情项：不含明文，只在生成响应中出现一次（见 RegisterCodeCreated）。 */
export interface RegisterCode {
  id: string
  centerId: string
  presetName: string | null
  presetLabels: string[]
  expiresAt: string | null
  usedCount: number
  createdBy: string
  createdAt: string
  revokedAt: string | null
}

export type RegisterCodeCreateInput = generated.GeneratedRegisterCodeCreateInput

/**
 * 生成注册码的响应：`code` 明文只在这一次返回，库中只存哈希（MGR-NODE-01）。
 * 调用方必须只在内存态短暂持有用于一次性展示，关闭弹窗后即应丢弃引用。
 */
export interface RegisterCodeCreated extends RegisterCode {
  code: string
}

export function listNodes(query: NodeListQuery): Promise<Paginated<NodeSummary>> {
  if (isApiMockEnabled()) {
    return nodesMock.listNodes(query)
  }
  return generated.listNodes(query)
}

export function getNode(id: string): Promise<NodeDetail> {
  if (isApiMockEnabled()) {
    return nodesMock.getNode(id)
  }
  return generated.getNode(id)
}

export function updateNode(id: string, input: NodeUpdateInput): Promise<NodeSummary> {
  if (isApiMockEnabled()) {
    return nodesMock.updateNode(id, input)
  }
  return generated.updateNode(id, input)
}

export function revokeNodeToken(id: string): Promise<void> {
  if (isApiMockEnabled()) {
    return nodesMock.revokeNodeToken(id)
  }
  return generated.revokeNodeToken(id).then(() => undefined)
}

/* CleanupState done means no pending resources. Use deletedAt to identify deleted pipelines. */
export type NodeCleanupState = generated.GeneratedNodeCleanupState

/* `DELETE /nodes/{id}` 成功时的响应体（HTTP 202，不是 200）。 */
export interface NodeDeregisterResult {
  id: string
  cleanupState: NodeCleanupState
}

/**
 * 409 `node_has_dependents` 时 `error.details` 里的单条阻断项，对齐后端
 * `nodeDeregisterBlockerView`（`internal/manager/http/nodes.go` 里的同名
 * 结构体）。
 *
 *  明确要求 body 是「阻断者完整清单，不是计数或布尔」，所以界面
 * 必须把它逐条渲染出来——只弹一句「不能注销」等于把后端特意查出来的清单扔掉。
 */
export interface NodeDeregisterBlocker {
  pipelineId: string
  pipelineName: string
  cleanupState: NodeCleanupState
  /** 未删除的 pipeline 为 null（后端该字段无 omitempty，恒返回）。 */
  deletedAt: string | null
}

const CLEANUP_STATES: readonly NodeCleanupState[] = ['pending', 'in_progress', 'done', 'failed']

function isCleanupState(value: unknown): value is NodeCleanupState {
  return typeof value === 'string' && (CLEANUP_STATES as readonly string[]).includes(value)
}

function isNodeDeregisterBlocker(value: unknown): value is NodeDeregisterBlocker {
  if (typeof value !== 'object' || value === null) {
    return false
  }
  const item = value as Record<string, unknown>
  return (
    typeof item.pipelineId === 'string' &&
    typeof item.pipelineName === 'string' &&
    isCleanupState(item.cleanupState) &&
    (item.deletedAt === null || typeof item.deletedAt === 'string')
  )
}

/* Reject the entire blocker list if any item is malformed; a partial list would hide blockers. */
export function parseNodeDeregisterBlockers(details: unknown): NodeDeregisterBlocker[] {
  if (!Array.isArray(details) || details.length === 0) {
    return []
  }
  return details.every(isNodeDeregisterBlocker) ? details : []
}

/* 注销节点（MGR-NODE-06a，两步删除的第一步）：吊销 token → 软删除，
资源回收异步进行。被 pipeline 引用时后端返回 409 `node_has_dependents`，
完整阻断清单在 `ApiError.details` 里，用 `parseNodeDeregisterBlockers` 取。 */
export function deregisterNode(id: string): Promise<NodeDeregisterResult> {
  if (isApiMockEnabled()) {
    return nodesMock.deregisterNode(id)
  }
  return generated.deregisterNode(id)
}

export function setNodeLogLevel(id: string, input: NodeLogLevelInput): Promise<NodeLogLevelResult> {
  if (isApiMockEnabled()) {
    return nodesMock.setNodeLogLevel(id, input)
  }
  return generated.setNodeLogLevel(id, input)
}

export function listRegisterCodes(centerId?: string): Promise<RegisterCode[]> {
  if (isApiMockEnabled()) {
    return nodesMock.listRegisterCodes(centerId)
  }
  return generated.listRegisterCodes(centerId ? { centerId } : undefined)
}

export function createRegisterCode(input: RegisterCodeCreateInput): Promise<RegisterCodeCreated> {
  if (isApiMockEnabled()) {
    return nodesMock.createRegisterCode(input)
  }
  return generated.createRegisterCode(input)
}

export function revokeRegisterCode(id: string): Promise<void> {
  if (isApiMockEnabled()) {
    return nodesMock.revokeRegisterCode(id)
  }
  return generated.revokeRegisterCode(id).then(() => undefined)
}

export const nodesListQueryKey = (query: NodeListQuery) => ['nodes', 'list', query] as const
export const nodeDetailQueryKey = (id: string) => ['nodes', 'detail', id] as const
export const registerCodesQueryKey = (centerId?: string) =>
  ['nodes', 'register-codes', centerId ?? null] as const

export function useNodesQuery(query: NodeListQuery, enabled = true) {
  return useQuery({
    queryKey: nodesListQueryKey(query),
    queryFn: () => listNodes(query),
    placeholderData: keepPreviousData,
    enabled,
  })
}

export function useNodeQuery(id: string | undefined) {
  return useQuery({
    queryKey: nodeDetailQueryKey(id ?? ''),
    queryFn: () => getNode(id ?? ''),
    enabled: Boolean(id),
  })
}

function useInvalidateNodes() {
  const queryClient = useQueryClient()
  return (id?: string) => {
    void queryClient.invalidateQueries({ queryKey: ['nodes', 'list'] })
    if (id) {
      void queryClient.invalidateQueries({ queryKey: nodeDetailQueryKey(id) })
    }
  }
}

// 以下 mutation hook 一律 suppressGlobalError：调用方（页面 / 弹窗组件）都会在
// mutate() 的 onError 里自行展示提示（toast 或表单内联错误），不需要 queryClient
// 的全局 mutationCache 再重复弹一次（否则同一个错误会被展示两次）。

export function useUpdateNodeMutation() {
  const invalidate = useInvalidateNodes()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: NodeUpdateInput }) => updateNode(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, variables) => invalidate(variables.id),
  })
}

export function useRevokeNodeTokenMutation() {
  const invalidate = useInvalidateNodes()
  return useMutation({
    mutationFn: (id: string) => revokeNodeToken(id),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, id) => invalidate(id),
  })
}

export function useDeregisterNodeMutation() {
  const queryClient = useQueryClient()
  const invalidate = useInvalidateNodes()
  return useMutation({
    mutationFn: (id: string) => deregisterNode(id),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, id) => {
      // 节点已软删除，GET /nodes/{id} 之后会 404——把详情缓存直接移除，
      // 不留一份"还能看见但其实已经没了"的旧数据（列表页走 invalidate
      // 重新查即可，详情缓存没有对应的"重新查"路径，它的消费方正在被
      // 导航走）。
      queryClient.removeQueries({ queryKey: nodeDetailQueryKey(id) })
      invalidate()
    },
  })
}

export function useSetNodeLogLevelMutation() {
  const queryClient = useQueryClient()
  const invalidate = useInvalidateNodes()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: NodeLogLevelInput }) =>
      setNodeLogLevel(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: (data, variables) => {
      // 用响应直接回填详情页缓存的四个字段，不等下一次 GET——见
      // NodeLogLevelResult 的注释：这比"提交后再重新查一次"更准，也更能在
      // 交互上坐实"提交的是期望值，生效状态仍然未知"这件事。其余字段
      // （runtime、pipelines 等）不受这次提交影响，原样保留。
      queryClient.setQueryData<NodeDetail>(nodeDetailQueryKey(variables.id), (old) =>
        old ? { ...old, ...data } : old,
      )
      invalidate(variables.id)
    },
  })
}

/** `enabled` 默认 true；抽屉未打开时调用方应传 false，避免后台无谓查询。 */
export function useRegisterCodesQuery(centerId?: string, enabled = true) {
  return useQuery({
    queryKey: registerCodesQueryKey(centerId),
    queryFn: () => listRegisterCodes(centerId),
    enabled,
  })
}

export function useCreateRegisterCodeMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: RegisterCodeCreateInput) => createRegisterCode(input),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['nodes', 'register-codes'] })
    },
  })
}

export function useRevokeRegisterCodeMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => revokeRegisterCode(id),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['nodes', 'register-codes'] })
    },
  })
}
