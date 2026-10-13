import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import type { Paginated } from './types'
import { isApiMockEnabled } from '@/mocks/config'
import * as pipelinesMock from '@/mocks/pipelinesMock'
import { authorizePipelineResume } from './pipelineResume'

/**
 * pipeline 的初始化方式（对应 model.InitMode 字符串常量，HTTP 层原样透传）。
 * `full_and_incremental` 是后端默认值，不代表前端应该预选它——创建表单未
 * 落地前不引入这个假设。
 */
export type PipelineInitMode =
  | 'full_and_incremental'
  | 'incremental_only_latest'
  | 'incremental_only_gtid_set'
  | 'incremental_only_binlog_pos'
  | 'incremental_only_timestamp'
  | 'full_only'
  | 'schema_only'

/** 用户可设置的期望状态（只有这三种，不包含 Draft/Error 等实际态）。 */
export type PipelineDesiredState = 'running' | 'paused' | 'stopped'

/* 同一个节点可能同时兼任某条 pipeline 的两侧（后端 `dispatchCommand` 的去重
说明里写明了这种情形），此时它仍然要以两个角色各上报一次——所以"侧"不等于
"节点"，不能用 nodeId 去代替它。 */
export type PipelineSide = 'source' | 'target'

/**
 * pipeline 状态机的实际状态。`unknown` 对应节点离线时「实际状态
 * 记为未知，不改写」，不是「没有意见」，界面上不应该和其余状态同等对待
 * （同 web/src/api/nodes.ts `NodeActualLogLevel` 的 `unknown` 处理原则）。
 */
export type PipelineActualState =
  | 'draft'
  | 'prechecking'
  | 'deploying'
  | 'migrating'
  | 'snapshotting'
  | 'catching_up'
  | 'running'
  | 'paused'
  | 'error'
  | 'stopped'
  | 'unknown'

/**
 * pipelineView：list 与 get 共用同一份形状，不拆 Summary/Detail（后端注释：
 * 现在没有详情页需求撑一个更大的字段集，拆分是凭空造两份要同步维护的东西）。
 * 不含 runId/snapshotUuid/schemaUuid/binlogServerId/cleanup_*：这些是内部
 * 机制字段，pipelineView 明确不暴露。
 */
export interface PipelineSummary {
  id: string
  name: string
  topologyId: string
  sourceEndpointId: string
  sourceNodeId: string
  targetEndpointId: string
  targetNodeId: string
  strategyId: string
  strategyVersion: number
  initMode: PipelineInitMode
  /** 结构随 initMode 变化，见文件头注释；未设置时后端不返回该字段。 */
  startPosition?: unknown
  /** 已确认的起点修订号；尚未修改时为 0。 */
  positionRevision: number
  desiredState: PipelineDesiredState

  displayState: PipelineActualState
  /** 与 `displayState` 取自**同一侧**，不会出现"状态来自源侧、错误来自目标侧"
   * 的错配。仅在该侧有错误时返回。 */
  displayLastError?: string
  /** `displayState`/`displayLastError` 取自哪一侧。**不要自己比两侧反推**——
   * 那等于把后端的聚合优先级表在前端重实现一遍，后端调表时这里会静默失真。 */
  displayStateSide: PipelineSide

  /** 源端实际状态。**恒有**（后端该字段无 omitempty，列本身 not null），
   * 所以这里是必填而不是可选：写成可选会逼每个消费点处理一个永不出现的
   * 分支，而处理它的唯一办法是在前端编一个默认值。 */
  sourceActualState: PipelineActualState
  /** 源端最近错误；仅在有错误时返回。
   *
   * **该侧 `unknown`（离线）时，这里是"离线前"的那条真实错误（也可能没有），
   * 不是离线的说明**——后端标离线时刻意不覆盖它，为的就是保住离线前的诊断。
   * 所以渲染时必须按该侧状态选标签，不能一律叫"最近错误"（那会把一条陈年
   * 错误说成当前状态），配合 `sourceNodeLastHeartbeatAt` 一起看。 */
  sourceLastError?: string
  /** 目标端实际状态。恒有，理由同 `sourceActualState`。 */
  targetActualState: PipelineActualState
  /** 目标端最近错误；语义与 `sourceLastError` 完全对称。 */
  targetLastError?: string

  /* 源端节点最后一次心跳的时间；null 表示从未心跳过或查不到该节点。

给的是节点心跳时间而不是"这一侧最后一次上报 pipeline 状态的时间"
——后者今天没有这份数据。而离线本来就是按节点判的（心跳超时 → 把该侧标
`unknown`），所以它配合 `sourceActualState === 'unknown'` 一起看，回答的
正是"哪一侧、从什么时候起失联"。 */
  sourceNodeLastHeartbeatAt: string | null
  /** 目标端节点最后一次心跳的时间；语义与 `sourceNodeLastHeartbeatAt` 对称。 */
  targetNodeLastHeartbeatAt: string | null

  recoveryExhausted: boolean

  configVersion: number
  createdAt: string
  updatedAt: string
}

export interface PipelineListQuery {
  page: number
  pageSize: number
  topologyId?: string
  keyword?: string
}

/** POST /pipelines 请求体，对应 pipelineCreateRequest。 */
export type PipelineCreateInput = generated.GeneratedPipelineCreateInput

export interface PipelineUpdateInput {
  name?: string
  strategyId?: string
  strategyVersion?: number
  initMode?: PipelineInitMode
}

/**
 * `PipelineUpdateInput` 的键集，供契约测试与后端 struct 比对。
 *
 * 类型刻意是 `Record<keyof PipelineUpdateInput, true>` 而不是一个字符串数组：
 * 数组只能保证"里面的每个都是合法键"（⊆），保证不了"每个合法键都在里面"（⊇），
 * 而漏掉一个正是要防的方向。
 */
const PIPELINE_UPDATE_FIELD_SET: Record<keyof PipelineUpdateInput, true> = {
  name: true,
  strategyId: true,
  strategyVersion: true,
  initMode: true,
}

export const PIPELINE_UPDATE_FIELDS: readonly string[] = Object.keys(
  PIPELINE_UPDATE_FIELD_SET,
).sort()

export function listPipelines(query: PipelineListQuery): Promise<Paginated<PipelineSummary>> {
  if (isApiMockEnabled()) {
    return pipelinesMock.listPipelines(query)
  }
  return generated.listPipelines(query)
}

export function getPipeline(id: string): Promise<PipelineSummary> {
  if (isApiMockEnabled()) {
    return pipelinesMock.getPipeline(id)
  }
  return generated.getPipeline(id)
}

export function createPipeline(input: PipelineCreateInput): Promise<PipelineSummary> {
  if (isApiMockEnabled()) {
    return pipelinesMock.createPipeline(input)
  }
  return generated.createPipeline(input)
}

export function updatePipeline(
  id: string,
  input: PipelineUpdateInput,
): Promise<PipelineSummary> {
  if (isApiMockEnabled()) {
    return pipelinesMock.updatePipeline(id, input)
  }
  return generated.updatePipeline(id, input)
}

/** 软删除（MGR-PL-13 两步删除的第一步）；后续资源回收由 reclaim 线异步完成。 */
export function deletePipeline(id: string): Promise<void> {
  if (isApiMockEnabled()) {
    return pipelinesMock.deletePipeline(id)
  }
  return generated.deletePipeline(id).then(() => undefined)
}

export function startPipeline(id: string): Promise<PipelineSummary> {
  if (isApiMockEnabled()) {
    return pipelinesMock.startPipeline(id)
  }
  return generated.startPipeline(id)
}

export function pausePipeline(id: string): Promise<PipelineSummary> {
  if (isApiMockEnabled()) {
    return pipelinesMock.pausePipeline(id)
  }
  return generated.pausePipeline(id)
}

/**
 * 从暂停恢复运行。与 startPipeline 目标状态相同（都是 desiredState=running）
 * 但保留两个独立端点/函数——「从暂停恢复」和「首次启动」在事故复盘时不是
 * 一回事，后端审计要能分开筛，前端不应该把两个按钮合并成同一次调用。
 */
export function resumePipeline(id: string): Promise<PipelineSummary> {
  if (isApiMockEnabled()) {
    return pipelinesMock.resumePipeline(id)
  }
  return generated.resumePipeline(id)
}

export function stopPipeline(id: string): Promise<PipelineSummary> {
  if (isApiMockEnabled()) {
    return pipelinesMock.stopPipeline(id)
  }
  return generated.stopPipeline(id)
}

/* 不能用 start/resume 代替：那两个改的是 desiredState，而走到
`recoveryExhausted` 的 pipeline 其 desiredState 通常本来就已经是 running
——再点一次"启动"什么都不会发生。本端点无条件重置恢复状态、不改
desiredState，两者管的不是同一件事。

无请求体；返回体就是 pipelineView（其中 `recoveryExhausted` 已变 false）。 */
export function retryPipeline(id: string): Promise<PipelineSummary> {
  if (isApiMockEnabled()) {
    return pipelinesMock.retryPipeline(id)
  }
  return generated.retryPipeline(id)
}

/* 配置下发记录归属的一侧；与 pipeline 自身 source/target 节点一一对应
（节点创建后不可变，见 PipelineUpdateInput 注释），前端可以直接用 side
映射回已经解析好的 sourceNodeName/targetNodeName，不需要为 nodeId 单独
发起查询。

就是 `PipelineSide`，保留这个名字是因为它已经在契约里用着了；两者指的是
同一个"侧"，不要再造第三个同形状的类型。 */
export type PipelineReleaseSide = PipelineSide

/** 对应 internal/manager/model.ReleaseStatus：pending 已下发、等待节点回执
 * （不是失败也不是成功，长期停留在 pending 本身就是诊断信号）；applied 节点
 * 应用成功；failed 节点尝试应用但出错；rejected 节点因版本倒退等原因主动
 * 拒绝（是安全机制生效，不等同于 failed 那种"出错"）。四个值
 * 必须原样展示，不得合并——同源约束见 desiredState/actualState 分离展示
 * 的先例。 */
export type PipelineReleaseStatus = 'pending' | 'applied' | 'failed' | 'rejected'

export interface PipelineRelease {
  id: string
  configVersion: number
  side: PipelineReleaseSide
  nodeId: string
  status: PipelineReleaseStatus
  /** 后端已对已知的两种敏感信息形状（DSN 风格的 user:pass@ 前缀）脱敏并截断到
   * 2000 rune，但明确不是通用敏感信息扫描器——裸令牌在字符形状上无法与普通
   * 字符串可靠区分，硬猜正则只会误伤诊断信息。前端仍必须按不可信文本对待：
   * 只用文本节点渲染，不得拼进 dangerouslySetInnerHTML 或其它 HTML 注入路径。
   * 可能为空而省略。 */
  message?: string
  createdAt: string
  /** 非 applied 状态时为 null 或省略。 */
  appliedAt?: string | null
}

export interface PipelineReleaseListQuery {
  page: number
  pageSize: number
}

export function listPipelineReleases(
  id: string,
  query: PipelineReleaseListQuery,
): Promise<Paginated<PipelineRelease>> {
  if (isApiMockEnabled()) {
    return pipelinesMock.listPipelineReleases(id, query)
  }
  return generated.listPipelineReleases(id, query)
}

export const pipelinesListQueryKey = (query: PipelineListQuery) =>
  ['pipelines', 'list', query] as const
export const pipelineDetailQueryKey = (id: string) => ['pipelines', 'detail', id] as const
export const pipelineReleasesQueryKey = (id: string, query: PipelineReleaseListQuery) =>
  ['pipelines', 'releases', id, query] as const

export function usePipelinesQuery(query: PipelineListQuery, enabled = true) {
  return useQuery({
    queryKey: pipelinesListQueryKey(query),
    queryFn: () => listPipelines(query),
    placeholderData: keepPreviousData,
    enabled,
  })
}

export function usePipelineQuery(id: string | undefined) {
  return useQuery({
    queryKey: pipelineDetailQueryKey(id ?? ''),
    queryFn: () => getPipeline(id ?? ''),
    enabled: Boolean(id),
    refetchInterval: 5000,
  })
}

export function usePipelineReleasesQuery(
  id: string | undefined,
  query: PipelineReleaseListQuery,
  enabled = true,
) {
  return useQuery({
    queryKey: pipelineReleasesQueryKey(id ?? '', query),
    queryFn: () => listPipelineReleases(id ?? '', query),
    placeholderData: keepPreviousData,
    enabled: Boolean(id) && enabled,
  })
}

function useInvalidatePipelines() {
  const queryClient = useQueryClient()
  return (id?: string) => {
    void queryClient.invalidateQueries({ queryKey: ['pipelines', 'risk'] })
    void queryClient.invalidateQueries({ queryKey: ['pipelines', 'rules'] })
    void queryClient.invalidateQueries({ queryKey: ['pipelines', 'list'] })
    void queryClient.invalidateQueries({ queryKey: ['pipelines', 'cleanup'] })
    if (id) {
      void queryClient.invalidateQueries({ queryKey: pipelineDetailQueryKey(id) })
      void queryClient.invalidateQueries({ queryKey: ['pipelines', 'precheck', id] })
    }
  }
}

// 以下 mutation hook 一律 suppressGlobalError：调用方自行在 onError 里展示
// 提示，避免和 queryClient 全局 mutationCache 的提示重复（同 web/src/api/nodes.ts 先例）。

export function useCreatePipelineMutation() {
  const invalidate = useInvalidatePipelines()
  return useMutation({
    mutationFn: (input: PipelineCreateInput) => createPipeline(input),
    meta: { suppressGlobalError: true },
    onSuccess: () => invalidate(),
  })
}

export function useUpdatePipelineMutation() {
  const invalidate = useInvalidatePipelines()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: PipelineUpdateInput }) =>
      updatePipeline(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, variables) => invalidate(variables.id),
  })
}

export function useDeletePipelineMutation() {
  const invalidate = useInvalidatePipelines()
  return useMutation({
    mutationFn: (id: string) => deletePipeline(id),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, id) => invalidate(id),
  })
}

function usePipelineLifecycleMutation(fn: (id: string) => Promise<PipelineSummary>) {
  const invalidate = useInvalidatePipelines()
  const client = useQueryClient()
  return useMutation({
    mutationFn: fn,
    meta: { suppressGlobalError: true },
    onSuccess: (_data, id) => invalidate(id),
    onSettled: (_data, _error, id) => {
      void client.invalidateQueries({ queryKey: ['pipelines', 'precheck', id] })
    },
  })
}

export function useStartPipelineMutation() {
  return usePipelineLifecycleMutation(startPipeline)
}

export function usePausePipelineMutation() {
  return usePipelineLifecycleMutation(pausePipeline)
}

export function useResumePipelineMutation() {
  return usePipelineLifecycleMutation(async (id) => {
    if (!isApiMockEnabled()) await authorizePipelineResume(id)
    return resumePipeline(id)
  })
}

export function useStopPipelineMutation() {
  return usePipelineLifecycleMutation(stopPipeline)
}

export function useRetryPipelineMutation() {
  return usePipelineLifecycleMutation(retryPipeline)
}
