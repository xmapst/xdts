// 表单使用 lowerCamelCase；strategyRulesWire 负责转换为服务端 snake_case 协议。
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ApiError } from './client'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import * as strategiesMock from '@/mocks/strategiesMock'
import { toWireRules } from './strategyRulesWire'

/* Stored strategy rules use protojson with snake_case keys and symbolic enum names; config digests use a separate encoding. */
export type FilterAction =
  | 'FILTER_ACTION_UNSPECIFIED'
  | 'FILTER_ACTION_INCLUDE'
  | 'FILTER_ACTION_EXCLUDE'
export type MatchType =
  | 'MATCH_TYPE_UNSPECIFIED'
  | 'MATCH_TYPE_EXACT'
  | 'MATCH_TYPE_GLOB'
  | 'MATCH_TYPE_REGEX'
export type OriginPolicyMode =
  | 'ORIGIN_POLICY_MODE_UNSPECIFIED'
  | 'ORIGIN_POLICY_MODE_LOCAL_ONLY'
  | 'ORIGIN_POLICY_MODE_INCLUDE_ORIGINS'
  | 'ORIGIN_POLICY_MODE_EXCLUDE_ORIGINS'
  | 'ORIGIN_POLICY_MODE_ALL'

export type DdlPolicyMode = 'sync' | 'ignore' | 'from_origins'
export type ConflictPolicyMode = 'source_wins' | 'last_write_wins' | 'stop'

export interface LWWTableRule {
  schema: string
  table: string
  timestampColumn: string
}

export interface CleanseTransformRule {
  schema: string
  table: string
  column: string
  kind:
    | 'CLEANSE_TRANSFORM_KIND_MASK'
    | 'CLEANSE_TRANSFORM_KIND_CONST_FILL'
    | 'CLEANSE_TRANSFORM_KIND_TYPE_CONVERT'
    | 'CLEANSE_TRANSFORM_KIND_LUA'
    | 'CLEANSE_TRANSFORM_KIND_PLUGIN'
  params: Record<string, string>
  execution_side?:
    | 'TRANSFORM_EXECUTION_SIDE_UNSPECIFIED'
    | 'TRANSFORM_EXECUTION_SIDE_SOURCE'
    | 'TRANSFORM_EXECUTION_SIDE_TARGET'
}

export interface MappingRules {
  schemas?: Array<{ source_schema: string; target_schema: string }>
  tables?: Array<{ source_schema: string; source_table: string; target_table: string }>
  columns?: Array<{
    source_schema: string
    source_table: string
    source_column: string
    target_column: string
  }>
}

/* table_match and table_pattern must both be set or both be empty for a database-wide rule. */
export interface ObjectFilterRule {
  action: FilterAction
  schema_match: MatchType
  schema_pattern: string
  table_match: MatchType
  table_pattern: string
}

/** INSERT / UPDATE / DELETE / DDL 独立开关，仅作用于增量（源端，P0）。对齐
 * proto `OperationFilter` 消息，字段名本来就是 lowerCamelCase 之外的普通
 * 小写单词，snake_case/camelCase 在这四个字段上没有区别。 */
export interface OperationFilter {
  insert: boolean
  update: boolean
  delete: boolean
  ddl: boolean
}

/** 回环识别的来源策略（源端，P0）。对齐 proto `OriginPolicy` 消息：
 * `centers` 只在 `mode` 为 `ORIGIN_POLICY_MODE_INCLUDE_ORIGINS`/
 * `_EXCLUDE_ORIGINS` 时生效且不能为空，`_LOCAL_ONLY`/`_ALL` 时必须留空
 * （`internal/rule/compile.go`
 * `validateOriginPolicy` 双向强制，`web/src/features/strategies/validateRules.ts` 镜像同一约束）；
 * `max_hops` 为 0 表示不限制跳数——这不是"未填"的哨兵，是一个有意义的取值
 * （`internal/rule/types.go` OriginPolicy 字段注释、`internal/rule/plan.go` AllowOrigin 的
 * `MaxHops > 0` 判断都以此为准），不要在表单/校验里再当成"必须 >= 1"处理。 */
export interface OriginPolicy {
  mode: OriginPolicyMode
  centers: string[]
  max_hops: number
  drop_unknown_origin?: boolean
}

/* The form uses a flat projection. Unknown advanced fields are preserved as JSON through editing. */
export interface StrategyRules {
  object_filters: ObjectFilterRule[]
  /** 操作过滤（源端，P0，仅作用于增量）。 */
  operation_filter: OperationFilter
  /** 来源策略（源端，P0，回环识别）。 */
  origin_policy: OriginPolicy
  /** DDL 策略：同步 / 忽略 / 仅同步来自指定中心；危险 DDL 单独开关（目标端，P0）。 */
  ddlPolicy: {
    mode: DdlPolicyMode
    fromOrigins: string[]
    blockDangerous: boolean
    definerFallback?: string
  }
  /** 冲突策略：源覆盖（默认）/ 时间戳列 LWW / 停止（目标端，P0）。 */
  conflictPolicy: {
    mode: ConflictPolicyMode
    lwwTables?: LWWTableRule[]
    maskRules?: { schema: string; table: string; columns: string[] }[]
  }
  /** 错误策略：可重试错误重试次数与退避；不可重试默认停止（目标端，P0）。 */
  errorPolicy: {
    maxRetries: number
    backoffMs: number
    skipAndLogOnNonRetryable: boolean
  }
  /** 全量参数：读取并发、分块行数、单事务上限、写入并发、限流、是否从只读从库读（两端，P0）。 */
  snapshotParams: {
    readConcurrency: number
    chunkRows: number
    maxRowsPerTxn: number
    maxBytesPerTxn: number
    writeConcurrency: number
    rateLimit: number | null
    readFromReplica: boolean
    replicaEndpointId?: string
  }
  /** 写入参数：批大小、并行回放度、同事务内同表多行合并（目标端，P0 / P1）。 */
  writeParams: {
    batchSize: number
    parallelism: number
    mergeRowsInSameTable: boolean
    orderedCommit?: boolean
  }
  /**
   * 尚无专门表单区块的扩展配置，在 JSON 编辑器里整体编辑；保存仍受后端校验。
   */
  advanced?: Record<string, unknown>
  /** 精确源库表列白名单/黑名单，可在 JSON 编辑器配置。 */
  /** 精确源库表行表达式，全量与增量均生效。 */
  row_filters?: Array<{
    schema: string
    table: string
    expression: string
    language?: string
    columns?: string[]
  }>
  transforms?: CleanseTransformRule[]
  mappings?: MappingRules
  incremental_window?: { max_batches: number; max_bytes: number; reorder_timeout_ms: number }
  incremental_rate_limit?: { rows_per_sec?: number | string; bytes_per_sec?: number | string }
  column_filters?: Array<{ schema: string; table: string; action: FilterAction; columns: string[] }>
}

export function createDefaultStrategyRules(): StrategyRules {
  return {
    object_filters: [],
    operation_filter: { insert: true, update: true, delete: true, ddl: false },
    origin_policy: { mode: 'ORIGIN_POLICY_MODE_LOCAL_ONLY', centers: [], max_hops: 3 },
    ddlPolicy: { mode: 'ignore', fromOrigins: [], blockDangerous: true },
    conflictPolicy: { mode: 'source_wins', lwwTables: [] },
    errorPolicy: { maxRetries: 5, backoffMs: 2000, skipAndLogOnNonRetryable: false },
    snapshotParams: {
      readConcurrency: 4,
      chunkRows: 5000,
      maxRowsPerTxn: 20000,
      maxBytesPerTxn: 16 * 1024 * 1024,
      writeConcurrency: 4,
      rateLimit: null,
      readFromReplica: false,
    },
    writeParams: {
      batchSize: 500,
      parallelism: 2,
      mergeRowsInSameTable: false,
      orderedCommit: false,
    },
  }
}

export type StrategySummary = generated.GeneratedStrategySummary

export type StrategyInput = generated.GeneratedStrategyInput

export type StrategyUpdateInput = Partial<StrategyInput>

export interface StrategyVersionSummary {
  id: string
  strategyId: string
  version: number
  /* 线格式（`{source, target}`），不是扁平的 `StrategyRules`。 */
  rules: unknown
  createdBy: string
  createdAt: string
}

export type StrategyDiffChangeType = 'added' | 'removed' | 'changed'

export type StrategyDiffEntry = generated.GeneratedStrategyDiffEntry

export function listStrategies(): Promise<StrategySummary[]> {
  if (isApiMockEnabled()) {
    return strategiesMock.listStrategies()
  }
  return generated.listStrategies()
}

export function getStrategy(id: string): Promise<StrategySummary> {
  if (isApiMockEnabled()) {
    return strategiesMock.getStrategy(id)
  }
  return generated.getStrategy(id)
}

export function createStrategy(input: StrategyInput): Promise<StrategySummary> {
  if (isApiMockEnabled()) {
    return strategiesMock.createStrategy(input)
  }
  return generated.createStrategy(input)
}

export function updateStrategyMeta(
  id: string,
  input: StrategyUpdateInput,
): Promise<StrategySummary> {
  if (isApiMockEnabled()) {
    return strategiesMock.updateStrategyMeta(id, input)
  }
  return generated.updateStrategyMeta(id, input)
}

/** 被任一未删除的 pipeline 引用时后端拒绝删除（MGR-STR-04），错误信息原样展示。 */
export function deleteStrategy(id: string): Promise<void> {
  if (isApiMockEnabled()) {
    return strategiesMock.deleteStrategy(id)
  }
  return generated.deleteStrategy(id).then(() => undefined)
}

export function listStrategyVersions(id: string): Promise<StrategyVersionSummary[]> {
  if (isApiMockEnabled()) {
    return strategiesMock.listStrategyVersions(id)
  }
  return generated.listStrategyVersions(id)
}

/**
 * 创建新版本——这就是「编辑」（MGR-STR-01：版本不可变，修改即生成新版本）。
 * 版本号由后端在事务里取「当前最大版本号 + 1」，同时把策略的 current_version
 * 推进到这个新版本；已经显式固定版本号的旧 pipeline 不受影响。
 */
/* Validate and map flat form rules to the source/target wire shape at the API boundary. Read responses through fromWireRules. */
export function createStrategyVersion(
  id: string,
  rules: StrategyRules,
): Promise<StrategyVersionSummary> {
  const mapped = toWireRules(rules)
  if (!mapped.ok) {
    return Promise.reject(
      new ApiError(
        mapped.blockers.map((b) => b.message).join('；'),
        400,
        'invalid_rules',
        undefined,
        mapped.blockers,
      ),
    )
  }
  if (isApiMockEnabled()) {
    return strategiesMock.createStrategyVersion(id, mapped.wire)
  }
  return generated.createStrategyVersion(id, { rules: mapped.wire })
}

/**
 * 见文件头注释 4：`base` 是较旧的对比基准，方向固定 `base → version`；省略
 * `base` 时后端默认取 `version - 1`。
 */
export function getStrategyVersionDiff(
  id: string,
  version: number,
  base?: number,
): Promise<StrategyDiffEntry[]> {
  if (isApiMockEnabled()) {
    return strategiesMock.getStrategyVersionDiff(id, version, base)
  }
  return generated.getStrategyVersionDiff(id, version, base !== undefined ? { base } : undefined)
}

/**
 * 见文件头注释 3：`error.message` 会点名具体是哪一条拒绝理由（当前生效版本 /
 * 被 pipeline 引用），原样展示，不要自己拼一句笼统的提示盖掉它。
 */
export function deleteStrategyVersion(id: string, version: number): Promise<void> {
  if (isApiMockEnabled()) {
    return strategiesMock.deleteStrategyVersion(id, version)
  }
  return generated.deleteStrategyVersion(id, version).then(() => undefined)
}

export const strategiesQueryKey = ['strategies', 'list'] as const
export const strategyDetailQueryKey = (id: string) => ['strategies', 'detail', id] as const
export const strategyVersionsQueryKey = (id: string) => ['strategies', id, 'versions'] as const
export const strategyDiffQueryKey = (id: string, version: number, base: number | undefined) =>
  ['strategies', id, 'diff', version, base ?? 'default'] as const

export function useStrategiesQuery(enabled = true) {
  return useQuery({ queryKey: strategiesQueryKey, queryFn: listStrategies, enabled })
}

export function useStrategyQuery(id: string | undefined) {
  return useQuery({
    queryKey: strategyDetailQueryKey(id ?? ''),
    queryFn: () => getStrategy(id ?? ''),
    enabled: Boolean(id),
  })
}

export function useStrategyVersionsQuery(id: string | undefined, enabled = true) {
  return useQuery({
    queryKey: strategyVersionsQueryKey(id ?? ''),
    queryFn: () => listStrategyVersions(id ?? ''),
    enabled: Boolean(id) && enabled,
  })
}

export function useStrategyVersionDiffQuery(
  id: string | undefined,
  version: number | undefined,
  base: number | undefined,
) {
  return useQuery({
    queryKey: strategyDiffQueryKey(id ?? '', version ?? 0, base),
    queryFn: () => getStrategyVersionDiff(id ?? '', version ?? 0, base),
    enabled: Boolean(id) && version !== undefined && base !== undefined && version !== base,
  })
}

export function useCreateStrategyMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: createStrategy,
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: strategiesQueryKey })
    },
  })
}

export function useUpdateStrategyMetaMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: StrategyUpdateInput }) =>
      updateStrategyMeta(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: strategiesQueryKey })
    },
  })
}

export function useDeleteStrategyMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => deleteStrategy(id),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: strategiesQueryKey })
    },
  })
}

export function useCreateStrategyVersionMutation(id: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (rules: StrategyRules) => createStrategyVersion(id, rules),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: strategyVersionsQueryKey(id) })
      void queryClient.invalidateQueries({ queryKey: strategiesQueryKey })
      void queryClient.invalidateQueries({ queryKey: strategyDetailQueryKey(id) })
    },
  })
}

export function useDeleteStrategyVersionMutation(id: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (version: number) => deleteStrategyVersion(id, version),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: strategyVersionsQueryKey(id) })
    },
  })
}
