/**
 * 顶层键。对应 `internal/manager/pipelinespec/rules.go` `rawRulesDoc`。
 *
 * **只有这两个。** 特别是没有 `advanced`：前端 `StrategyRules.advanced`
 * 在这里没有任何落点，它装的 P1/P2 内容要么落到下面 source/target 的对应键，
 * 要么无处可放（如"结构迁移"——`SchemaMigrationPlan` 挂在
 * `TargetSideConfig.schema_plan`，是 Manager 算出来下发的，不是用户配的）。
 */
export const WIRE_TOP_KEYS = ['source', 'target'] as const

/** `source` 下的键。对应 `rules.go` `rawSourceRules` 的 9 个字段，顺序一致。 */
export const WIRE_SOURCE_KEYS = [
  'object_filters',
  'operation_filter',
  'column_filters',
  'row_filters',
  'shadow_tables',
  'incremental_rate_limit',
  'origin_policy',
  'snapshot',
  'incremental_window',
] as const

/* `target` 下的键。对应 `internal/manager/pipelinespec/rules.go` `rawTargetRules` 的 7 个字段，顺序一致。 */
export const WIRE_TARGET_KEYS = [
  'mappings',
  'transforms',
  'write_policy',
  'conflict_policy',
  'ddl_policy',
  'error_policy',
  'snapshot_write_concurrency',
] as const

export type WireTopKey = (typeof WIRE_TOP_KEYS)[number]
export type WireSourceKey = (typeof WIRE_SOURCE_KEYS)[number]
export type WireTargetKey = (typeof WIRE_TARGET_KEYS)[number]

/** 线格式文档。叶子值刻意是 `unknown`：本层只管"这个键认不认识"，
 * 不碰值的内容——和服务端 `unmarshalStrictContainer` 的职责边界一致
 * （`internal/manager/pipelinespec/rules.go` 的 `unmarshalStrictContainer` 注释明确划了这条线）。叶子内容由 protojson 在服务端管。 */
export interface WireRulesDoc {
  source?: Partial<Record<WireSourceKey, unknown>>
  target?: Partial<Record<WireTargetKey, unknown>>
}

export type WireRejectionCode = 'invalid_rules'

export interface WireRejection {
  code: WireRejectionCode
  message: string
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function unknownKeys(obj: Record<string, unknown>, allowed: readonly string[]): string[] {
  return Object.keys(obj).filter((k) => !allowed.includes(k))
}

/* Validate wire shape before unsupported categories, matching server error priority. Leaf validation belongs to the rule validators. */
export function validateWireRules(value: unknown): WireRejection | null {
  if (!isPlainObject(value)) {
    return { code: 'invalid_rules', message: 'rules 必须是一个 JSON 对象' }
  }

  const extraTop = unknownKeys(value, WIRE_TOP_KEYS)
  if (extraTop.length > 0) {
    return {
      code: 'invalid_rules',
      // 报出全部未知键而不是第一个：整份形状不对时（今天前端发的扁平体就是
      // 这种）只报 `object_filters` 会让人以为只错了一个字段。
      message: `rules 顶层只接受 source/target，不认识的键：${extraTop.join(', ')}`,
    }
  }

  const { source, target } = value as WireRulesDoc

  if (source !== undefined) {
    if (!isPlainObject(source)) {
      return { code: 'invalid_rules', message: 'rules.source 必须是一个 JSON 对象' }
    }
    const extra = unknownKeys(source, WIRE_SOURCE_KEYS)
    if (extra.length > 0) {
      return { code: 'invalid_rules', message: `rules.source 不认识的键：${extra.join(', ')}` }
    }
  }

  if (target !== undefined) {
    if (!isPlainObject(target)) {
      return { code: 'invalid_rules', message: 'rules.target 必须是一个 JSON 对象' }
    }
    const extra = unknownKeys(target, WIRE_TARGET_KEYS)
    if (extra.length > 0) {
      return { code: 'invalid_rules', message: `rules.target 不认识的键：${extra.join(', ')}` }
    }
  }

  return null
}

// ════════════════════════════════════════════════════════════════════
// 出站映射：扁平 StrategyRules → 线格式
// ════════════════════════════════════════════════════════════════════

import type { ConflictPolicyMode, DdlPolicyMode, StrategyRules } from './strategies'

/* 映射被挡住的一条原因。`path` 与 `web/src/features/strategies/validateRules.ts` 的 `RuleValidationError.path`
同形（`StrategyRules` 的字段路径），便于表单定位到具体区块。 */
export interface WireBlocker {
  path: string
  message: string
}

export function validateTransformRules(input: unknown): string | null {
  if (input === undefined) return null
  const invalid = i18n.t('strategies.editor.fields.transformInvalid')
  if (!Array.isArray(input) || input.length > 128) return invalid
  const seen = new Set<string>()
  const byteLength = (value: string) => new TextEncoder().encode(value).length
  for (const value of input) {
    if (!isPlainObject(value)) return invalid
    const { schema, table, column, kind, execution_side: side } = value
    // protojson omits an empty parameter map; masking uses its default parameters.
    const params = value.params === undefined ? {} : value.params
    if (
      [schema, table, column].some(
        (name) =>
          typeof name !== 'string' || !name.trim() || byteLength(name) > 256 || name.includes('\0'),
      )
    )
      return invalid
    if (
      side !== undefined &&
      ![
        'TRANSFORM_EXECUTION_SIDE_UNSPECIFIED',
        'TRANSFORM_EXECUTION_SIDE_SOURCE',
        'TRANSFORM_EXECUTION_SIDE_TARGET',
      ].includes(String(side))
    )
      return invalid
    const key = `${String(schema).toLowerCase()}\0${String(table).toLowerCase()}\0${String(column).toLowerCase()}`
    if (seen.has(key) || !isPlainObject(params)) return invalid
    seen.add(key)
    const allowed =
      kind === 'CLEANSE_TRANSFORM_KIND_MASK'
        ? ['pattern', 'keep_prefix', 'keep_suffix']
        : kind === 'CLEANSE_TRANSFORM_KIND_CONST_FILL'
          ? ['value', 'target_type']
          : kind === 'CLEANSE_TRANSFORM_KIND_TYPE_CONVERT'
            ? ['target_type']
            : kind === 'CLEANSE_TRANSFORM_KIND_LUA'
              ? ['script']
              : kind === 'CLEANSE_TRANSFORM_KIND_PLUGIN'
                ? ['plugin', 'cutset', 'old', 'new', 'path']
                : null
    if (!allowed) return invalid
    for (const [name, param] of Object.entries(params)) {
      if (param === undefined) continue
      if (
        !allowed.includes(name) ||
        typeof param !== 'string' ||
        byteLength(param) > 4096 ||
        param.includes('\0')
      )
        return invalid
    }
    if (kind === 'CLEANSE_TRANSFORM_KIND_MASK') {
      for (const name of ['keep_prefix', 'keep_suffix']) {
        if (
          params[name] !== undefined &&
          (!/^\d+$/.test(String(params[name])) || Number(params[name]) > 1024)
        )
          return invalid
      }
    } else if (kind === 'CLEANSE_TRANSFORM_KIND_LUA') {
      if (typeof params.script !== 'string' || !params.script.trim()) return invalid
    } else if (kind === 'CLEANSE_TRANSFORM_KIND_PLUGIN') {
      if (!['trim', 'replace', 'json_extract'].includes(String(params.plugin))) return invalid
      if (
        params.plugin === 'replace' &&
        (typeof params.old !== 'string' || !params.old || typeof params.new !== 'string')
      )
        return invalid
      if (
        params.plugin === 'json_extract' &&
        (typeof params.path !== 'string' || !params.path.startsWith('$.'))
      )
        return invalid
    } else {
      if (kind === 'CLEANSE_TRANSFORM_KIND_CONST_FILL' && typeof params.value !== 'string')
        return invalid
      const type =
        params.target_type ?? (kind === 'CLEANSE_TRANSFORM_KIND_CONST_FILL' ? 'text' : '')
      if (!['text', 'int64', 'uint64', 'float64', 'bool', 'null'].includes(String(type)))
        return invalid
    }
  }
  return null
}

export type WireMapResult =
  | { ok: true; wire: GeneratedStrategyRulesDoc }
  | { ok: false; blockers: WireBlocker[] }

import i18n from '@/i18n'
import type {
  GeneratedStrategyRulesDoc,
  GeneratedProtoDDLMode,
  GeneratedProtoConflictMode,
} from './generated'

function validUint64(value: unknown): boolean {
  if (typeof value === 'number') return Number.isSafeInteger(value) && value >= 0
  return (
    typeof value === 'string' &&
    value.length <= 20 &&
    /^(0|[1-9]\d*)$/.test(value) &&
    BigInt(value) <= 18446744073709551615n
  )
}

export function validateDDLDefinerFallback(input: unknown): string | null {
  if (input === undefined || input === '') return null
  const invalid = i18n.t('strategies.editor.fields.ddlDefinerInvalid')
  if (typeof input !== 'string' || input.includes('\0')) return invalid
  const value = input.trim()
  if (/^CURRENT_USER(?:\(\))?$/i.test(value)) return null
  let quoted = false
  let split = -1
  for (let i = 0; i < value.length; i++) {
    if (value[i] === '`') quoted = !quoted
    if (value[i] === '@' && !quoted) {
      split = i
      break
    }
  }
  if (split < 0) return invalid
  const unquote = (part: string) =>
    part.length >= 2 && part.startsWith('`') && part.endsWith('`')
      ? part.slice(1, -1).replaceAll('``', '`')
      : part
  const user = unquote(value.slice(0, split))
  const host = unquote(value.slice(split + 1))
  return user && host && [...user].length <= 32 && new TextEncoder().encode(host).length <= 255
    ? null
    : invalid
}

export function validateIncrementalRate(input: unknown): string | null {
  if (input === undefined) return null
  const fail = () => i18n.t('strategies.editor.fields.incrementalRateInvalid')
  if (!input || typeof input !== 'object' || Array.isArray(input)) return fail()
  for (const [name, value] of Object.entries(input)) {
    if (
      !['rows_per_sec', 'bytes_per_sec'].includes(name) ||
      (value !== undefined && !validUint64(value))
    )
      return fail()
  }
  return null
}

export function validateMappingRules(input: unknown): string | null {
  if (input === undefined) return null
  const fail = () => i18n.t('strategies.editor.fields.mappingInvalid')
  if (!input || typeof input !== 'object' || Array.isArray(input)) return fail()
  const mappings = input as Record<string, unknown>
  if (Object.keys(mappings).some((key) => !['schemas', 'tables', 'columns'].includes(key)))
    return fail()
  for (const kind of ['schemas', 'tables', 'columns'] as const) {
    const entries = mappings[kind]
    if (entries === undefined) continue
    if (!Array.isArray(entries)) return fail()
    const names =
      kind === 'schemas'
        ? ['source_schema', 'target_schema']
        : kind === 'tables'
          ? ['source_schema', 'source_table', 'target_table']
          : ['source_schema', 'source_table', 'source_column', 'target_column']
    const sources = new Set<string>()
    const targets = new Set<string>()
    for (const entry of entries) {
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) return fail()
      const fields = entry as Record<string, unknown>
      if (Object.keys(fields).some((key) => !names.includes(key))) return fail()
      for (const name of names) {
        const value = fields[name]
        if (
          typeof value !== 'string' ||
          value.length === 0 ||
          [...value].length > 64 ||
          value.includes('\0')
        )
          return fail()
        if (
          name.endsWith('schema') &&
          ['mysql', 'sys', 'information_schema', 'performance_schema'].includes(value.toLowerCase())
        )
          return fail()
      }
      const source = names
        .filter((name) => name.startsWith('source'))
        .map((name) => (fields[name] as string).toLowerCase())
        .join('\0')
      if (sources.has(source)) return fail()
      sources.add(source)
      if (kind === 'columns') {
        const target = `${String(fields.source_schema).toLowerCase()}\0${String(fields.source_table).toLowerCase()}\0${String(fields.target_column).toLowerCase()}`
        if (targets.has(target)) return fail()
        targets.add(target)
      }
    }
  }
  return null
}

/** JSON editor input must receive the same validation as form input. */
export function validateConflictMaskRules(input: unknown): string | null {
  if (input === undefined) return null
  const invalid = i18n.t('strategies.editor.conflictMaskInvalid')
  if (!Array.isArray(input)) return invalid
  if (input.length > 4000) return i18n.t('strategies.editor.conflictMaskLimit')
  const tables = new Set<string>()
  for (const entry of input) {
    const rule = obj(entry)
    if (
      typeof rule.schema !== 'string' ||
      !rule.schema.trim() ||
      rule.schema.length > 128 ||
      typeof rule.table !== 'string' ||
      !rule.table.trim() ||
      rule.table.length > 128 ||
      !Array.isArray(rule.columns) ||
      rule.columns.length === 0 ||
      rule.columns.length > 4096
    )
      return invalid
    const key = `${rule.schema.toLowerCase()}\0${rule.table.toLowerCase()}`
    if (tables.has(key)) return invalid
    tables.add(key)
    const columns = new Set<string>()
    for (const column of rule.columns) {
      if (
        typeof column !== 'string' ||
        !column.trim() ||
        column.length > 64 ||
        columns.has(column.toLowerCase())
      )
        return invalid
      columns.add(column.toLowerCase())
    }
  }
  return null
}

/* Explicit enum mappings cover nonmatching names such as from_origins and last_write_wins. */
const DDL_MODE_WIRE: Record<DdlPolicyMode, GeneratedProtoDDLMode> = {
  sync: 'DDL_MODE_SYNC',
  ignore: 'DDL_MODE_IGNORE',
  from_origins: 'DDL_MODE_ONLY_FROM_CENTERS',
}

const CONFLICT_MODE_WIRE: Record<ConflictPolicyMode, GeneratedProtoConflictMode> = {
  source_wins: 'CONFLICT_MODE_SOURCE_WINS',
  last_write_wins: 'CONFLICT_MODE_LWW',
  stop: 'CONFLICT_MODE_STOP',
}

const UINT32_MAX = 4294967295

/* 校验有限整数、非负值和协议上界，在请求发送前返回可定位的错误。 */
function checkUint(path: string, value: unknown, max: number, blockers: WireBlocker[]): void {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    blockers.push({ path, message: `${path} 必须是数字` })
    return
  }
  if (!Number.isInteger(value)) {
    blockers.push({ path, message: `${path} 必须是整数（协议是无符号整型）` })
    return
  }
  if (value < 0) {
    blockers.push({ path, message: `${path} 不能为负数（协议是无符号整型）` })
    return
  }
  if (value > max) {
    blockers.push({ path, message: `${path} 超出协议上限 ${max}` })
  }
}

export function toWireRules(rules: StrategyRules): WireMapResult {
  const blockers: WireBlocker[] = []
  const definerError = validateDDLDefinerFallback(rules.ddlPolicy.definerFallback)
  if (definerError) blockers.push({ path: 'ddlPolicy.definerFallback', message: definerError })
  const mappingError = validateMappingRules(rules.mappings)
  if (mappingError) blockers.push({ path: 'mappings', message: mappingError })
  const rateError = validateIncrementalRate(rules.incremental_rate_limit)
  if (rateError) blockers.push({ path: 'incremental_rate_limit', message: rateError })
  const transformError = validateTransformRules(rules.transforms)
  if (transformError) blockers.push({ path: 'transforms', message: transformError })
  if (rules.row_filters !== undefined) {
    if (!Array.isArray(rules.row_filters) || rules.row_filters.length > 128) {
      blockers.push({ path: 'row_filters', message: '行过滤最多允许128条条件' })
    } else {
      for (const [index, rule] of rules.row_filters.entries()) {
        if (
          !rule ||
          typeof rule.schema !== 'string' ||
          !rule.schema.trim() ||
          typeof rule.table !== 'string' ||
          !rule.table.trim() ||
          typeof rule.expression !== 'string' ||
          !rule.expression.trim() ||
          new TextEncoder().encode(rule.schema).length > 256 ||
          new TextEncoder().encode(rule.table).length > 256 ||
          new TextEncoder().encode(rule.expression).length > 4096 ||
          (rule.language !== undefined && !['expr', 'lua'].includes(rule.language)) ||
          (rule.language === 'lua' &&
            (!Array.isArray(rule.columns) ||
              rule.columns.length === 0 ||
              rule.columns.length > 200)) ||
          (rule.language !== 'lua' && (rule.columns?.length ?? 0) > 0)
        ) {
          blockers.push({
            path: `row_filters.${index}`,
            message: '行过滤需要源库、源表及不超过4KiB的表达式',
          })
        }
      }
    }
  }

  // ——— 挡住的三类 ———
  const advanced = rules.advanced
  if (advanced && typeof advanced === 'object' && Object.keys(advanced).length > 0) {
    blockers.push({
      path: 'advanced',
      message: `高级配置（${Object.keys(advanced).join(', ')}）当前无法保存：协议顶层只接受 source/target，这些类别尚无落点`,
    })
  }

  const lwwTables = rules.conflictPolicy.lwwTables ?? []
  const maskRules = rules.conflictPolicy.maskRules ?? []
  const maskError = validateConflictMaskRules(rules.conflictPolicy.maskRules)
  if (maskError) blockers.push({ path: 'conflictPolicy', message: maskError })
  if (rules.conflictPolicy.mode === 'last_write_wins') {
    const tables = new Set<string>()
    for (const table of lwwTables) {
      const key = `${table.schema.toLowerCase()}\0${table.table.toLowerCase()}`
      if (
        !table.schema.trim() ||
        !table.table.trim() ||
        !table.timestampColumn.trim() ||
        tables.has(key)
      ) {
        blockers.push({ path: 'conflictPolicy', message: 'LWW 表配置需要唯一的库、表和时间戳列' })
      }
      tables.add(key)
    }
  }

  const ddlMode = DDL_MODE_WIRE[rules.ddlPolicy.mode]
  if (ddlMode === undefined) {
    // 只能从 JSON 编辑器来（`JSON.parse(...) as StrategyRules` 是一次断言，
    // 运行期没有检查）。**不回退成 *_UNSPECIFIED**：那会把"映射表漏了一项"
    // 或"用户写错了"伪装成"用户没配"，正是 `pipelinespec.ValidateObjectFilterRules`
    // 文档注释描述的坑（「proto3 枚举零值天然模糊了"没填"和"填了 UNSPECIFIED"」）。
    blockers.push({
      path: 'ddlPolicy',
      message: `DDL 模式取值无法识别：${String(rules.ddlPolicy.mode)}`,
    })
  }
  const conflictMode = CONFLICT_MODE_WIRE[rules.conflictPolicy.mode]
  if (conflictMode === undefined) {
    blockers.push({
      path: 'conflictPolicy',
      message: `冲突模式取值无法识别：${String(rules.conflictPolicy.mode)}`,
    })
  }

  // ——— 数值边界 ———
  const sp = rules.snapshotParams
  if (
    sp.readFromReplica &&
    (typeof sp.replicaEndpointId !== 'string' ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
        sp.replicaEndpointId,
      ) ||
      sp.replicaEndpointId === '00000000-0000-0000-0000-000000000000')
  ) {
    blockers.push({
      path: 'snapshotParams.replicaEndpointId',
      message: 'replica snapshot requires a registered endpoint UUID',
    })
  } else if (!sp.readFromReplica && sp.replicaEndpointId) {
    blockers.push({
      path: 'snapshotParams.replicaEndpointId',
      message: 'replica endpoint requires replica snapshot mode',
    })
  }
  const wp = rules.writeParams
  const ep = rules.errorPolicy
  checkUint('origin_policy.max_hops', rules.origin_policy.max_hops, UINT32_MAX, blockers)
  checkUint('snapshotParams.readConcurrency', sp.readConcurrency, UINT32_MAX, blockers)
  checkUint('snapshotParams.chunkRows', sp.chunkRows, UINT32_MAX, blockers)
  checkUint('snapshotParams.maxRowsPerTxn', sp.maxRowsPerTxn, UINT32_MAX, blockers)
  checkUint('snapshotParams.maxBytesPerTxn', sp.maxBytesPerTxn, UINT32_MAX, blockers)
  checkUint('snapshotParams.writeConcurrency', sp.writeConcurrency, UINT32_MAX, blockers)
  checkUint('writeParams.batchSize', wp.batchSize, 10000, blockers)
  checkUint('writeParams.parallelism', wp.parallelism, 128, blockers)
  if (rules.incremental_window !== undefined) {
    const window = rules.incremental_window
    checkUint('incremental_window.max_batches', window.max_batches, 128, blockers)
    checkUint('incremental_window.max_bytes', window.max_bytes, 1073741824, blockers)
    checkUint('incremental_window.reorder_timeout_ms', window.reorder_timeout_ms, 600000, blockers)
    if (window.reorder_timeout_ms > 0 && window.reorder_timeout_ms < 100) {
      blockers.push({
        path: 'incremental_window.reorder_timeout_ms',
        message: 'incremental_window.reorder_timeout_ms must be zero or at least 100',
      })
    }
  }
  checkUint('errorPolicy.maxRetries', ep.maxRetries, UINT32_MAX, blockers)
  checkUint('errorPolicy.backoffMs', ep.backoffMs, UINT32_MAX, blockers)
  if (sp.rateLimit !== null) {
    // rows_per_sec 是 uint64，JS number 撑不到它的上界，用安全整数上界兜。
    checkUint('snapshotParams.rateLimit', sp.rateLimit, Number.MAX_SAFE_INTEGER, blockers)
  }

  if (blockers.length > 0) {
    return { ok: false, blockers }
  }

  return {
    ok: true,
    wire: {
      source: {
        ...(rules.incremental_window !== undefined
          ? {
              incremental_window: {
                ...rules.incremental_window,
                max_bytes: String(rules.incremental_window.max_bytes),
              },
            }
          : {}),
        ...(rules.incremental_rate_limit !== undefined
          ? {
              incremental_rate_limit: Object.fromEntries(
                Object.entries(rules.incremental_rate_limit).filter(
                  ([, value]) => value !== undefined,
                ),
              ),
            }
          : {}),
        ...(rules.row_filters !== undefined
          ? { row_filters: rules.row_filters.map((rule) => ({ ...rule })) }
          : {}),
        ...(rules.column_filters !== undefined ? { column_filters: rules.column_filters } : {}),
        // 这三个前端本来就是 snake_case 且叶子字段逐一对得上，**只缺一层包装**。
        object_filters: rules.object_filters,
        operation_filter: rules.operation_filter,
        origin_policy: rules.origin_policy,
        snapshot: {
          read_concurrency: sp.readConcurrency,
          chunk_rows: sp.chunkRows,
          // 注意是 max_txn_rows / max_txn_bytes，不是 max_rows_per_txn：
          // 协议换的是词序，不是分隔符。
          max_txn_rows: sp.maxRowsPerTxn,
          max_txn_bytes: sp.maxBytesPerTxn,
          read_from_replica: sp.readFromReplica,
          ...(sp.replicaEndpointId ? { replica_endpoint_id: sp.replicaEndpointId } : {}),

          ...(sp.rateLimit === null ? {} : { rate_limit: { rows_per_sec: sp.rateLimit } }),
        },
      },
      target: {
        ...(rules.mappings !== undefined ? { mappings: structuredClone(rules.mappings) } : {}),
        ...(rules.transforms !== undefined
          ? {
              transforms: rules.transforms.map((rule) => ({
                ...rule,
                params: Object.fromEntries(
                  Object.entries(rule.params).filter(([, value]) => value !== undefined),
                ),
              })),
            }
          : {}),
        write_policy: {
          batch_size: wp.batchSize,
          parallel_replay_degree: wp.parallelism,
          merge_same_table_rows: wp.mergeRowsInSameTable,
          ...(wp.orderedCommit ? { ordered_commit: true } : {}),
        },
        conflict_policy: {
          mode: conflictMode,
          ...(maskRules.length > 0 ? { mask_rules: maskRules } : {}),
          ...(rules.conflictPolicy.mode === 'last_write_wins'
            ? {
                lww_tables: lwwTables.map((table) => ({
                  schema: table.schema,
                  table: table.table,
                  timestamp_column: table.timestampColumn,
                })),
              }
            : {}),
        },
        ddl_policy: {
          mode: ddlMode,
          only_from_centers: rules.ddlPolicy.fromOrigins,
          // blockDangerous maps to block_drop and block_truncate.
          allow_drop: !rules.ddlPolicy.blockDangerous,
          allow_truncate: !rules.ddlPolicy.blockDangerous,
          ...(rules.ddlPolicy.definerFallback !== undefined
            ? { definer_fallback: rules.ddlPolicy.definerFallback }
            : {}),
        },
        error_policy: {
          retryable_max_retries: ep.maxRetries,
          retry_backoff_ms: ep.backoffMs,
          // retry_backoff_max_ms 协议有、前端无：**有意不填**，走 omitempty。
          // 写在这里是因为下一个人对着 proto 看会以为这里漏了。
          skip_and_log_unretryable: ep.skipAndLogOnNonRetryable,
        },
        // **提到 target 这一层**，不在 snapshot 里面——目标端独占，不随源端
        // 那份下发给源节点（proto/config.proto 的 SnapshotParams 文档）。放错层服务端会 400。
        snapshot_write_concurrency: sp.writeConcurrency,
      },
    },
  }
}

// ════════════════════════════════════════════════════════════════════
// 入站映射：线格式 → 扁平 StrategyRules（+ 必须告知的事项）
// ════════════════════════════════════════════════════════════════════

/* 入站时"协议表达得出、本界面表达不出"的一条。 */
export interface WireNotice {
  /* 对应 `StrategyRules` 的字段路径，与 `validateRules.ts` 的 `path` 同形。 */
  path: string
  /* `disclose`：回显了一个近似值，必须显式告知，但允许保存。
`unrepresentable`：不回显任何值，并且禁止提交该区块。 */
  kind: 'disclose' | 'unrepresentable'
  code:
    | 'ddlDangerousMismatch'
    | 'lwwTablesUnrepresentable'
    | 'enumUnrecognized'
    | 'replicaEndpointUnrepresentable'
    | 'integerUnrepresentable'
  /** 渲染文案要用的原始值。**服务端原值原样放进来，不要在这里格式化。** */
  params: Record<string, unknown>
}

export interface FromWireResult {
  rules: StrategyRules
  notices: WireNotice[]
}

/** 反向枚举表。同样是 `Record<线上取值, 前端取值>`，漏一项 tsc 不会报
 * （键是 string），所以下面查不到时**不塌缩到默认值**而是产出 notice。 */
const DDL_MODE_FROM_WIRE: Record<string, DdlPolicyMode> = {
  DDL_MODE_SYNC: 'sync',
  DDL_MODE_IGNORE: 'ignore',
  DDL_MODE_ONLY_FROM_CENTERS: 'from_origins',
}
const CONFLICT_MODE_FROM_WIRE: Record<string, ConflictPolicyMode> = {
  CONFLICT_MODE_SOURCE_WINS: 'source_wins',
  CONFLICT_MODE_LWW: 'last_write_wins',
  CONFLICT_MODE_STOP: 'stop',
}

function obj(v: unknown): Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : {}
}
/** 取数字，缺席/非数字时用 fallback。**不把 undefined 当 0**：0 是合法观测值。 */
function num(v: unknown, fallback: number): number {
  return typeof v === 'number' && Number.isFinite(v) ? v : fallback
}
// protojson serializes int64/uint64 as decimal strings. Numeric controls only
// accept safe integers; values outside that range must block submission while
// preserving the original JSON rather than silently becoming a default.
function wireInteger(
  value: unknown,
  fallback: number,
  path: string,
  notices: WireNotice[],
): number {
  if (value === undefined) return fallback
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return value
  if (typeof value === 'string' && /^(0|[1-9]\d*)$/.test(value) && value.length <= 20) {
    const n = BigInt(value)
    if (n <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(n)
  }
  notices.push({
    path,
    kind: 'unrepresentable',
    code: 'integerUnrepresentable',
    params: { field: path, value },
  })
  return fallback
}
function bool(v: unknown, fallback: boolean): boolean {
  return typeof v === 'boolean' ? v : fallback
}

/* 把线格式转回扁平 `StrategyRules`，并列出所有"本界面表达不出"的事项。

`base` 是缺省基线（通常是 `createDefaultStrategyRules()`）：线格式里缺的字段
用它补。由调用方传入而不是本模块 import，一是避免与 `web/src/api/strategies.ts`
形成运行期循环依赖，二是"缺省值是什么"本来就该由调用方决定。 */
export function fromWireRules(wire: unknown, base: StrategyRules): FromWireResult {
  const notices: WireNotice[] = []
  const doc = obj(wire)
  const src = obj(doc.source)
  const tgt = obj(doc.target)
  const snap = obj(src.snapshot)
  if ('replica_endpoint_id' in snap && typeof snap.replica_endpoint_id !== 'string') {
    notices.push({
      path: 'snapshotParams.replicaEndpointId',
      kind: 'unrepresentable',
      code: 'replicaEndpointUnrepresentable',
      params: { value: snap.replica_endpoint_id },
    })
  }
  const wp = obj(tgt.write_policy)
  const ep = obj(tgt.error_policy)
  const dp = obj(tgt.ddl_policy)
  const cp = obj(tgt.conflict_policy)

  // ——— DDL：两个布尔回到一个，不一致时取安全侧并告知 ———
  const allowDrop = dp.allow_drop
  const allowTruncate = dp.allow_truncate
  let blockDangerous: boolean
  if (typeof allowDrop === 'boolean' && typeof allowTruncate === 'boolean') {
    if (allowDrop === allowTruncate) {
      blockDangerous = !allowDrop
    } else {
      // 取**阻断侧**。两个方向都会改写用户配置，但方向不对称：取放行侧会
      // 打开一个用户没打开过的危险操作（DROP/TRUNCATE 打到目标库），取阻断侧
      // 最坏是多拦一次、用户再点开。**失败方向要选可恢复的那个。**
      blockDangerous = true
      notices.push({
        path: 'ddlPolicy',
        kind: 'disclose',
        code: 'ddlDangerousMismatch',
        // 三样都带上：服务端两个原值、以及"照现在这样保存会把两者都改成什么"。
        params: { allowDrop, allowTruncate, willSetBothTo: false },
      })
    }
  } else {
    blockDangerous = base.ddlPolicy.blockDangerous
  }

  const ddlModeRaw = dp.mode
  let ddlMode = base.ddlPolicy.mode
  if (typeof ddlModeRaw === 'string') {
    const mapped = DDL_MODE_FROM_WIRE[ddlModeRaw]
    if (mapped !== undefined) {
      ddlMode = mapped
    } else {
      // 含 `DDL_MODE_UNSPECIFIED`。**不塌缩成"用户选了第一个值"**——那正是
      // proto3 枚举零值把"没填"和"填了 UNSPECIFIED"搅在一起的那个坑。
      notices.push({
        path: 'ddlPolicy',
        kind: 'disclose',
        code: 'enumUnrecognized',
        params: { field: 'ddl_policy.mode', value: ddlModeRaw, fallback: ddlMode },
      })
    }
  }

  // 逐表读取 LWW 列名，缺失字段留空交给校验，不补猜测值。
  const conflictModeRaw = cp.mode
  let conflictMode = base.conflictPolicy.mode
  if (typeof conflictModeRaw === 'string') {
    const mapped = CONFLICT_MODE_FROM_WIRE[conflictModeRaw]
    if (mapped !== undefined) {
      conflictMode = mapped
    } else {
      notices.push({
        path: 'conflictPolicy',
        kind: 'disclose',
        code: 'enumUnrecognized',
        params: { field: 'conflict_policy.mode', value: conflictModeRaw, fallback: conflictMode },
      })
    }
  }

  const lwwTables = Array.isArray(cp.lww_tables)
    ? cp.lww_tables.map((entry) => {
        const table = obj(entry)
        return {
          schema: typeof table.schema === 'string' ? table.schema : '',
          table: typeof table.table === 'string' ? table.table : '',
          timestampColumn: typeof table.timestamp_column === 'string' ? table.timestamp_column : '',
        }
      })
    : []
  const maskRules = Array.isArray(cp.mask_rules)
    ? cp.mask_rules.map((entry) => {
        const rule = obj(entry)
        return {
          schema: typeof rule.schema === 'string' ? rule.schema : '',
          table: typeof rule.table === 'string' ? rule.table : '',
          columns: Array.isArray(rule.columns)
            ? rule.columns.map((column) => (typeof column === 'string' ? column : ''))
            : [],
        }
      })
    : []

  const rules: StrategyRules = {
    ...('mappings' in tgt
      ? { mappings: structuredClone(tgt.mappings) as StrategyRules['mappings'] }
      : base.mappings !== undefined
        ? { mappings: structuredClone(base.mappings) }
        : {}),
    ...('incremental_window' in src
      ? {
          incremental_window: {
            max_batches: num(obj(src.incremental_window).max_batches, 1),
            max_bytes: wireInteger(
              obj(src.incremental_window).max_bytes,
              67108864,
              'incremental_window.max_bytes',
              notices,
            ),
            reorder_timeout_ms: wireInteger(
              obj(src.incremental_window).reorder_timeout_ms,
              60000,
              'incremental_window.reorder_timeout_ms',
              notices,
            ),
          },
        }
      : base.incremental_window !== undefined
        ? { incremental_window: { ...base.incremental_window } }
        : {}),
    ...('incremental_rate_limit' in src
      ? {
          incremental_rate_limit: structuredClone(
            obj(src.incremental_rate_limit),
          ) as StrategyRules['incremental_rate_limit'],
        }
      : base.incremental_rate_limit !== undefined
        ? { incremental_rate_limit: { ...base.incremental_rate_limit } }
        : {}),
    ...(Array.isArray(tgt.transforms)
      ? {
          transforms: tgt.transforms.map((entry) =>
            isPlainObject(entry) && entry.params === undefined
              ? { ...structuredClone(entry), params: {} }
              : structuredClone(entry),
          ) as StrategyRules['transforms'],
        }
      : base.transforms !== undefined
        ? { transforms: structuredClone(base.transforms) }
        : {}),
    ...(Array.isArray(src.row_filters)
      ? {
          row_filters: src.row_filters.map((entry) => {
            const rule = obj(entry)
            return {
              schema: typeof rule.schema === 'string' ? rule.schema : '',
              table: typeof rule.table === 'string' ? rule.table : '',
              expression: typeof rule.expression === 'string' ? rule.expression : '',
              ...(typeof rule.language === 'string' ? { language: rule.language } : {}),
              ...(Array.isArray(rule.columns) ? { columns: rule.columns.map(String) } : {}),
            }
          }),
        }
      : base.row_filters !== undefined
        ? { row_filters: base.row_filters.map((rule) => ({ ...rule })) }
        : {}),
    ...(Array.isArray(src.column_filters)
      ? { column_filters: src.column_filters as NonNullable<StrategyRules['column_filters']> }
      : base.column_filters !== undefined
        ? { column_filters: base.column_filters }
        : {}),
    object_filters: Array.isArray(src.object_filters)
      ? (src.object_filters as StrategyRules['object_filters'])
      : base.object_filters,
    operation_filter: {
      insert: bool(obj(src.operation_filter).insert, base.operation_filter.insert),
      update: bool(obj(src.operation_filter).update, base.operation_filter.update),
      delete: bool(obj(src.operation_filter).delete, base.operation_filter.delete),
      ddl: bool(obj(src.operation_filter).ddl, base.operation_filter.ddl),
    },
    origin_policy: {
      mode: (typeof obj(src.origin_policy).mode === 'string'
        ? obj(src.origin_policy).mode
        : base.origin_policy.mode) as StrategyRules['origin_policy']['mode'],
      centers: Array.isArray(obj(src.origin_policy).centers)
        ? (obj(src.origin_policy).centers as string[])
        : base.origin_policy.centers,
      max_hops: num(obj(src.origin_policy).max_hops, base.origin_policy.max_hops),
      ...(typeof obj(src.origin_policy).drop_unknown_origin === 'boolean'
        ? { drop_unknown_origin: obj(src.origin_policy).drop_unknown_origin as boolean }
        : base.origin_policy.drop_unknown_origin !== undefined
          ? { drop_unknown_origin: base.origin_policy.drop_unknown_origin }
          : {}),
    },
    ddlPolicy: {
      mode: ddlMode,
      fromOrigins: Array.isArray(dp.only_from_centers)
        ? (dp.only_from_centers as string[])
        : base.ddlPolicy.fromOrigins,
      blockDangerous,
      ...(typeof dp.definer_fallback === 'string'
        ? { definerFallback: dp.definer_fallback }
        : base.ddlPolicy.definerFallback !== undefined
          ? { definerFallback: base.ddlPolicy.definerFallback }
          : {}),
    },
    conflictPolicy: {
      mode: conflictMode,
      lwwTables,
      ...(maskRules.length > 0 ? { maskRules } : {}),
    },
    errorPolicy: {
      maxRetries: num(ep.retryable_max_retries, base.errorPolicy.maxRetries),
      backoffMs: wireInteger(
        ep.retry_backoff_ms,
        base.errorPolicy.backoffMs,
        'errorPolicy.backoffMs',
        notices,
      ),
      skipAndLogOnNonRetryable: bool(
        ep.skip_and_log_unretryable,
        base.errorPolicy.skipAndLogOnNonRetryable,
      ),
    },
    snapshotParams: {
      readConcurrency: num(snap.read_concurrency, base.snapshotParams.readConcurrency),
      chunkRows: num(snap.chunk_rows, base.snapshotParams.chunkRows),
      maxRowsPerTxn: num(snap.max_txn_rows, base.snapshotParams.maxRowsPerTxn),
      maxBytesPerTxn: num(snap.max_txn_bytes, base.snapshotParams.maxBytesPerTxn),
      // 提回来：它在线格式里是 target 的字段，不在 snapshot 里。
      writeConcurrency: num(tgt.snapshot_write_concurrency, base.snapshotParams.writeConcurrency),
      // **消息缺席 ⇒ null（不限）**，与出站那一侧严格互逆。有消息但
      // rows_per_sec 缺席时按 0 读——那是"配过 0"，与"没配过"不同。
      rateLimit:
        'rate_limit' in snap
          ? wireInteger(obj(snap.rate_limit).rows_per_sec, 0, 'snapshotParams.rateLimit', notices)
          : null,
      readFromReplica: bool(snap.read_from_replica, base.snapshotParams.readFromReplica),
      ...(typeof snap.replica_endpoint_id === 'string' && snap.replica_endpoint_id
        ? { replicaEndpointId: snap.replica_endpoint_id }
        : {}),
    },
    writeParams: {
      batchSize: num(wp.batch_size, base.writeParams.batchSize),
      parallelism: num(wp.parallel_replay_degree, base.writeParams.parallelism),
      orderedCommit: bool(wp.ordered_commit, false),
      mergeRowsInSameTable: bool(wp.merge_same_table_rows, base.writeParams.mergeRowsInSameTable),
    },
  }

  return { rules, notices }
}
