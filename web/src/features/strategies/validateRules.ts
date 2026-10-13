// 保存前校验表单字段；服务端负责规则编译和最终语义校验。
import type { StrategyRules } from '@/api/strategies'
import {
  validateConflictMaskRules,
  validateTransformRules,
  validateMappingRules,
  validateIncrementalRate,
  validateDDLDefinerFallback,
} from '@/api/strategyRulesWire'

export interface RuleValidationError {
  /** 对应 StrategyRules 的字段路径，用于表单定位。 */
  path: string
  message: string
}

function isCompilableRegex(pattern: string): boolean {
  try {
    new RegExp(pattern)
    return true
  } catch {
    return false
  }
}

/** 校验通过返回空数组；否则返回每条问题及其字段路径，供表单内联展示。 */
export function validateRules(rules: StrategyRules): RuleValidationError[] {
  const errors: RuleValidationError[] = []
  const definerError = validateDDLDefinerFallback(rules.ddlPolicy.definerFallback)
  if (definerError) errors.push({ path: 'ddlPolicy.definerFallback', message: definerError })
  const mappingError = validateMappingRules(rules.mappings)
  if (mappingError) errors.push({ path: 'mappings', message: mappingError })
  const rateError = validateIncrementalRate(rules.incremental_rate_limit)
  if (rateError) errors.push({ path: 'incremental_rate_limit', message: rateError })
  const transformError = validateTransformRules(rules.transforms)
  if (transformError) errors.push({ path: 'transforms', message: transformError })

  // 对象过滤：逐条规则校验，镜像 internal/rule/compile.go compileObjectFilters
  // 的强制要求——action 必须已选择；schema_match/schema_pattern 永远必填
  // （每条规则都会走 compilePattern，不存在"只按库匹配就不用填库模式"这种
  // 省略）；regex 模式下的库/表模式都必须能编译；table_match/table_pattern
  // 必须同时填或同时留空，不能只填一个。
  rules.object_filters.forEach((rule, index) => {
    const path = `object_filters.${index}`
    if (rule.action === 'FILTER_ACTION_UNSPECIFIED') {
      errors.push({ path, message: `第 ${index + 1} 条对象过滤规则未选择「放行 / 排除」` })
    }
    if (rule.schema_match === 'MATCH_TYPE_UNSPECIFIED') {
      errors.push({ path, message: `第 ${index + 1} 条对象过滤规则未选择库名匹配方式` })
    } else if (!rule.schema_pattern.trim()) {
      errors.push({ path, message: `第 ${index + 1} 条对象过滤规则未填写库名匹配模式` })
    } else if (
      rule.schema_match === 'MATCH_TYPE_REGEX' &&
      !isCompilableRegex(rule.schema_pattern)
    ) {
      errors.push({
        path,
        message: `第 ${index + 1} 条对象过滤规则的库名正则「${rule.schema_pattern}」无法编译`,
      })
    }
    const hasTableMatch = rule.table_match !== 'MATCH_TYPE_UNSPECIFIED'
    const hasTablePattern = rule.table_pattern.trim() !== ''
    if (hasTableMatch !== hasTablePattern) {
      errors.push({
        path,
        message: `第 ${index + 1} 条对象过滤规则的表名匹配方式与匹配模式必须同时填写或同时留空`,
      })
    } else if (
      hasTableMatch &&
      rule.table_match === 'MATCH_TYPE_REGEX' &&
      !isCompilableRegex(rule.table_pattern)
    ) {
      errors.push({
        path,
        message: `第 ${index + 1} 条对象过滤规则的表名正则「${rule.table_pattern}」无法编译`,
      })
    }
  })

  if (rules.origin_policy.mode === 'ORIGIN_POLICY_MODE_UNSPECIFIED') {
    errors.push({ path: 'origin_policy', message: '来源策略未选择模式' })
  } else if (
    (rules.origin_policy.mode === 'ORIGIN_POLICY_MODE_INCLUDE_ORIGINS' ||
      rules.origin_policy.mode === 'ORIGIN_POLICY_MODE_EXCLUDE_ORIGINS') &&
    rules.origin_policy.centers.length === 0
  ) {
    errors.push({
      path: 'origin_policy',
      message: '选择「包含 / 排除指定来源」时必须至少填写一个来源中心',
    })
  } else if (
    (rules.origin_policy.mode === 'ORIGIN_POLICY_MODE_LOCAL_ONLY' ||
      rules.origin_policy.mode === 'ORIGIN_POLICY_MODE_ALL') &&
    rules.origin_policy.centers.length > 0
  ) {
    errors.push({
      path: 'origin_policy',
      message: '「仅本中心 / 全部来源」模式下不需要来源中心，请清空后再提交',
    })
  }
  if (rules.origin_policy.max_hops < 0) {
    errors.push({ path: 'origin_policy', message: '最大跳数不能为负数（0 表示不限）' })
  }

  // DDL 策略：仅同步指定中心时必须至少填一个来源中心。
  if (rules.ddlPolicy.mode === 'from_origins' && rules.ddlPolicy.fromOrigins.length === 0) {
    errors.push({
      path: 'ddlPolicy',
      message: '选择「仅同步来自指定中心的 DDL」时必须至少填写一个来源中心',
    })
  }

  if (rules.conflictPolicy.mode === 'last_write_wins') {
    const tables = rules.conflictPolicy.lwwTables ?? []
    const seen = new Set<string>()
    for (const table of tables) {
      const key = `${table.schema.toLowerCase()}\0${table.table.toLowerCase()}`
      if (
        !table.schema.trim() ||
        !table.table.trim() ||
        !table.timestampColumn.trim() ||
        seen.has(key)
      ) {
        errors.push({ path: 'conflictPolicy', message: 'LWW 表配置需要唯一的库、表和时间戳列' })
      }
      seen.add(key)
    }
  }

  const maskError = validateConflictMaskRules(rules.conflictPolicy.maskRules)
  if (maskError) errors.push({ path: 'conflictPolicy', message: maskError })

  // 错误策略：重试次数与退避不能是负数。
  if (rules.errorPolicy.maxRetries < 0) {
    errors.push({ path: 'errorPolicy', message: '重试次数不能为负数' })
  }
  if (rules.errorPolicy.backoffMs < 0) {
    errors.push({ path: 'errorPolicy', message: '退避时间不能为负数' })
  }

  // 全量参数：并发数、分块行数必须为正数，避免 0 并发导致全量卡死。
  if (rules.snapshotParams.readConcurrency < 1) {
    errors.push({ path: 'snapshotParams', message: '读取并发必须大于等于 1' })
  }
  if (rules.snapshotParams.chunkRows < 1) {
    errors.push({ path: 'snapshotParams', message: '分块行数必须大于等于 1' })
  }
  if (rules.snapshotParams.writeConcurrency < 1) {
    errors.push({ path: 'snapshotParams', message: '写入并发必须大于等于 1' })
  }

  // 写入参数：批大小、并行回放度必须为正数。
  if (rules.writeParams.batchSize < 1) {
    errors.push({ path: 'writeParams', message: '批大小必须大于等于 1' })
  }
  if (rules.writeParams.parallelism < 1) {
    errors.push({ path: 'writeParams', message: '并行回放度必须大于等于 1' })
  }

  return errors
}
