import { ApiError } from '@/api/client'
import type { AlertEntry, AlertQuery, AlertRule, AlertRuleInput } from '@/api/alerts'
import type { PageQuery, Paginated } from '@/api/types'
import { fetchMe } from './authMock'

export const backendWriteParity = {
  liveRepoWrites: ['CreateAlertRule', 'UpdateAlertRule', 'DeleteAlertRule'],
} as const

let rules: AlertRule[] = []
let entries: AlertEntry[] = []
const webhookByID = new Map<string, string>()
const dimensions = [
  'node_offline',
  'latency_exceeded',
  'pipeline_error',
  'queue_backlog',
  'federation_link_down',
  'unregistered_origin_uuid',
  'local_store_abnormal',
]

function page<T>(items: T[], query: PageQuery): Paginated<T> {
  const p = Math.max(1, query.page)
  const n = Math.min(200, Math.max(1, query.pageSize))
  return {
    items: structuredClone(items.slice((p - 1) * n, p * n)),
    total: items.length,
    page: p,
    pageSize: n,
  }
}
async function admin() {
  if ((await fetchMe()).user?.role !== 'admin') throw new ApiError('无权限', 403, 'forbidden')
}
function validate(input: AlertRuleInput, except?: string) {
  if (
    !input.name.trim() ||
    input.name.length > 128 ||
    !dimensions.includes(input.dimension) ||
    !Number.isSafeInteger(input.threshold.value) ||
    input.threshold.value < 0 ||
    !Number.isInteger(input.threshold.windowSeconds) ||
    input.threshold.windowSeconds < 1 ||
    input.threshold.windowSeconds > 86400 ||
    !Number.isInteger(input.suppressSeconds) ||
    input.suppressSeconds < 0 ||
    input.suppressSeconds > 86400
  )
    throw new ApiError('告警规则参数不合法', 400, 'invalid_alert_rule')
  if (
    rules.some((r) => r.id !== except && r.name.toLowerCase() === input.name.trim().toLowerCase())
  )
    throw new ApiError('告警规则名称已存在', 409, 'alert_rule_name_conflict')
  if (input.webhookUrl) {
    let url: URL
    try {
      url = new URL(input.webhookUrl)
    } catch {
      throw new ApiError('Webhook地址不合法', 400, 'invalid_alert_rule')
    }
    if (
      !['http:', 'https:'].includes(url.protocol) ||
      url.username ||
      url.password ||
      url.hash ||
      input.webhookUrl.length > 255
    )
      throw new ApiError('Webhook地址不合法', 400, 'invalid_alert_rule')
  }
}
export async function listAlertRules(query: PageQuery) {
  return page(rules, query)
}
export async function listAlerts(query: AlertQuery) {
  return page(
    entries.filter(
      (a) =>
        (!query.ruleId || a.ruleId === query.ruleId) &&
        (!query.status || a.status === query.status) &&
        (!query.level || a.level === query.level) &&
        (!query.since || a.firedAt >= query.since) &&
        (!query.until || a.firedAt <= query.until),
    ),
    query,
  )
}
export async function createAlertRule(input: AlertRuleInput) {
  await admin()
  validate(input)
  const rule: AlertRule = {
    id: crypto.randomUUID(),
    name: input.name.trim(),
    dimension: input.dimension,
    enabled: input.enabled,
    threshold: structuredClone(input.threshold),
    suppressSeconds: input.suppressSeconds,
    webhookConfigured: Boolean(input.webhookUrl),
    version: 1,
    createdAt: new Date().toISOString(),
  }
  if (input.webhookUrl) webhookByID.set(rule.id, input.webhookUrl)
  rules = [rule, ...rules]
  return structuredClone(rule)
}
export async function updateAlertRule(id: string, input: AlertRuleInput & { version: number }) {
  await admin()
  const old = rules.find((r) => r.id === id)
  if (!old) throw new ApiError('告警规则不存在', 404, 'not_found')
  if (old.version !== input.version)
    throw new ApiError('规则已修改，请刷新后重试', 409, 'alert_rule_version_conflict')
  validate(input, id)
  if (input.webhookUrl !== undefined) {
    if (input.webhookUrl) webhookByID.set(id, input.webhookUrl)
    else webhookByID.delete(id)
  }
  const rule = {
    ...old,
    name: input.name.trim(),
    dimension: input.dimension,
    enabled: input.enabled,
    threshold: structuredClone(input.threshold),
    suppressSeconds: input.suppressSeconds,
    webhookConfigured: webhookByID.has(id),
    version: old.version + 1,
  }
  rules = rules.map((r) => (r.id === id ? rule : r))
  if (!rule.enabled || old.dimension !== rule.dimension)
    entries = entries.map((a) =>
      a.ruleId === id && a.status === 'firing'
        ? { ...a, status: 'resolved', resolvedAt: new Date().toISOString() }
        : a,
    )
  return structuredClone(rule)
}
export async function deleteAlertRule(id: string, version: number) {
  await admin()
  const rule = rules.find((r) => r.id === id)
  if (!rule) throw new ApiError('告警规则不存在', 404, 'not_found')
  if (rule.version !== version)
    throw new ApiError('规则已修改，请刷新后重试', 409, 'alert_rule_version_conflict')
  rules = rules.filter((r) => r.id !== id)
  webhookByID.delete(id)
  entries = entries.map((a) =>
    a.ruleId === id && a.status === 'firing'
      ? { ...a, status: 'resolved', resolvedAt: new Date().toISOString() }
      : a,
  )
}
