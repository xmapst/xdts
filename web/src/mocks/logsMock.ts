// 时间范围缺省 / 校验逻辑原样照抄 `internal/manager/http/logs.go` 的
// `resolveLogTimeRange`/`effectiveLogMaxWindow`——这是真实后端已经落地的
// 实现，不是本文件自己的设计选择；后端如果调整这段逻辑，这里需要同步改，
// 否则 mock 行为会和真实契约漂移。
import { ApiError } from '@/api/client'
import type { LogEntry, LogLevel, LogListResult, LogQuery } from '@/api/logs'
import {
  MOCK_NODE_BJ_01,
  MOCK_NODE_BJ_02,
  MOCK_NODE_MGR_01,
  MOCK_NODE_SH_01,
  MOCK_NODE_SH_02,
} from './seedIds'

const LEVELS: LogLevel[] = ['debug', 'info', 'info', 'info', 'warn', 'error']
const MODULES = ['controller', 'store', 'reporter', 'probe', 'pipeline']

const HOUR_MS = 60 * 60 * 1000
const DAY_MS = 24 * HOUR_MS
const DEFAULT_WINDOW_MS = 24 * HOUR_MS
// 深翻页保护：对齐 `internal/manager/http/logs.go` 的 maxLogFetchRows。
const MAX_FETCH_ROWS = 5000

const LOG_MAX_WINDOW_COST_CAP_MS = 15 * DAY_MS

/**
 * 模拟运维配置的 `retention.logs`：真实值来自后端配置，前端编译期不可能
 * 知道。这里特意选一个**小于**成本硬上限的值（而不是让它恰好等于或大于
 * 15 天）——如果 mock 的"生效上限"永远停在 15 天，前端就算偷懒把
 * `maxWindowSeconds` 硬编码回 15 也测不出区别；选一个更小的数字，才能让
 * "前端有没有真的读响应里的 maxWindowSeconds"这件事在开发环境里可观察。
 */
const MOCK_LOG_RETENTION_MS = 10 * DAY_MS

export function effectiveLogMaxWindow(retentionMs: number): { windowMs: number; reason: string } {
  if (retentionMs > 0 && retentionMs < LOG_MAX_WINDOW_COST_CAP_MS) {
    return {
      windowMs: retentionMs,
      reason: 'retention.logs 配置的日志保留期——超过这个跨度的日志已经不存在，不是被限制',
    }
  }
  // 刻意不在这句里出现"保留期"三个字，理由同后端注释：运维排查时通常搜
  // 关键字定位错误类别，避免被带去改一个不起作用的配置项。
  return {
    windowMs: LOG_MAX_WINDOW_COST_CAP_MS,
    reason: '查询成本上限（避免一次扫描过多分区拖慢管理库，跟日志能存多久无关）',
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * 按节点 ID 生成一份确定性的演示日志，铺在「最近 9 天」内（留在
 * `MOCK_LOG_RETENTION_MS`=10 天以内，同时比默认 24 小时窗口宽得多）——这样
 * 用户在 LogsPage 里切换「最近 24 小时」「自定义最近几天」等不同范围时能
 * 看到不同的结果集，真正演示到时间范围过滤这个功能本身，而不是无论怎么选
 * 都是同一批数据。span 必须小于当前生效上限，否则「用一个覆盖它的显式范围
 * 才能搜到」这个演示场景会反而先撞上 400。
 */
function generateForNode(nodeId: string): LogEntry[] {
  const base = Date.now()
  const entriesPerNode = 200
  const spanMs = 9 * DAY_MS
  const stepMs = Math.floor(spanMs / entriesPerNode)
  const entries: LogEntry[] = []
  for (let i = 0; i < entriesPerNode; i += 1) {
    const level = LEVELS[i % LEVELS.length] ?? 'info'
    const moduleName = MODULES[i % MODULES.length] ?? 'controller'
    entries.push({
      id: `${nodeId}-log-${i}`,
      ts: new Date(base - i * stepMs).toISOString(),
      level,
      module: moduleName,
      message: `[${moduleName}] 演示日志条目 #${i}（node=${nodeId}）`,
      nodeId,
      pipelineId: i % 3 === 0 ? 'pl-1001' : null,
    })
  }
  return entries
}

const ALL_NODE_IDS = [
  MOCK_NODE_MGR_01,
  MOCK_NODE_BJ_01,
  MOCK_NODE_BJ_02,
  MOCK_NODE_SH_01,
  MOCK_NODE_SH_02,
]

/* Mirror server range defaults, bound validation and the retention-derived maximum span. */
function resolveLogTimeRange(
  rawSince: string | undefined,
  rawUntil: string | undefined,
  now: Date,
) {
  let since: Date | undefined
  let until: Date | undefined

  if (rawSince) {
    since = new Date(rawSince)
    if (Number.isNaN(since.getTime())) {
      throw new ApiError('since 格式不合法，需为 RFC3339', 400, 'invalid_time_range')
    }
  }
  if (rawUntil) {
    until = new Date(rawUntil)
    if (Number.isNaN(until.getTime())) {
      throw new ApiError('until 格式不合法，需为 RFC3339', 400, 'invalid_time_range')
    }
  }

  if (!since && !until) {
    until = now
    since = new Date(now.getTime() - DEFAULT_WINDOW_MS)
  } else if (since && !until) {
    until = new Date(since.getTime() + DEFAULT_WINDOW_MS)
    if (until.getTime() > now.getTime()) {
      until = now
    }
  } else if (!since && until) {
    since = new Date(until.getTime() - DEFAULT_WINDOW_MS)
  }

  // 上面四个分支已经覆盖全部组合，since/until 此时必然都已赋值；这两个
  // 判断只是让 TypeScript 的控制流分析确认非空，不代表还有第五种情况。
  if (!since || !until) {
    throw new ApiError('时间范围解析失败', 400, 'invalid_time_range')
  }

  if (until.getTime() < since.getTime()) {
    throw new ApiError('until 不能早于 since', 400, 'invalid_time_range')
  }

  const { windowMs, reason } = effectiveLogMaxWindow(MOCK_LOG_RETENTION_MS)
  if (until.getTime() - since.getTime() > windowMs) {
    const days = Math.floor(windowMs / DAY_MS)
    throw new ApiError(`时间范围不能超过 ${days} 天（${reason}）`, 400, 'invalid_time_range')
  }

  return { since, until, maxWindowSeconds: Math.floor(windowMs / 1000) }
}

export async function listLogs(query: LogQuery): Promise<LogListResult> {
  await delay(200)

  const { since, until, maxWindowSeconds } = resolveLogTimeRange(
    query.since,
    query.until,
    new Date(),
  )

  const fetchRows = query.page * query.pageSize
  if (fetchRows > MAX_FETCH_ROWS) {
    throw new ApiError('翻页跨度过大，请缩小时间范围或增加过滤条件后重试', 400, 'page_too_deep')
  }

  const nodeIds = query.nodeId ? [query.nodeId] : ALL_NODE_IDS
  let entries = nodeIds.flatMap((id) => generateForNode(id))

  const sinceIso = since.toISOString()
  const untilIso = until.toISOString()
  entries = entries.filter((entry) => entry.ts >= sinceIso && entry.ts <= untilIso)
  entries.sort((a, b) => b.ts.localeCompare(a.ts))

  if (query.level) {
    entries = entries.filter((entry) => entry.level === query.level)
  }
  if (query.keyword) {
    const keyword = query.keyword.trim().toLowerCase()
    if (keyword.length > 0) {
      entries = entries.filter((entry) => entry.message.toLowerCase().includes(keyword))
    }
  }

  const total = entries.length
  const start = (query.page - 1) * query.pageSize
  const items = entries.slice(start, start + query.pageSize)
  return {
    items,
    total,
    page: query.page,
    pageSize: query.pageSize,
    since: sinceIso,
    until: untilIso,
    maxWindowSeconds,
  }
}
