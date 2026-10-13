// 这个端点从头就没有强制时间窗（对齐 `internal/manager/http/conflicts.go`
// 的落地实现，不是猜测）：since/until 全程可选、独立生效，不补默认窗口、
// 没有跨度上限，唯一校验是 until 不能早于 since。这是三个端点里最简单的
// 一个——本文件不引入 `resolveXxxTimeRange` 这类辅助函数，since/until 只是
// 两个普通的可选过滤条件。
import { ApiError } from '@/api/client'
import type { Paginated } from '@/api/types'
import type {
  ConflictEntry,
  ConflictQuery,
  ConflictResolution,
  ConflictType,
} from '@/api/conflicts'

// 深翻页保护：对齐 `internal/manager/http/conflicts.go` 的 maxConflictFetchRows。
const MAX_FETCH_ROWS = 5000

const DAY_MS = 24 * 60 * 60 * 1000

const CONFLICT_TYPES: ConflictType[] = [
  'insert_duplicate',
  'update_missing',
  'delete_missing',
  'update_mismatch',
  'delete_mismatch',
  'lww_rejected',
]
const RESOLUTIONS: ConflictResolution[] = ['source_wins', 'target_wins', 'skipped', 'stopped']
const PIPELINE_IDS = ['pl-1001', 'pl-1002']
const SCHEMAS = ['orders_db']
const TABLES = ['orders', 'order_items']

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function generateAll(): ConflictEntry[] {
  const base = Date.now()
  const total = 90
  const spanMs = 60 * DAY_MS
  const stepMs = Math.floor(spanMs / total)
  const entries: ConflictEntry[] = []
  for (let i = 0; i < total; i += 1) {
    const conflictType = CONFLICT_TYPES[i % CONFLICT_TYPES.length] ?? 'insert_duplicate'
    const resolution = RESOLUTIONS[i % RESOLUTIONS.length] ?? 'source_wins'
    const pipelineId = PIPELINE_IDS[i % PIPELINE_IDS.length] ?? 'pl-1001'
    const table = TABLES[i % TABLES.length] ?? 'orders'
    const pk = { id: 10000 + i }
    entries.push({
      id: `conflict-${i}`,
      ts: new Date(base - i * stepMs).toISOString(),
      pipelineId,
      schemaName: SCHEMAS[0] ?? 'orders_db',
      table,
      pk,
      conflictType,
      resolution,
      sourceRow: !conflictType.startsWith('delete_')
        ? { ...pk, updated_at: new Date(base - i * stepMs).toISOString() }
        : undefined,
      targetRow:
        resolution === 'target_wins' || conflictType.endsWith('_mismatch')
          ? { ...pk, updated_at: new Date(base - i * stepMs - 1000).toISOString() }
          : undefined,
    })
  }
  return entries
}

export async function listConflicts(query: ConflictQuery): Promise<Paginated<ConflictEntry>> {
  await delay(200)

  let since: Date | undefined
  let until: Date | undefined
  if (query.since) {
    since = new Date(query.since)
    if (Number.isNaN(since.getTime())) {
      throw new ApiError('since 格式不合法，需为 RFC3339', 400, 'invalid_time_range')
    }
  }
  if (query.until) {
    until = new Date(query.until)
    if (Number.isNaN(until.getTime())) {
      throw new ApiError('until 格式不合法，需为 RFC3339', 400, 'invalid_time_range')
    }
  }
  if (since && until && until.getTime() < since.getTime()) {
    throw new ApiError('until 不能早于 since', 400, 'invalid_time_range')
  }

  const fetchRows = query.page * query.pageSize
  if (fetchRows > MAX_FETCH_ROWS) {
    throw new ApiError('翻页跨度过大，请缩小时间范围或增加过滤条件后重试', 400, 'page_too_deep')
  }

  let entries = generateAll()
  if (since) {
    const sinceIso = since.toISOString()
    entries = entries.filter((entry) => entry.ts >= sinceIso)
  }
  if (until) {
    const untilIso = until.toISOString()
    entries = entries.filter((entry) => entry.ts <= untilIso)
  }
  entries.sort((a, b) => b.ts.localeCompare(a.ts))

  if (query.pipelineId) {
    entries = entries.filter((entry) => entry.pipelineId === query.pipelineId)
  }
  if (query.conflictType) {
    entries = entries.filter((entry) => entry.conflictType === query.conflictType)
  }
  if (query.resolution) {
    entries = entries.filter((entry) => entry.resolution === query.resolution)
  }

  const total = entries.length
  const start = (query.page - 1) * query.pageSize
  const items = entries.slice(start, start + query.pageSize)
  return { items, total, page: query.page, pageSize: query.pageSize }
}
