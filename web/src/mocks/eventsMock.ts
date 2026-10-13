// Event queries default to 24 hours without a span cap. Import shared event constants directly to avoid an API/mock cycle.
import { GOLDEN_EVENT_TYPES } from '@/api/eventTypesGolden'
import { ApiError } from '@/api/client'
import type { EventEntry, EventLevel, EventListResult, EventQuery, EventType } from '@/api/events'
import {
  MOCK_NODE_BJ_01,
  MOCK_NODE_BJ_02,
  MOCK_NODE_MGR_01,
  MOCK_NODE_SH_01,
  MOCK_NODE_SH_02,
} from './seedIds'

const HOUR_MS = 60 * 60 * 1000
const DAY_MS = 24 * HOUR_MS
const DEFAULT_WINDOW_MS = 24 * HOUR_MS
// 深翻页保护：对齐 `internal/manager/http/events.go` 的 maxEventFetchRows。
const MAX_FETCH_ROWS = 5000

const MOCK_ACKED_BY = 'user-admin-01'

// type 与"是否挂 pipelineId"的对应关系是本文件的演示选择，不是后端强制的
// 规则（后端 EventQuery 允许任意 type 搭配任意 pipelineId/nil）；只是让
// 演示数据看起来更像真实场景：DDL / 冲突 / 背压 / 本地存储重建都是
// pipeline 范围内的事件，节点离线 / 探测失败是节点范围的事件。
const PIPELINE_SCOPED_TYPES: EventType[] = [
  'ddl',
  'conflict',
  'backpressure',
  'local_store_rebuild',
]
// 演示事件的类型取自后端金样（web/src/api/events.ts 的 EVENT_TYPES ←
// internal/manager/model/event_types.golden.json），不在本文件再抄第三份。
// 好处是后端新增取值后，本地 dev 立刻能看到那种事件——否则新类型在前端
// 开发期**根本不存在**，而它在生产里存在。
//
// `as EventType[]` 是这份 mock 里唯一的收窄点：金样元素的静态类型是 string
// （JSON 导入推不出字面量），而两者取值一致由 web/src/api/events.golden.test.ts 钉住。
const MOCK_EVENT_TYPES = GOLDEN_EVENT_TYPES as EventType[]
const EVENT_LEVELS: EventLevel[] = ['info', 'info', 'warning', 'error']

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * 按节点 ID 生成一份确定性的演示事件，铺在「最近 40 天」内——刻意选一个
 * 明显超过 `/logs` 那条 15 天成本上限的跨度，用来在开发环境里演示
 * "`/events` 真的没有跨度上限，一次查 40 天不会被拒绝"这件事本身，而不是
 * 恰好在演示数据规模上也悄悄维持了一个跟 `/logs` 一样的边界。
 */
function generateForNode(nodeId: string, base: number): EventEntry[] {
  const entriesPerNode = 120
  const spanMs = 40 * DAY_MS
  const stepMs = Math.floor(spanMs / entriesPerNode)
  const entries: EventEntry[] = []
  for (let i = 0; i < entriesPerNode; i += 1) {
    const type = MOCK_EVENT_TYPES[i % MOCK_EVENT_TYPES.length] ?? 'error'
    const level = EVENT_LEVELS[i % EVENT_LEVELS.length] ?? 'info'
    // 一半一半（不是 i % 4，见 web/src/features/logs/EventsPage.test.tsx 的教训）：默认 24 小时
    // 窗口在当前 stepMs 下每个节点只落几条演示事件，"每 4 条才有 1 条已
    // 确认"在这么小的样本里很容易因为窗口边界的几毫秒误差而一条已确认的
    // 都不落在窗口内，让"未勾选 onlyUnacked 时两种状态都该出现"这类测试
    // 变成看运气；一半一半在任意 >=2 条的样本里都能稳定同时出现两种状态。
    const acked = i % 2 === 0
    const pipelineId = PIPELINE_SCOPED_TYPES.includes(type) ? 'pl-1001' : null
    let payload: Record<string, unknown> | undefined
    if (type === 'ddl') {
      payload = { sql: 'ALTER TABLE orders ADD COLUMN remark VARCHAR(255)' }
    } else if (type === 'node_offline') {
      payload = { lastHeartbeatAgoSeconds: 90 }
    }
    entries.push({
      id: `${nodeId}-event-${i}`,
      ts: new Date(base - i * stepMs).toISOString(),
      type,
      level,
      nodeId,
      pipelineId,
      ackedBy: acked ? MOCK_ACKED_BY : null,
      ackedAt:
        acked
          ? new Date(Math.min(base, base - i * stepMs + 60_000)).toISOString()
          : null,
      payload,
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

/**
 * 原样照抄 `internal/manager/http/time_range.go` 的 `resolveTimeRangeQuery`
 * 在 `maxWindow<=0` 分支下的行为（internal/manager/http/events.go 调用它时传的就是 0）：只补默认
 * 窗口，不设跨度上限——都不给取最近 24 小时；只给一边补另一边；都给原样
 * 使用；唯一的校验是 until 不能早于 since。
 */
function resolveEventTimeRange(
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

  if (!since || !until) {
    throw new ApiError('时间范围解析失败', 400, 'invalid_time_range')
  }
  if (until.getTime() < since.getTime()) {
    throw new ApiError('until 不能早于 since', 400, 'invalid_time_range')
  }
  // 刻意没有跨度上限校验——见文件头注释，这不是遗漏。

  return { since, until }
}

export async function listEvents(query: EventQuery): Promise<EventListResult> {
  await delay(200)

  const { since, until } = resolveEventTimeRange(query.since, query.until, new Date())

  const fetchRows = query.page * query.pageSize
  if (fetchRows > MAX_FETCH_ROWS) {
    throw new ApiError('翻页跨度过大，请缩小时间范围或增加过滤条件后重试', 400, 'page_too_deep')
  }

  const nodeIds = query.nodeId ? [query.nodeId] : ALL_NODE_IDS
  // **覆盖必须套在过滤之前**：`onlyUnacked` 在下面按 `ackedBy === null` 筛，
  // 套晚了的话刚确认过的行在"仅未确认"视图里不会消失，而那正是确认后最该发生的事。
  let entries = nodeIds.flatMap((id) => generateForNode(id, until.getTime())).map(applyAckOverride)

  const sinceIso = since.toISOString()
  const untilIso = until.toISOString()
  entries = entries.filter((entry) => entry.ts >= sinceIso && entry.ts <= untilIso)
  entries.sort((a, b) => b.ts.localeCompare(a.ts))

  if (query.pipelineId) {
    entries = entries.filter((entry) => entry.pipelineId === query.pipelineId)
  }
  if (query.type) {
    entries = entries.filter((entry) => entry.type === query.type)
  }
  if (query.level) {
    entries = entries.filter((entry) => entry.level === query.level)
  }
  if (query.onlyUnacked) {
    entries = entries.filter((entry) => entry.ackedBy === null)
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
  }
}

/**
 * 本会话内已确认的事件 id → {确认人, 确认时间}。
 *
 * mock 的事件是**生成式**的（`generateForNode` 每次按 `until` 重算），没有可写的
 * 底库。要让"确认"这个动作在界面上留下痕迹，只能另存一层覆盖。
 */
const ackOverrides = new Map<string, { by: string; at: string }>()

/* This fixture simulates another user acknowledging an event after the list was loaded, producing 409. */
const CONCURRENTLY_ACKED_ID = `${MOCK_NODE_MGR_01}-event-1`
const OTHER_ACK_USER = 'user-ops-02'

function applyAckOverride(entry: EventEntry): EventEntry {
  const ov = ackOverrides.get(entry.id)
  if (ov === undefined) {
    return entry
  }
  return { ...entry, ackedBy: ov.by, ackedAt: ov.at }
}

/* 确认一条事件（`POST /events/:id/ack` 的 mock）。

后端先 `UPDATE … WHERE id=? AND acked_by IS NULL`；CAS 未命中时再读一次消歧：

- 记录不存在                 ⇒ 404
- `acked_by` 就是自己    ⇒ 幂等返回成功（200）
- `acked_by` 是别人          ⇒ 409 `event_already_acked` */
export async function ackEvent(id: string): Promise<void> {
  await delay(120)
  const existing = ackOverrides.get(id)
  if (existing !== undefined) {
    if (existing.by === MOCK_ACKED_BY) {
      return // 自己重复确认：幂等成功，与后端一致
    }
    throw new ApiError('事件已被他人确认', 409, 'event_already_acked')
  }
  if (id === CONCURRENTLY_ACKED_ID) {
    // 先落他人的确认再抛 409：刷新后这一行会显示成已确认，
    // 这正是"已被他人确认，列表已刷新"那句提示所承诺的。
    ackOverrides.set(id, { by: OTHER_ACK_USER, at: new Date().toISOString() })
    throw new ApiError('事件已被他人确认', 409, 'event_already_acked')
  }
  ackOverrides.set(id, { by: MOCK_ACKED_BY, at: new Date().toISOString() })
}

/** 供测试引用的并发确认种子 id（见 CONCURRENTLY_ACKED_ID 的注释）。 */
export const MOCK_CONCURRENTLY_ACKED_EVENT_ID = CONCURRENTLY_ACKED_ID

/** 仅供测试重置本模块的会话态，生产代码不要调用。 */
export function __resetAckOverridesForTest(): void {
  ackOverrides.clear()
}

/**
 * 本 mock 域镜像的后端写入方（`web/src/mocks/backendWriteParity.test.ts` 核对）。
 *
 * `AckEvent` 由 `internal/manager/http/events.go` 的 `eventHandler.ack` 调用，是活的。
 */
export const backendWriteParity = {
  liveRepoWrites: ['AckEvent'],
  note: '确认事件；后端 POST /events/:id/ack 2026-09-30 实测已注册',
} as const
