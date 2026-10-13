import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { GOLDEN_EVENT_TYPES } from './eventTypesGolden'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import * as eventsMock from '@/mocks/eventsMock'

/* 后端 `EventType` 的取值全集，直接取自金样
`internal/manager/model/event_types.golden.json`。

前端这边直接用它，不再各自抄一份：类型筛选下拉框（EventsPage）与开发期
mock 的事件生成都从这里取。这一条解决的是三处手抄里最危险的那一处——
下拉框漏掉新类型时，按类型筛选会让那些事件整条消失，且界面不作任何说明
（渲染层至少还显示了那一行）。 */
export const EVENT_TYPES: readonly string[] = GOLDEN_EVENT_TYPES

export type EventType =
  | 'error'
  | 'ddl'
  | 'conflict'
  | 'unknown_origin'
  | 'backpressure'
  | 'local_store_rebuild'
  | 'link_probe_failed'
  | 'node_offline'
  | 'config_applied'

/**
 * union 成员的运行时清单，用 `satisfies Record<EventType, true>` 强制穷举——
 * 少写一个成员编译就红，所以它不可能偷偷落后于 union 本身。
 *
 * 它存在的唯一目的是让 union 能和金样在运行时比对（类型没法在运行时反射）。
 */
export const EVENT_TYPE_UNION_MEMBERS: readonly string[] = Object.keys({
  error: true,
  ddl: true,
  conflict: true,
  unknown_origin: true,
  backpressure: true,
  local_store_rebuild: true,
  link_probe_failed: true,
  node_offline: true,
  config_applied: true,
} satisfies Record<EventType, true>)

export type EventLevel = 'info' | 'warning' | 'error'

export interface EventEntry {
  id: string
  ts: string
  type: EventType
  level: EventLevel
  nodeId: string | null
  pipelineId: string | null

  ackedBy: string | null
  /* Migration v43 leaves acked_at null on previously acknowledged events. */
  ackedAt: string | null
  /** 结构随 type 变化，字段不存在时省略（不是 null）。 */
  payload?: Record<string, unknown>
}

export interface EventQuery {
  page: number
  pageSize: number
  nodeId?: string
  pipelineId?: string
  type?: EventType
  level?: EventLevel
  onlyUnacked?: boolean
  /** RFC3339 字符串；省略时由后端补最近 24 小时，跨度没有上限。 */
  since?: string
  until?: string
}

/**
 * `GET /events` 的响应体，形状对齐 `LogListResult`（同样不复用 `Paginated<T>`，
 * 因为要携带实际生效的 since/until），但**没有 `maxWindowSeconds`**——这个
 * 端点没有跨度上限，见文件头注释第 1 点。
 */
export type EventListResult = generated.GeneratedEventListResult

export function listEvents(query: EventQuery): Promise<EventListResult> {
  if (isApiMockEnabled()) {
    return eventsMock.listEvents(query)
  }
  return generated.listEvents(query)
}

export const eventsQueryKey = (query: EventQuery) => ['events', 'list', query] as const

/** `placeholderData: keepPreviousData`：翻页 / 改筛选条件时保留上一页数据，理由同 `useLogsQuery`。 */
export function useEventsQuery(query: EventQuery, enabled = true) {
  return useQuery({
    queryKey: eventsQueryKey(query),
    queryFn: () => listEvents(query),
    placeholderData: keepPreviousData,
    enabled,
  })
}

/* 确认一条事件。

- `POST /api/v1/events/:id/ack`，无请求体——确认人取自会话，前端不传也不该传。
- 成功 `200` + 空对象 `{}`。
- `400 invalid_id` / `404 not_found` / `409 event_already_acked` /
  `500 ack_event_no_authenticated_user`、`ack_event_zero_user_id`、`ack_event_failed`。
- CSRF 由 `apiClient` 统一附带（非幂等方法自动加双提交头），此处不另做。
- 只读角色会被 `/api/v1` 组上的只读闸门拦下，前端不预判角色、由后端拒绝。 */
export function ackEvent(id: string): Promise<void> {
  if (isApiMockEnabled()) {
    return eventsMock.ackEvent(id)
  }
  return generated.ackEvent(id).then(() => undefined)
}

/**
 * `meta.suppressGlobalError`：**409 是本流程的正常分支之一**（他人已确认），
 * 要由页面按语义处理，不能让全局错误提示把它当成失败弹出来。
 */
export function useAckEventMutation() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ackEvent,
    meta: { suppressGlobalError: true },

    onSettled: () => qc.invalidateQueries({ queryKey: ['events'] }),
  })
}
