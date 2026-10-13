// `DEFAULT_LOG_WINDOW_HOURS = 24` 保留：这个值对应的是
// `defaultLogWindow`，后端把它保持为代码常量、明确不做成运维可配置项（见
// `internal/manager/http/logs.go` 的注释），和 `retention.logs` 是不同性质
// 的东西——一个是"用户没说时给的默认起点"（产品决策），一个是"数据能活
// 多久"（运维配置）。只要后端不改这个常量，前端保留同一份数值展示提示语
// 不算这次要修的那类缺陷。
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import * as logsMock from '@/mocks/logsMock'

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export type LogEntry = generated.GeneratedLogEntry

/** 见文件头注释：只对应后端仍然保留的代码常量 `defaultLogWindow`。 */
export const DEFAULT_LOG_WINDOW_HOURS = 24

export interface LogQuery {
  page: number
  pageSize: number
  nodeId?: string
  level?: LogLevel
  keyword?: string
  /** RFC3339 字符串；省略时由后端按文件头注释的缺省规则补齐。 */
  since?: string
  until?: string
}

/**
 * `GET /logs` 的响应体，对齐后端 `logListView`（见文件头注释：为什么不复用
 * `Paginated<T>`）。`since`/`until` 是**实际生效**的查询范围，`maxWindowSeconds`
 * 是**本次生效**的跨度上限（秒），界面必须显示前者、应该用后者做选择器层面
 * 的提前拦截。
 */
export type LogListResult = generated.GeneratedLogListResult

export function listLogs(query: LogQuery): Promise<LogListResult> {
  if (isApiMockEnabled()) {
    return logsMock.listLogs(query)
  }
  return generated.listLogs(query)
}

export const logsQueryKey = (query: LogQuery) => ['logs', 'list', query] as const

/**
 * `placeholderData: keepPreviousData`：翻页 / 改筛选条件时保留上一页数据
 * 直到新数据到达，避免表格在请求期间整体清空又重绘（体验同 `useNodesQuery`）。
 */
export function useLogsQuery(query: LogQuery, enabled = true) {
  return useQuery({
    queryKey: logsQueryKey(query),
    queryFn: () => listLogs(query),
    placeholderData: keepPreviousData,
    enabled,
  })
}
