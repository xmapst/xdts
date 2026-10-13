import * as generated from './generated'
import type { Paginated } from './types'
import { isApiMockEnabled } from '@/mocks/config'
import * as conflictsMock from '@/mocks/conflictsMock'
import { keepPreviousData, useQuery } from '@tanstack/react-query'

export type ConflictType =
  | 'insert_duplicate'
  | 'update_missing'
  | 'delete_missing'
  | 'update_mismatch'
  | 'delete_mismatch'
  | 'lww_rejected'

export type ConflictResolution = 'source_wins' | 'target_wins' | 'skipped' | 'stopped'

export interface ConflictEntry {
  id: string
  ts: string
  pipelineId: string
  schemaName: string
  table: string
  pk: Record<string, unknown>
  conflictType: ConflictType
  resolution: ConflictResolution
  /** 字段不存在时省略（不是 null）。 */
  sourceRow?: Record<string, unknown>
  targetRow?: Record<string, unknown>
}

export interface ConflictQuery {
  page: number
  pageSize: number
  pipelineId?: string
  conflictType?: ConflictType
  resolution?: ConflictResolution
  /** RFC3339 字符串；纯可选过滤条件，省略不会被后端补默认窗口，见文件头注释。 */
  since?: string
  until?: string
}

export function listConflicts(query: ConflictQuery): Promise<Paginated<ConflictEntry>> {
  if (isApiMockEnabled()) {
    return conflictsMock.listConflicts(query)
  }
  return generated.listConflicts(query)
}

export const conflictsQueryKey = (query: ConflictQuery) => ['conflicts', 'list', query] as const

/** `placeholderData: keepPreviousData`：翻页 / 改筛选条件时保留上一页数据，理由同 `useLogsQuery`。 */
export function useConflictsQuery(query: ConflictQuery, enabled = true) {
  return useQuery({
    queryKey: conflictsQueryKey(query),
    queryFn: () => listConflicts(query),
    placeholderData: keepPreviousData,
    enabled,
  })
}
