import { ApiError } from './client'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import type { PageQuery, Paginated } from './types'
import i18n from '@/i18n'

export interface SnapshotTable {
  schema: string
  table: string
  estimatedRows: number
  doneRows: number
  status: 'pending' | 'reading' | 'done' | 'failed'
  totalChunks: number | null
  doneChunks: number | null
  rowsPerSec: number | null
  observedAt: string | null
}
export interface PipelineSnapshot {
  pipelineId: string
  runId: number
  snapshotUuid: string
  status: 'planning' | 'reading' | 'chunks_done' | 'converging' | 'converged' | 'paused' | 'failed'
  totalChunks: number
  doneChunks: number
  catchupRemainingTxns: number | null
  startedAt: string | null
  finishedAt: string | null
  updatedAt: string
  tables: Paginated<SnapshotTable>
}
export const snapshotKey = (id: string | undefined) => ['pipelines', 'snapshot', id] as const
const count = (v: number) => Number.isSafeInteger(v) && v >= 0
const date = (v: string | null) =>
  v === null || (typeof v === 'string' && Number.isFinite(Date.parse(v)))
export async function getPipelineSnapshot(
  id: string,
  page: PageQuery,
  signal?: AbortSignal,
): Promise<PipelineSnapshot | null> {
  if (isApiMockEnabled()) return null
  let result: PipelineSnapshot
  try {
    result = await generated.getPipelineSnapshot(id, page, { signal })
  } catch (error) {
    if (error instanceof ApiError && error.status === 404 && error.code === 'snapshot_not_found')
      return null
    throw error
  }
  if (
    !result ||
    !count(result.runId) ||
    !count(result.totalChunks) ||
    !count(result.doneChunks) ||
    result.doneChunks > result.totalChunks ||
    !result.pipelineId ||
    !result.snapshotUuid ||
    !['planning', 'reading', 'chunks_done', 'converging', 'converged', 'paused', 'failed'].includes(
      result.status,
    ) ||
    !date(result.startedAt) ||
    !date(result.finishedAt) ||
    typeof result.updatedAt !== 'string' ||
    !date(result.updatedAt) ||
    (result.catchupRemainingTxns !== null && !count(result.catchupRemainingTxns)) ||
    !result.tables ||
    !Array.isArray(result.tables.items) ||
    !count(result.tables.total) ||
    !count(result.tables.page) ||
    result.tables.page < 1 ||
    !count(result.tables.pageSize) ||
    result.tables.pageSize < 1 ||
    result.tables.pageSize > 200 ||
    result.tables.items.length > result.tables.pageSize ||
    result.tables.items.some(
      (table) =>
        !table ||
        !table.schema ||
        !table.table ||
        !count(table.estimatedRows) ||
        !count(table.doneRows) ||
        !['pending', 'reading', 'done', 'failed'].includes(table.status) ||
        !date(table.observedAt) ||
        (table.totalChunks === null) !== (table.doneChunks === null) ||
        (table.totalChunks !== null &&
          (!count(table.totalChunks) ||
            !count(table.doneChunks!) ||
            table.doneChunks! > table.totalChunks)) ||
        (table.rowsPerSec !== null && (!Number.isFinite(table.rowsPerSec) || table.rowsPerSec < 0)),
    )
  ) {
    throw new ApiError(i18n.t('pipelines.snapshot.invalidResult'), 422, 'invalid_snapshot_result')
  }
  return result
}
