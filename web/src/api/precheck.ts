import { ApiError } from './client'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import i18n from '@/i18n'

export interface PrecheckItem {
  id: string
  item: string
  level: 'info' | 'warning' | 'blocking'
  status: 'pending' | 'pass' | 'fail'
  detail?: unknown
  confirmed: boolean
  confirmedAt: string | null
}
export interface PipelinePrecheck {
  batchId: string
  configVersion: number
  complete: boolean
  checkedAt: string
  expiresAt: string
  canStart: boolean
  reason?: string
  items: PrecheckItem[]
}
export const precheckKey = (id: string | undefined) => ['pipelines', 'precheck', id] as const

function exactResult(result: PipelinePrecheck): PipelinePrecheck {
  if (
    !result ||
    !Number.isSafeInteger(result.configVersion) ||
    result.configVersion < 0 ||
    !result.batchId ||
    typeof result.complete !== 'boolean' ||
    typeof result.canStart !== 'boolean' ||
    !Array.isArray(result.items) ||
    result.items.length > 100 ||
    !Number.isFinite(Date.parse(result.expiresAt)) ||
    !Number.isFinite(Date.parse(result.checkedAt)) ||
    result.items.some(
      (item) =>
        !item ||
        !item.id ||
        !item.item ||
        !['info', 'warning', 'blocking'].includes(item.level) ||
        !['pending', 'pass', 'fail'].includes(item.status) ||
        typeof item.confirmed !== 'boolean',
    ) ||
    new Set(result.items.map((item) => item.id)).size !== result.items.length
  ) {
    throw new ApiError(i18n.t('pipelines.precheck.invalidResult'), 422, 'invalid_precheck_result')
  }
  return result
}
export async function getPipelinePrecheck(
  id: string,
  signal?: AbortSignal,
): Promise<PipelinePrecheck | null> {
  // Development fixtures cannot produce actual database and federation proof.
  if (isApiMockEnabled()) return null
  try {
    return exactResult(await generated.getPipelinePrecheck(id, { signal }))
  } catch (error) {
    if (error instanceof ApiError && error.status === 404 && error.code === 'precheck_not_found')
      return null
    throw error
  }
}
export async function runPipelinePrecheck(id: string): Promise<PipelinePrecheck> {
  if (isApiMockEnabled())
    throw new ApiError(i18n.t('pipelines.precheck.mockUnavailable'), 503, 'precheck_unavailable')
  return exactResult(await generated.runPipelinePrecheck(id, {}))
}
export async function confirmPipelinePrecheck(
  id: string,
  batchId: string,
  items: string[],
): Promise<PipelinePrecheck> {
  if (
    !batchId ||
    items.length === 0 ||
    items.length > 100 ||
    new Set(items).size !== items.length
  ) {
    throw new ApiError(
      i18n.t('pipelines.precheck.selectWarnings'),
      422,
      'invalid_precheck_confirmation',
    )
  }
  if (isApiMockEnabled())
    throw new ApiError(i18n.t('pipelines.precheck.mockUnavailable'), 503, 'precheck_unavailable')
  return exactResult(await generated.confirmPipelinePrecheck(id, { batchId, items, confirm: true }))
}
