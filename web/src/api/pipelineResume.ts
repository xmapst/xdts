import i18n from '@/i18n'
import { ApiError } from './client'
import { getPipelinePrecheck, runPipelinePrecheck } from './precheck'

export function isPipelinePrecheckError(error: unknown): error is ApiError {
  return error instanceof ApiError && !!error.code?.startsWith('precheck_')
}

export async function authorizePipelineResume(id: string): Promise<void> {
  let result = await getPipelinePrecheck(id)
  if (
    !result ||
    result.reason === 'precheck_changed' ||
    result.reason === 'precheck_expired' ||
    Date.parse(result.expiresAt) <= Date.now()
  ) {
    result = await runPipelinePrecheck(id)
  }
  if (!result.canStart || !result.complete || Date.parse(result.expiresAt) <= Date.now()) {
    const reason = result.reason || 'precheck_incomplete'
    throw new ApiError(
      i18n.t(`pipelines.precheck.reasons.${reason}`, {
        defaultValue: i18n.t('pipelines.precheck.unavailable'),
      }),
      409,
      reason,
      undefined,
      result,
    )
  }
}
