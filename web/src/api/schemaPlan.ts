import { ApiError } from './client'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import i18n from '@/i18n'
import type { SchemaPlanStep } from './reinit'

export interface SchemaPlanPreviewInput {
  schemas: string[]
  definerFallback?: string
}
export interface SchemaPlanRecord {
  pipelineId: string
  runId: number
  configVersion: number
  digest: string
  confirmedAt: string | null
  plan: { steps: SchemaPlanStep[]; blocked: boolean }
}
export const schemaPlanKey = (id: string | undefined) => ['pipelines', 'schema-plan', id] as const
function exactPlan(record: SchemaPlanRecord): SchemaPlanRecord {
  if (
    !record ||
    !record.pipelineId ||
    ![record.runId, record.configVersion].every((v) => Number.isSafeInteger(v) && v >= 0) ||
    !/^[a-f0-9]{64}$/.test(record.digest) ||
    (record.confirmedAt !== null &&
      (typeof record.confirmedAt !== 'string' ||
        !Number.isFinite(Date.parse(record.confirmedAt)))) ||
    !record.plan ||
    typeof record.plan.blocked !== 'boolean' ||
    !Array.isArray(record.plan.steps) ||
    record.plan.steps.length > 4000 ||
    record.plan.steps.some(
      (step, index) =>
        !step ||
        step.seq !== index + 1 ||
        ![
          'schema',
          'table',
          'view',
          'procedure',
          'function',
          'foreign_key',
          'trigger',
          'event',
        ].includes(step.kind) ||
        !['create', 'alter', 'drop', 'rebuild', 'skip', 'block'].includes(step.action) ||
        typeof step.ddl !== 'string' ||
        typeof step.sourceDdl !== 'string' ||
        typeof step.afterSnapshot !== 'boolean' ||
        !step.schema ||
        !step.name ||
        (step.differences !== null &&
          (!Array.isArray(step.differences) ||
            step.differences.some(
              (diff) =>
                !diff ||
                typeof diff.path !== 'string' ||
                typeof diff.expected !== 'string' ||
                typeof diff.actual !== 'string',
            ))),
    )
  ) {
    throw new ApiError(i18n.t('pipelines.schemaPlan.invalidResult'), 422, 'invalid_schema_plan')
  }
  return record
}
export async function getSchemaPlan(
  id: string,
  signal?: AbortSignal,
): Promise<SchemaPlanRecord | null> {
  if (isApiMockEnabled()) return null
  try {
    return exactPlan(await generated.getSchemaPlan(id, { signal }))
  } catch (error) {
    if (error instanceof ApiError && error.status === 404 && error.code === 'plan_not_found')
      return null
    throw error
  }
}
export async function previewSchemaPlan(
  id: string,
  input: SchemaPlanPreviewInput,
): Promise<SchemaPlanRecord> {
  if (isApiMockEnabled())
    throw new ApiError(
      i18n.t('pipelines.schemaPlan.mockUnavailable'),
      503,
      'schema_plan_unavailable',
    )
  return exactPlan(await generated.previewSchemaPlan(id, input))
}
export async function confirmSchemaPlan(id: string, digest: string): Promise<SchemaPlanRecord> {
  if (isApiMockEnabled())
    throw new ApiError(
      i18n.t('pipelines.schemaPlan.mockUnavailable'),
      503,
      'schema_plan_unavailable',
    )
  if (!/^[a-f0-9]{64}$/.test(digest))
    throw new ApiError(i18n.t('pipelines.schemaPlan.invalidResult'), 422, 'invalid_schema_plan')
  return exactPlan(await generated.confirmSchemaPlan(id, { digest, confirm: true }))
}
