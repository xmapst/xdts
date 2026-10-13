import { ApiError } from './client'
import * as generated from './generated'
import i18n from '@/i18n'

export type ReinitTargetHandling = 'keep' | 'clear'
export interface ReinitPreviewInput {
  schemas: string[]
  targetHandling: ReinitTargetHandling
  schemaMigration: boolean
  definerFallback?: string
}
export interface ReinitTable {
  sourceSchema: string
  sourceTable: string
  sourceDdl?: string
  schema: string
  table: string
  expectedDdl: string
}
export type SchemaObjectKind =
  | 'schema'
  | 'table'
  | 'view'
  | 'procedure'
  | 'function'
  | 'foreign_key'
  | 'trigger'
  | 'event'
export type SchemaMigrationAction = 'create' | 'alter' | 'drop' | 'rebuild' | 'skip' | 'block'
export interface SchemaPlanStep {
  seq: number
  kind: SchemaObjectKind
  sourceSchema: string
  sourceName: string
  schema: string
  name: string
  action: SchemaMigrationAction
  sourceDdl: string
  ddl: string
  differences: { path: string; expected: string; actual: string }[] | null
  blockReason?: string
  needsDelimiter: boolean
  afterSnapshot: boolean
  tableName?: string
  viewPlaceholder?: boolean
  viewReplacementSeq?: number
  viewReplacementDdl?: string
  viewPlaceholderSeq?: number
  viewPlaceholderDdl?: string
}
export interface ReinitRecord {
  pipelineId: string
  fromRunId: number
  configVersion: number
  digest: string
  document: {
    inputsDigest?: string
    schemas: string[]
    targetHandling: ReinitTargetHandling
    targetCaseSensitive: boolean
    tables: ReinitTable[]
    schemaPlan?: { steps: SchemaPlanStep[]; blocked: boolean }
  }
  confirmedAt: string | null
  newRunId: number
  snapshotUuid: string
  schemaUuid: string
  prepareUuid: string
  cmdSeq: number
  sourceApplied: boolean
  targetApplied: boolean
}

function exactIdentity(record: ReinitRecord): ReinitRecord {
  // JSON numbers above the JS integer limit cannot be sent back faithfully.
  if (
    ![record.fromRunId, record.configVersion, record.newRunId, record.cmdSeq].every(
      (value) => Number.isSafeInteger(value) && value >= 0,
    )
  )
    throw new ApiError(i18n.t('pipelines.reinit.invalidIdentity'), 422, 'invalid_reinit_identity')
  return record
}
export async function getReinit(id: string, signal?: AbortSignal): Promise<ReinitRecord | null> {
  try {
    return exactIdentity(await generated.getReinit(id, { signal }))
  } catch (error) {
    if (error instanceof ApiError && error.status === 404 && error.code === 'reinit_not_found')
      return null
    throw error
  }
}
export async function previewReinit(id: string, input: ReinitPreviewInput): Promise<ReinitRecord> {
  return exactIdentity(await generated.previewReinit(id, input))
}
export async function confirmReinit(
  id: string,
  record: Pick<ReinitRecord, 'fromRunId' | 'digest'>,
): Promise<ReinitRecord> {
  if (!Number.isSafeInteger(record.fromRunId) || record.fromRunId < 0) {
    throw new ApiError(i18n.t('pipelines.reinit.invalidIdentity'), 422, 'invalid_reinit_identity')
  }
  return exactIdentity(
    await generated.confirmReinit(id, {
      fromRunId: record.fromRunId,
      digest: record.digest,
      confirm: true,
    }),
  )
}
export const reinitQueryKey = (id: string) => ['reinit', id] as const
