import { ApiError } from './client'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import i18n from '@/i18n'

export type PositionKind = 'latest' | 'gtid_set' | 'binlog_pos' | 'timestamp'
export type PositionInput =
  | { kind: 'latest' }
  | { kind: 'gtid_set'; gtidSet: string }
  | { kind: 'binlog_pos'; file: string; offset: number }
  | { kind: 'timestamp'; tsMs: number }

export interface PositionChange {
  pipelineId: string
  runId: number
  configVersion: number
  positionRevision: number
  digest: string
  kind: PositionKind
  gtidSet: string
  file: string
  offset: number
  tsMs: number
  commitTsMs: number
  publishedGtidSet: string | null
  checkpointEpoch: number
  forwardJump: boolean
  warnings: string[]
  expiresAt: string
  confirmedAt: string | null
}

export interface PositionObservation {
  nodeId: string
  role: 'source' | 'target'
  runId: number
  observedAt: string
  values: { readGtidSet: string; publishedGtidSet: string; appliedGtidSet: string }
}

export interface PipelinePositions {
  pipelineId: string
  runId: number
  configVersion: number
  positionRevision: number
  editable: boolean
  startPosition: unknown
  observations: PositionObservation[]
}

const count = (v: number) => Number.isSafeInteger(v) && v >= 0
const date = (v: string) => typeof v === 'string' && Number.isFinite(Date.parse(v))
function validateChange(result: PositionChange, id: string): PositionChange {
  if (
    !result ||
    result.pipelineId !== id ||
    !count(result.runId) ||
    !count(result.configVersion) ||
    !count(result.positionRevision) ||
    !count(result.offset) ||
    result.offset > 4294967295 ||
    !count(result.tsMs) ||
    !count(result.commitTsMs) ||
    !count(result.checkpointEpoch) ||
    !/^[a-f0-9]{64}$/.test(result.digest) ||
    !['latest', 'gtid_set', 'binlog_pos', 'timestamp'].includes(result.kind) ||
    typeof result.gtidSet !== 'string' ||
    typeof result.file !== 'string' ||
    (result.publishedGtidSet !== null && typeof result.publishedGtidSet !== 'string') ||
    typeof result.forwardJump !== 'boolean' ||
    !Array.isArray(result.warnings) ||
    result.warnings.length > 16 ||
    result.warnings.some((warning) => typeof warning !== 'string' || warning.length === 0) ||
    !date(result.expiresAt) ||
    (result.confirmedAt !== null && !date(result.confirmedAt))
  ) {
    throw new ApiError(i18n.t('pipelines.position.invalidResult'), 422, 'invalid_position_result')
  }
  return result
}

export async function getPipelinePositions(
  id: string,
  signal?: AbortSignal,
): Promise<PipelinePositions | null> {
  if (isApiMockEnabled()) return null
  const result = await generated.getPipelinePositions(id, { signal })
  if (
    !result ||
    result.pipelineId !== id ||
    !count(result.runId) ||
    !count(result.configVersion) ||
    !count(result.positionRevision) ||
    typeof result.editable !== 'boolean' ||
    !Array.isArray(result.observations) ||
    result.observations.length > 2 ||
    new Set(result.observations.map((row) => row?.role)).size !== result.observations.length ||
    result.observations.some(
      (row) =>
        !row ||
        !row.nodeId ||
        !['source', 'target'].includes(row.role) ||
        row.runId !== result.runId ||
        !date(row.observedAt) ||
        !row.values ||
        [row.values.readGtidSet, row.values.publishedGtidSet, row.values.appliedGtidSet].some(
          (set) => typeof set !== 'string',
        ),
    )
  ) {
    throw new ApiError(i18n.t('pipelines.position.invalidResult'), 422, 'invalid_position_result')
  }
  return result
}

export async function getPositionChange(id: string): Promise<PositionChange | null> {
  if (isApiMockEnabled()) return null
  try {
    return validateChange(await generated.getPositionChange(id), id)
  } catch (error) {
    if (
      error instanceof ApiError &&
      error.status === 409 &&
      error.code === 'position_change_changed'
    )
      return null
    throw error
  }
}

export async function previewPositionChange(
  id: string,
  input: PositionInput,
): Promise<PositionChange> {
  return validateChange(await generated.previewPositionChange(id, input), id)
}

export async function confirmPositionChange(
  id: string,
  digest: string,
  confirmForwardJump: boolean,
): Promise<PositionChange> {
  return validateChange(
    await generated.confirmPositionChange(id, {
      digest,
      confirm: true,
      confirmForwardJump,
    }),
    id,
  )
}

export interface PositionBoundary {
  file: string
  requested: number
  preceding: number
  following: number | null
}
export function parsePositionBoundary(error: unknown): PositionBoundary | null {
  if (
    !(error instanceof ApiError) ||
    error.code !== 'position_boundary' ||
    !error.details ||
    typeof error.details !== 'object'
  )
    return null
  const d = error.details
  if (
    !('file' in d) ||
    typeof d.file !== 'string' ||
    !('requested' in d) ||
    typeof d.requested !== 'number' ||
    !count(d.requested) ||
    !('preceding' in d) ||
    typeof d.preceding !== 'number' ||
    !count(d.preceding) ||
    !('following' in d) ||
    (d.following !== null && (typeof d.following !== 'number' || !count(d.following)))
  )
    return null
  return { file: d.file, requested: d.requested, preceding: d.preceding, following: d.following }
}
