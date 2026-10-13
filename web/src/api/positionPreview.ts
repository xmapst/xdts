import * as generated from './generated'

export interface PositionPreview {
  pipelineId: string
  runId: number
  configVersion: number
  digest: string
  requestedTsMs: number
  gtidSet: string
  file: string
  offset: number
  commitTsMs: number
  confirmedAt: string | null
}

export function previewTimestampPosition(id: string, tsMs: number): Promise<PositionPreview> {
  return generated.previewTimestampPosition(id, { tsMs })
}

export function getTimestampPosition(id: string): Promise<PositionPreview> {
  return generated.getTimestampPosition(id)
}

export function confirmTimestampPosition(id: string, digest: string): Promise<PositionPreview> {
  return generated.confirmTimestampPosition(id, {
    digest,
    confirm: true,
  })
}
