import {
  cancelPipelineDataCheck,
  createPipelineDataCheck,
  listPipelineDataChecks,
  type GeneratedDataCheckComparison,
  type GeneratedDataCheckRun,
  type GeneratedDataCheckTable,
} from './generated'

export type DataCheckTable = GeneratedDataCheckTable
export type DataCheckComparison = GeneratedDataCheckComparison
export type DataCheckRun = GeneratedDataCheckRun

export const listDataChecks = (id: string, signal?: AbortSignal) =>
  listPipelineDataChecks(id, { signal })
export const createDataCheck = (
  id: string,
  config: DataCheckRun['config'],
  sampleModulo: number,
) => createPipelineDataCheck(id, { config, sampleModulo })
export const cancelDataCheck = (id: string, checkId: string) =>
  cancelPipelineDataCheck(id, checkId)
