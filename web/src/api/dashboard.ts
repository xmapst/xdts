import { useQuery } from '@tanstack/react-query'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import * as dashboardMock from '@/mocks/dashboardMock'
import type { EventEntry } from './events'
import type { PipelineSnapshot } from './snapshot'

export type DashboardLag = generated.GeneratedDashboardLag
export type DashboardError = Pick<EventEntry, 'id' | 'ts' | 'type' | 'pipelineId' | 'nodeId'>
export type DashboardSnapshot = Pick<
  PipelineSnapshot,
  | 'pipelineId'
  | 'runId'
  | 'status'
  | 'totalChunks'
  | 'doneChunks'
  | 'catchupRemainingTxns'
  | 'updatedAt'
> & { name: string }
export type DashboardOverview = generated.GeneratedDashboardOverview
export function getDashboard(): Promise<DashboardOverview> {
  return isApiMockEnabled() ? dashboardMock.getDashboard() : generated.getDashboard()
}
export function useDashboardQuery() {
  return useQuery({ queryKey: ['dashboard'], queryFn: getDashboard, refetchInterval: 15000 })
}
