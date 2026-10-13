import { useQuery } from '@tanstack/react-query'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'

export type PipelineMetrics = generated.GeneratedPipelineMetrics
export type MetricHistory = generated.GeneratedMetricHistory
export type ProgressObservation = generated.GeneratedProgressObservation
export type MetricsQuery = NonNullable<Parameters<typeof generated.getPipelineMetrics>[1]> & {
  windowHours?: number
}

export async function getPipelineMetrics(
  id: string,
  { windowHours, ...query }: MetricsQuery,
  signal?: AbortSignal,
): Promise<PipelineMetrics | null> {
  if (isApiMockEnabled()) return null
  if (windowHours !== undefined) {
    const now = Date.now()
    query.from = new Date(now - windowHours * 3600000).toISOString()
    query.to = new Date(now).toISOString()
  }
  return generated.getPipelineMetrics(id, query, { signal })
}

export function usePipelineMetricsQuery(id: string, query: MetricsQuery) {
  return useQuery({
    queryKey: ['pipelines', 'metrics', id, query],
    queryFn: ({ signal }) => getPipelineMetrics(id, query, signal),
    retry: false,
    staleTime: 0,
    refetchInterval: 5000,
    meta: { suppressGlobalError: true },
  })
}
