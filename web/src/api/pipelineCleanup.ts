import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import type { Paginated } from './types'
import { isApiMockEnabled } from '@/mocks/config'

export type CleanupState = 'pending' | 'in_progress' | 'done' | 'failed'
export interface CleanupStep {
  state: 'pending' | 'done' | 'failed'
  error: string
}
export interface PipelineCleanup {
  id: string
  name: string
  state: CleanupState
  deletedAt: string
  attempts: number
  lastAttemptAt: string | null
  lastError: string
  source: CleanupStep
  target: CleanupStep
  queue: CleanupStep
  serverId: CleanupStep
}

export function listPipelineCleanup(
  page: number,
  pageSize: number,
): Promise<Paginated<PipelineCleanup>> {
  if (isApiMockEnabled()) return Promise.resolve({ items: [], total: 0, page, pageSize })
  return generated.listPipelineCleanup({ page, pageSize })
}

export function retryPipelineCleanup(id: string): Promise<{ scheduled: boolean }> {
  return generated.retryPipelineCleanup(id, {})
}

export function usePipelineCleanupQuery(page: number, pageSize: number) {
  return useQuery({
    queryKey: ['pipelines', 'cleanup', page, pageSize],
    queryFn: () => listPipelineCleanup(page, pageSize),
    placeholderData: keepPreviousData,
    refetchInterval: 3000,
  })
}

export function useRetryPipelineCleanupMutation() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: retryPipelineCleanup,
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['pipelines', 'cleanup'] })
    },
  })
}
