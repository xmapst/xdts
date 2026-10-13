import { useMutation, useQueryClient } from '@tanstack/react-query'
import { ApiError } from './client'
import * as generated from './generated'
import i18n from '@/i18n'
import type { PipelineCreateInput, PipelineSummary } from './pipelines'
import { isApiMockEnabled } from '@/mocks/config'
import * as operationsMock from '@/mocks/pipelinesMock'

export type PipelineCloneInput = Pick<PipelineCreateInput, 'name'> &
  Partial<Omit<PipelineCreateInput, 'name' | 'rules'>>
export type PipelineBatchAction = 'start' | 'pause' | 'resume' | 'stop'
export interface PipelineBatchInput {
  action: PipelineBatchAction
  ids: string[]
}
export interface PipelineBatchResult {
  id: string
  success: boolean
  code: string
  message: string
}
export interface PipelineBatchResponse {
  results: PipelineBatchResult[]
}

export function clonePipeline(id: string, input: PipelineCloneInput): Promise<PipelineSummary> {
  if (isApiMockEnabled()) return operationsMock.clonePipeline(id, input)
  return generated.clonePipeline(id, input)
}

export async function batchPipelines(
  input: PipelineBatchInput,
): Promise<PipelineBatchResponse> {
  if (isApiMockEnabled()) return operationsMock.batchPipelines(input)
  const response = await generated.batchPipelines(input)
  if (
    !response ||
    !Array.isArray(response.results) ||
    response.results.length !== input.ids.length ||
    new Set(response.results.map((item) => item?.id)).size !== input.ids.length ||
    response.results.some(
      (item) =>
        !item ||
        !input.ids.includes(item.id) ||
        typeof item.success !== 'boolean' ||
        typeof item.code !== 'string' ||
        typeof item.message !== 'string',
    )
  ) {
    throw new ApiError(i18n.t('pipelines.batch.invalidResult'), 422, 'invalid_batch_result')
  }
  return response
}

export function useClonePipelineMutation() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: PipelineCloneInput }) =>
      clonePipeline(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['pipelines'] })
    },
  })
}

export function useBatchPipelinesMutation() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: batchPipelines,
    meta: { suppressGlobalError: true },
    onSettled: () => {
      void client.invalidateQueries({ queryKey: ['pipelines'] })
    },
  })
}
