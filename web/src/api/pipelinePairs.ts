import { useMutation, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import type { PipelineSummary } from './pipelines'
import { isApiMockEnabled } from '@/mocks/config'

export type PipelinePairSideInput = generated.GeneratedPipelinePairSideInput
export type PipelinePairInput = generated.GeneratedPipelinePairInput

export interface PipelinePairResult {
  forward: PipelineSummary
  reverse: PipelineSummary
}

export async function createPipelinePair(
  input: PipelinePairInput,
): Promise<PipelinePairResult> {
  if (isApiMockEnabled()) {
    const mock = await import('@/mocks/pipelinePairsMock')
    return mock.createPipelinePair(input)
  }
  return generated.createPipelinePair(input)
}

export function useCreatePipelinePairMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: createPipelinePair,
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      for (const domain of ['pipelines', 'strategies']) {
        void queryClient.invalidateQueries({ queryKey: [domain] })
      }
    },
  })
}
