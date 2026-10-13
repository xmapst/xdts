import { useQuery, useQueryClient, useMutation } from '@tanstack/react-query'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'

export type PipelineRisk = generated.GeneratedPipelineRisk

export async function getPipelineRisk(id: string): Promise<PipelineRisk> {
  if (isApiMockEnabled()) return (await import('@/mocks/pipelineRiskMock')).getPipelineRisk(id)
  return generated.getPipelineRisk(id)
}

export async function confirmPipelineRisk(id: string, digest: string): Promise<PipelineRisk> {
  if (isApiMockEnabled())
    return (await import('@/mocks/pipelineRiskMock')).confirmPipelineRisk(id, digest)
  return generated.confirmPipelineRisk(id, { digest })
}

export function usePipelineRiskQuery(id: string) {
  return useQuery({
    queryKey: ['pipelines', 'risk', id],
    queryFn: () => getPipelineRisk(id),
    enabled: Boolean(id),
    refetchInterval: 5000,
  })
}

export function useConfirmPipelineRiskMutation(id: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (digest: string) => confirmPipelineRisk(id, digest),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['pipelines'] })
    },
  })
}
