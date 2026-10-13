import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import { toWireRules } from './strategyRulesWire'
import type { StrategyRules } from './strategies'
import { ApiError } from './client'

export type PipelineRules = generated.GeneratedPipelineRules

export async function getPipelineRules(id: string): Promise<PipelineRules> {
  if (isApiMockEnabled())
    return (await import('@/mocks/pipelineRulesMock')).getPipelineRules(id)
  return generated.getPipelineRules(id)
}

export function usePipelineRulesQuery(id: string) {
  return useQuery({
    queryKey: ['pipelines', 'rules', id],
    queryFn: () => getPipelineRules(id),
    enabled: Boolean(id),
    refetchInterval: 5000,
  })
}

export function useSavePipelineRulesMutation(id: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: async ({
      configVersion,
      rules,
    }: {
      configVersion: number
      rules: StrategyRules
    }) => {
      const mapped = toWireRules(rules)
      if (!mapped.ok)
        throw new ApiError(
          mapped.blockers.map((blocker) => blocker.message).join('；'),
          400,
          'invalid_rules',
        )
      const input = { configVersion, rules: mapped.wire }
      if (isApiMockEnabled())
        return (await import('@/mocks/pipelineRulesMock')).savePipelineRules(id, input)
      return generated.savePipelineRules(id, input)
    },
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['pipelines'] })
    },
  })
}

export function useApplyPipelineRulesMutation(id: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: async (configVersion: number) => {
      if (isApiMockEnabled())
        return (await import('@/mocks/pipelineRulesMock')).applyPipelineRules(id, {
          configVersion,
        })
      return generated.applyPipelineRules(id, { configVersion })
    },
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['pipelines'] })
    },
  })
}
