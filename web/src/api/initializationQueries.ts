import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  confirmSchemaPlan,
  getSchemaPlan,
  previewSchemaPlan,
  schemaPlanKey,
  type SchemaPlanPreviewInput,
} from './schemaPlan'
import { getPipelineSnapshot, snapshotKey } from './snapshot'
import { pipelineDetailQueryKey } from './pipelines'
import { precheckKey } from './precheck'
import type { PageQuery } from './types'

export function usePipelineSnapshotQuery(id: string, page: PageQuery) {
  return useQuery({
    queryKey: [...snapshotKey(id), page],
    queryFn: ({ signal }) => getPipelineSnapshot(id, page, signal),
    retry: false,
    staleTime: 0,
    refetchInterval: 5000,
    meta: { suppressGlobalError: true },
  })
}
export function useSchemaPlanQuery(id: string) {
  return useQuery({
    queryKey: schemaPlanKey(id),
    queryFn: ({ signal }) => getSchemaPlan(id, signal),
    retry: false,
    staleTime: 0,
    meta: { suppressGlobalError: true },
  })
}
export function useSchemaPlanPreviewMutation(id: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (input: SchemaPlanPreviewInput) => previewSchemaPlan(id, input),
    retry: false,
    meta: { suppressGlobalError: true },
    onMutate: () => client.cancelQueries({ queryKey: schemaPlanKey(id) }),
    onSuccess: (result) => client.setQueryData(schemaPlanKey(id), result),
    onSettled: () => client.invalidateQueries({ queryKey: schemaPlanKey(id) }),
  })
}
export function useSchemaPlanConfirmMutation(id: string) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (digest: string) => confirmSchemaPlan(id, digest),
    retry: false,
    meta: { suppressGlobalError: true },
    onMutate: () => client.cancelQueries({ queryKey: schemaPlanKey(id) }),
    onSuccess: (result) => client.setQueryData(schemaPlanKey(id), result),
    onSettled: async () => {
      await Promise.all([
        client.invalidateQueries({ queryKey: schemaPlanKey(id) }),
        client.invalidateQueries({ queryKey: pipelineDetailQueryKey(id) }),
        client.invalidateQueries({ queryKey: precheckKey(id) }),
      ])
    },
  })
}
