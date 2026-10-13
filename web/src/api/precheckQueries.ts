import { useIsMutating, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import {
  confirmPipelinePrecheck,
  getPipelinePrecheck,
  runPipelinePrecheck,
  precheckKey,
  type PipelinePrecheck,
} from './precheck'

export function usePipelinePrecheckQuery(id: string | undefined) {
  return useQuery({
    queryKey: precheckKey(id),
    queryFn: ({ signal }) => getPipelinePrecheck(id!, signal),
    enabled: !!id,
    retry: false,
    staleTime: 0,
    refetchInterval: (query) => (query.state.data && !query.state.data.complete ? 2000 : 15000),
  })
}
export function usePipelinePrecheckMutation(id: string, confirmation = false) {
  const client = useQueryClient()
  return useMutation({
    mutationKey: precheckKey(id),
    mutationFn: (input: { batchId: string; items: string[] } | undefined) =>
      confirmation && input
        ? confirmPipelinePrecheck(id, input.batchId, input.items)
        : runPipelinePrecheck(id),
    retry: false,
    meta: { suppressGlobalError: true },
    onMutate: async () => {
      await client.cancelQueries({ queryKey: precheckKey(id) })
      client.setQueryData<PipelinePrecheck | null>(precheckKey(id), (current) =>
        current ? { ...current, canStart: false } : null,
      )
    },
    onSuccess: (result) => client.setQueryData(precheckKey(id), result),
    onSettled: async () => {
      await client.invalidateQueries({ queryKey: precheckKey(id) })
    },
  })
}

export function usePipelinePrecheckAuthorization(
  id: string | undefined,
  configVersion: number | undefined,
) {
  const query = usePipelinePrecheckQuery(id)
  const busy = useIsMutating({ mutationKey: precheckKey(id) }) > 0
  const [now, setNow] = useState(() => Date.now())
  const expires = query.data ? Date.parse(query.data.expiresAt) : 0
  useEffect(() => {
    const timer = setTimeout(
      () => setNow(Date.now()),
      Math.max(0, Math.min(expires - Date.now() + 1, 2147483647)),
    )
    return () => clearTimeout(timer)
  }, [expires])
  return (
    !!query.data?.canStart &&
    query.data.complete &&
    query.data.configVersion === configVersion &&
    expires > now &&
    !query.isError &&
    !query.isFetching &&
    !busy
  )
}
