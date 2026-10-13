// 凭据处理：`mqMgmtPassword` 只写不读——接口只在提交更新时携带，`CenterSummary`
// （GET 响应）里没有这个字段，避免明文凭据被回显、进日志或落 localStorage
// （对齐「凭据类字段明文存储，接口只写不读」）。
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import * as centersMock from '@/mocks/centersMock'

// Domain aliases preserve the screen and mock boundary; wire contracts come
// from OpenAPI and production requests use the generated operations below.
export type OriginUuidKind = generated.GeneratedOriginUuidKind
export type CenterSummary = generated.GeneratedCenterSummary
export type CenterInput = generated.GeneratedCenterInput
export type CenterUpdateInput = generated.GeneratedCenterUpdateInput
export type CenterBrokerInput = generated.GeneratedCenterBrokerInput
export type OriginUuid = generated.GeneratedOriginUuid
export type OriginUuidInput = generated.GeneratedOriginUuidInput

export function listCenters(): Promise<CenterSummary[]> {
  if (isApiMockEnabled()) {
    return centersMock.listCenters()
  }
  return generated.listCenters()
}

export function getCenter(id: string): Promise<CenterSummary> {
  if (isApiMockEnabled()) {
    return centersMock.getCenter(id)
  }
  return generated.getCenter(id)
}

export function createCenter(input: CenterInput): Promise<CenterSummary> {
  if (isApiMockEnabled()) {
    return centersMock.createCenter(input)
  }
  return generated.createCenter(input)
}

export function updateCenter(
  id: string,
  input: CenterUpdateInput & Partial<CenterBrokerInput>,
): Promise<CenterSummary> {
  if (isApiMockEnabled()) {
    return centersMock.updateCenter(id, input)
  }
  return generated.updateCenter(id, input)
}

/** 删除前后端会校验无归属的数据端点 / 节点 / pipeline，否则拒绝（MGR-CTR-04）。 */
export function deleteCenter(id: string): Promise<void> {
  if (isApiMockEnabled()) {
    return centersMock.deleteCenter(id)
  }
  return generated.deleteCenter(id).then(() => undefined)
}

export function listOriginUuids(centerId: string): Promise<OriginUuid[]> {
  if (isApiMockEnabled()) {
    return centersMock.listOriginUuids(centerId)
  }
  return generated.listOriginUuids(centerId)
}

export function addOriginUuid(
  centerId: string,
  input: OriginUuidInput,
): Promise<OriginUuid> {
  if (isApiMockEnabled()) {
    return centersMock.addOriginUuid(centerId, input)
  }
  return generated.addOriginUuid(centerId, input)
}

export function deleteOriginUuid(centerId: string, uuidId: string): Promise<void> {
  if (isApiMockEnabled()) {
    return centersMock.deleteOriginUuid(centerId, uuidId)
  }
  return generated.deleteOriginUuid(centerId, uuidId).then(() => undefined)
}

/** 指派巡检节点；传 null 表示恢复自动选择（默认自动选在线节点，离线时自动切换）。 */
export function setInspectorNode(centerId: string, nodeId: string | null): Promise<void> {
  if (isApiMockEnabled()) {
    return centersMock.setInspectorNode(centerId, nodeId)
  }
  return generated.setInspectorNode(centerId, { nodeId }).then(() => undefined)
}

export const centersQueryKey = ['centers', 'list'] as const
export const originUuidsQueryKey = (centerId: string) =>
  ['centers', centerId, 'origin-uuids'] as const

/** `enabled` 默认 true；未展开的抽屉候选查询可传 false。 */
export function useCentersQuery(enabled = true) {
  return useQuery({ queryKey: centersQueryKey, queryFn: listCenters, enabled })
}

export function useOriginUuidsQuery(centerId: string | undefined) {
  return useQuery({
    queryKey: originUuidsQueryKey(centerId ?? ''),
    queryFn: () => listOriginUuids(centerId ?? ''),
    enabled: Boolean(centerId),
  })
}

// 以下 mutation hook 一律 suppressGlobalError：调用方（页面 / 弹窗组件）都会在
// mutate() 的 onError 里自行展示提示（toast 或表单内联错误），不需要 queryClient
// 的全局 mutationCache 再重复弹一次（否则同一个错误会被展示两次）。

export function useCreateCenterMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: createCenter,
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: centersQueryKey })
    },
  })
}

export function useUpdateCenterMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      id,
      input,
    }: {
      id: string
      input: CenterUpdateInput & Partial<CenterBrokerInput>
    }) => updateCenter(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: centersQueryKey })
    },
  })
}

export function useDeleteCenterMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => deleteCenter(id),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: centersQueryKey })
    },
  })
}

export function useAddOriginUuidMutation(centerId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: OriginUuidInput) => addOriginUuid(centerId, input),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: originUuidsQueryKey(centerId) })
      void queryClient.invalidateQueries({ queryKey: centersQueryKey })
    },
  })
}

export function useDeleteOriginUuidMutation(centerId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (uuidId: string) => deleteOriginUuid(centerId, uuidId),
    meta: { suppressGlobalError: true },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: originUuidsQueryKey(centerId) })
      void queryClient.invalidateQueries({ queryKey: centersQueryKey })
    },
  })
}
