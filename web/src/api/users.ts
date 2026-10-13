// 密码只进不出（）：`CreateUserInput`/`ResetPasswordInput` 是
// 唯一带 password 字段的类型，`UserSummary`/`UserDetail` 任何时候都不声明这个
// 字段——不是"响应里这个字段是 undefined"，是类型上根本不存在这个字段可用。
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import type { Paginated } from './types'
import { isApiMockEnabled } from '@/mocks/config'
import * as usersMock from '@/mocks/usersMock'
import {
  listUserScopes as generatedListUserScopes,
  replaceUserScopes as generatedReplaceUserScopes,
} from './generated'
import type {
  GeneratedUserScope,
  GeneratedUserScopeInput,
  GeneratedUserScopesResponse,
  GeneratedReplaceUserScopesInput,
} from './generated'

// User resource grants are generated from web/openapi/xdts.yaml. Keep the
// domain aliases here so existing user screens do not depend on generator names.
export type UserScope = GeneratedUserScope
export type UserScopeInput = GeneratedUserScopeInput
export type UserScopesResponse = GeneratedUserScopesResponse
export type ReplaceUserScopesInput = GeneratedReplaceUserScopesInput

export function listUserScopes(id: string): Promise<UserScopesResponse> {
  return isApiMockEnabled() ? usersMock.listUserScopes(id) : generatedListUserScopes(id)
}

export function replaceUserScopes(
  id: string,
  input: ReplaceUserScopesInput,
): Promise<UserScopesResponse> {
  return isApiMockEnabled()
    ? usersMock.replaceUserScopes(id, input)
    : generatedReplaceUserScopes(id, input)
}

export const userScopesQueryKey = (id: string) => ['users', 'scopes', id] as const

export function useUserScopesQuery(id: string) {
  return useQuery({ queryKey: userScopesQueryKey(id), queryFn: () => listUserScopes(id) })
}

export function useReplaceUserScopesMutation(id: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: ReplaceUserScopesInput) => replaceUserScopes(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: (data) => queryClient.setQueryData(userScopesQueryKey(id), data),
  })
}

// OpenAPI owns these wire shapes, including write-only passwords.
export type UserListStatus = generated.GeneratedUserListStatus
export type UserDetailStatus = generated.GeneratedUserDetailStatus
export type UserSummary = generated.GeneratedUserSummary
export type UserDetail = generated.GeneratedUserDetail
export type UserListQuery = generated.GeneratedUserListQuery
export type CreateUserInput = generated.GeneratedCreateUserInput
export type UpdateUserInput = generated.GeneratedUpdateUserInput
export type ResetPasswordInput = generated.GeneratedResetPasswordInput
export type SetUserStatusInput = generated.GeneratedSetUserStatusInput
export type RevokeSessionsResult = generated.GeneratedRevokeSessionsResult

export function listUsers(query: UserListQuery): Promise<Paginated<UserSummary>> {
  if (isApiMockEnabled()) {
    return usersMock.listUsers(query)
  }
  return generated.listUsers(query)
}

export function getUser(id: string): Promise<UserDetail> {
  if (isApiMockEnabled()) {
    return usersMock.getUser(id)
  }
  return generated.getUser(id)
}

export function createUser(input: CreateUserInput): Promise<UserDetail> {
  if (isApiMockEnabled()) {
    return usersMock.createUser(input)
  }
  return generated.createUser(input)
}

export function updateUser(id: string, input: UpdateUserInput): Promise<UserDetail> {
  if (isApiMockEnabled()) {
    return usersMock.updateUser(id, input)
  }
  return generated.updateUser(id, input)
}

/** 204，无响应体——密码不回显，见。 */
export function resetUserPassword(id: string, input: ResetPasswordInput): Promise<void> {
  if (isApiMockEnabled()) {
    return usersMock.resetUserPassword(id, input)
  }
  return generated.resetUserPassword(id, input)
}

export function setUserStatus(id: string, input: SetUserStatusInput): Promise<UserDetail> {
  if (isApiMockEnabled()) {
    return usersMock.setUserStatus(id, input)
  }
  return generated.setUserStatus(id, input)
}

/** 幂等：未锁定时调用也返回 200，不是错误，见。 */
export function unlockUser(id: string): Promise<UserDetail> {
  if (isApiMockEnabled()) {
    return usersMock.unlockUser(id)
  }
  return generated.unlockUser(id)
}

export function revokeUserSessions(id: string): Promise<RevokeSessionsResult> {
  if (isApiMockEnabled()) {
    return usersMock.revokeUserSessions(id)
  }
  return generated.revokeUserSessions(id)
}

export function deleteUser(id: string): Promise<void> {
  if (isApiMockEnabled()) {
    return usersMock.deleteUser(id)
  }
  return generated.deleteUser(id)
}

export const usersListQueryKey = (query: UserListQuery) => ['users', 'list', query] as const
export const userDetailQueryKey = (id: string) => ['users', 'detail', id] as const

export function useUsersQuery(query: UserListQuery, enabled = true) {
  return useQuery({
    queryKey: usersListQueryKey(query),
    queryFn: () => listUsers(query),
    enabled,
  })
}

export function useUserQuery(id: string | undefined) {
  return useQuery({
    queryKey: userDetailQueryKey(id ?? ''),
    queryFn: () => getUser(id ?? ''),
    enabled: Boolean(id),
  })
}

function useInvalidateUsers() {
  const queryClient = useQueryClient()
  return (id?: string) => {
    void queryClient.invalidateQueries({ queryKey: ['users', 'list'] })
    if (id) {
      void queryClient.invalidateQueries({ queryKey: userDetailQueryKey(id) })
    }
  }
}

// 以下 mutation hook 一律 suppressGlobalError：调用方在 onError 里自行展示
// 提示，避免和 queryClient 全局 mutationCache 的提示重复。

export function useCreateUserMutation() {
  const invalidate = useInvalidateUsers()
  return useMutation({
    mutationFn: createUser,
    meta: { suppressGlobalError: true },
    onSuccess: () => invalidate(),
  })
}

export function useUpdateUserMutation() {
  const invalidate = useInvalidateUsers()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: UpdateUserInput }) =>
      updateUser(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, variables) => invalidate(variables.id),
  })
}

export function useResetPasswordMutation() {
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: ResetPasswordInput }) =>
      resetUserPassword(id, input),
    meta: { suppressGlobalError: true },
  })
}

export function useSetUserStatusMutation() {
  const invalidate = useInvalidateUsers()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: SetUserStatusInput }) =>
      setUserStatus(id, input),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, variables) => invalidate(variables.id),
  })
}

export function useUnlockUserMutation() {
  const invalidate = useInvalidateUsers()
  return useMutation({
    mutationFn: (id: string) => unlockUser(id),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, id) => invalidate(id),
  })
}

export function useRevokeUserSessionsMutation() {
  return useMutation({
    mutationFn: (id: string) => revokeUserSessions(id),
    meta: { suppressGlobalError: true },
  })
}

export function useDeleteUserMutation() {
  const invalidate = useInvalidateUsers()
  return useMutation({
    mutationFn: (id: string) => deleteUser(id),
    meta: { suppressGlobalError: true },
    onSuccess: (_data, id) => invalidate(id),
  })
}
