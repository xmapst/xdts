// 认证域：本地账号 / LDAP（共用同一登录表单，由后端按配置路由）/ OIDC。
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import * as authMock from '@/mocks/authMock'

export type UserRole = 'admin' | 'readonly'
export type AuthSource = 'local' | 'ldap' | 'oidc'

export interface CurrentUser {
  id: string
  username: string
  displayName: string
  role: UserRole
  authSource: AuthSource
  mustChangePassword?: boolean
}

/** 登录页据此决定展示哪些登录入口。 */
export interface LoginMethods {
  local: boolean
  ldap: boolean
  oidc: boolean
}

export interface MeResponse {
  authenticated: boolean
  user?: CurrentUser
  methods: LoginMethods
}

export interface LoginRequest {
  username: string
  password: string
}

export interface LoginResponse {
  user: CurrentUser
}

/** OIDC 登录直接整页跳转，不通过 fetch。 */
export const OIDC_START_PATH = '/api/v1/auth/oidc/start'

export async function fetchMe(): Promise<MeResponse> {
  if (isApiMockEnabled()) {
    return authMock.fetchMe()
  }
  return generated.fetchMe({ suppressAuthRedirect: true })
}

export async function login(payload: LoginRequest): Promise<LoginResponse> {
  if (isApiMockEnabled()) {
    return authMock.login(payload)
  }
  return generated.login(payload, {
    suppressAuthRedirect: true,
  })
}

export async function logout(): Promise<void> {
  if (isApiMockEnabled()) {
    return authMock.logout()
  }
  await generated.logout()
}

export interface ChangePasswordRequest {
  oldPassword: string
  newPassword: string
}

export async function changePassword(payload: ChangePasswordRequest): Promise<void> {
  if (isApiMockEnabled()) {
    return authMock.changePassword(payload)
  }
  await generated.changePassword(payload)
}

/**
 * 开发期 mock：真实 OIDC 走整页跳转无法用 fetch 模拟，因此在 mock 模式下
 * "使用 OIDC 登录" 按钮直接调用该函数模拟一次成功的 OIDC 登录，
 * 而不做真实跳转。生产构建中此函数不会被调用（由 LoginPage 按
 * isApiMockEnabled() 分支）。
 */
export async function mockOidcLogin(): Promise<LoginResponse> {
  return authMock.oidcLogin()
}

export const authQueryKey = ['auth', 'me'] as const

export function useMeQuery() {
  return useQuery({
    queryKey: authQueryKey,
    queryFn: fetchMe,
    staleTime: 60_000,
    meta: { suppressGlobalError: true },
  })
}

/** 便捷 hook：当前用户 / 鉴权状态 / 角色判断，供路由守卫与组件使用。 */
export function useCurrentUser() {
  const query = useMeQuery()
  const user = query.data?.user
  return {
    isLoading: query.isLoading,
    isAuthenticated: query.data?.authenticated === true,
    user,
    role: user?.role,
    isAdmin: user?.role === 'admin',
    methods: query.data?.methods,
    refetch: query.refetch,
  }
}

export function useLoginMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: login,
    meta: { suppressGlobalError: true },
    onSuccess: (data) => {
      queryClient.setQueryData<MeResponse>(authQueryKey, (prev) => ({
        authenticated: true,
        user: data.user,
        methods: prev?.methods ?? { local: true, ldap: false, oidc: false },
      }))
    },
  })
}

export function useLogoutMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: logout,
    onSuccess: () => {
      queryClient.setQueryData<MeResponse>(authQueryKey, (prev) => ({
        authenticated: false,
        methods: prev?.methods ?? { local: true, ldap: false, oidc: false },
      }))
      void queryClient.invalidateQueries({ queryKey: authQueryKey })
    },
  })
}
