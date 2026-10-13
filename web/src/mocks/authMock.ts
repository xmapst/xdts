// 认证域的开发期 mock 实现：后端未就绪时，让登录页三种方式都能在
// `npm run dev` 下走通。仅在 isApiMockEnabled() 为 true 时被调用（见 web/src/api/auth.ts）。
// 注意：这里的账号密码是随本仓库公开的演示数据，不是真实凭据。
import { ApiError } from '@/api/client'
import type {
  CurrentUser,
  ChangePasswordRequest,
  LoginMethods,
  LoginRequest,
  LoginResponse,
  MeResponse,
} from '@/api/auth'

interface MockAccount {
  username: string
  password: string
  user: CurrentUser
}

const ACCOUNTS: MockAccount[] = [
  {
    username: 'bootstrap',
    password: 'bootstrap123',
    user: {
      id: 'u-bootstrap',
      username: 'bootstrap',
      displayName: '初始管理员（演示）',
      role: 'admin',
      authSource: 'local',
      mustChangePassword: true,
    },
  },
  {
    username: 'admin',
    password: 'admin123',
    user: {
      id: 'u-admin',
      username: 'admin',
      displayName: '管理员（演示）',
      role: 'admin',
      authSource: 'local',
    },
  },
  {
    username: 'viewer',
    password: 'viewer123',
    user: {
      id: 'u-viewer',
      username: 'viewer',
      displayName: '只读用户（演示）',
      role: 'readonly',
      authSource: 'local',
    },
  },
  {
    username: 'ldap.user',
    password: 'ldap123',
    user: {
      id: 'u-ldap',
      username: 'ldap.user',
      displayName: 'LDAP 用户（演示）',
      role: 'readonly',
      authSource: 'ldap',
    },
  },
]

const OIDC_USER: CurrentUser = {
  id: 'u-oidc',
  username: 'oidc.user',
  displayName: 'OIDC 用户（演示）',
  role: 'admin',
  authSource: 'oidc',
}

/** mock 模式下三种登录方式全部开启，供登录页联调。 */
const MOCK_METHODS: LoginMethods = { local: true, ldap: true, oidc: true }

let sessionUser: CurrentUser | undefined

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export async function fetchMe(): Promise<MeResponse> {
  await delay(150)
  if (!sessionUser) {
    return { authenticated: false, methods: MOCK_METHODS }
  }
  return { authenticated: true, user: sessionUser, methods: MOCK_METHODS }
}

export async function login(payload: LoginRequest): Promise<LoginResponse> {
  await delay(300)
  const account = ACCOUNTS.find(
    (item) => item.username === payload.username && item.password === payload.password,
  )
  if (!account) {
    throw new ApiError('用户名或密码错误', 401)
  }
  sessionUser = account.user
  return { user: account.user }
}

export async function logout(): Promise<void> {
  await delay(100)
  sessionUser = undefined
}

export async function changePassword(payload: ChangePasswordRequest): Promise<void> {
  await delay(100)
  const account = ACCOUNTS.find((item) => item.user.id === sessionUser?.id)
  if (!account || account.password !== payload.oldPassword) {
    throw new ApiError('旧密码错误', 400, 'old_password_invalid')
  }
  if (!payload.newPassword || payload.newPassword === payload.oldPassword) {
    throw new ApiError('新密码不能与旧密码相同', 400, 'invalid_password')
  }
  account.password = payload.newPassword
  account.user.mustChangePassword = false
  sessionUser = undefined
}

export async function oidcLogin(): Promise<LoginResponse> {
  await delay(400)
  sessionUser = OIDC_USER
  return { user: OIDC_USER }
}

/**
 * 供其它 mock 模块（目前只有 web/src/mocks/usersMock.ts）判断"当前操作者是不是这一行"，
 * 用于模拟用户管理 的自我保护校验（禁止对自己删除/禁用/降级）。
 * 只读一个 id，不导出 `sessionUser` 本身，避免其它 mock 顺手改动登录态。
 */
export function getMockSessionUserId(): string | undefined {
  return sessionUser?.id
}
