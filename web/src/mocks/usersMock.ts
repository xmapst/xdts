// 种子账号的 id/username/role/authSource 与 web/src/mocks/authMock.ts 的 ACCOUNTS/OIDC_USER
// 逐一对应（同一批 u-admin/u-viewer/u-ldap/u-oidc），不是巧合也不是各自随便
// 编：本文件要靠 authMock.getMockSessionUserId() 判断"当前登录的是不是这一行"
// 来演示自我保护（禁止对自己删除/禁用/降级，），如果两份种子数据
// 各用各的 id，登录后永远匹配不到自己那一行，自我保护相关的三条错误码在
// mock 模式下就永远走不到、演示不出来。
import { ApiError } from '@/api/client'
import type {
  CreateUserInput,
  ResetPasswordInput,
  RevokeSessionsResult,
  SetUserStatusInput,
  UpdateUserInput,
  UserDetail,
  UserListQuery,
  UserSummary,
  UserScopesResponse,
  ReplaceUserScopesInput,
} from '@/api/users'
import type { Paginated } from '@/api/types'
import { getMockSessionUserId } from './authMock'

/**
 * 本域 mock 写操作镜像的后端写入方（**就地声明，不设中心白名单**）。
 * 由 `web/src/mocks/backendWriteParity.test.ts` 逐条核对可达性，双向失败。
 *
 * 用户管理后端路由已接线，本声明跟踪 mock 对应的 repo 写入方可达性。
 */
export const backendWriteParity = {
  liveRepoWrites: [
    'CreateUser',
    'UpdateUser',
    'DeleteUser',
    'SetUserStatus',
    'SetUserPassword',
    'ResetLoginLockout',
    'RevokeAllUserSessionsForUserCount',
    'ReplaceUserScopes',
  ],
  noBackendYet: ['RecordUserLoginSuccess'],
  note: '见上方注释',
} as const

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function seedUsers(): UserDetail[] {
  return [
    {
      id: 'u-admin',
      username: 'admin',
      displayName: 'admin',
      role: 'admin',
      status: 'active',
      authSource: 'local',
      lastLoginAt: '2026-09-24T01:30:00Z',
      createdAt: '2026-01-01T00:00:00Z',
      externalId: '',
      updatedAt: '2026-01-01T00:00:00Z',
      lockedUntil: null,
      failureCount: 0,
    },
    {
      id: 'u-admin2',
      username: 'alice',
      displayName: 'alice',
      role: 'admin',
      status: 'active',
      authSource: 'local',
      // 第二个本地管理员：演示"系统里不止一个 admin"，也让自我保护类演示
      // （只挡自己，不挡别人）看起来不是巧合凑出来的单管理员系统。
      lastLoginAt: '2026-09-20T08:00:00Z',
      createdAt: '2026-02-10T00:00:00Z',
      externalId: '',
      updatedAt: '2026-02-10T00:00:00Z',
      lockedUntil: null,
      failureCount: 0,
    },
    {
      id: 'u-viewer',
      username: 'viewer',
      displayName: 'viewer',
      role: 'readonly',
      status: 'active',
      authSource: 'local',
      lastLoginAt: '2026-09-23T12:00:00Z',
      createdAt: '2026-03-01T00:00:00Z',
      externalId: '',
      updatedAt: '2026-03-01T00:00:00Z',
      lockedUntil: null,
      failureCount: 0,
    },
    {
      id: 'u-disabled-demo',
      username: 'bob',
      displayName: 'bob',
      role: 'readonly',
      // 已禁用的本地账号：演示「启用」这条行内操作与 status 列的禁用态展示。
      status: 'disabled',
      authSource: 'local',
      lastLoginAt: '2026-08-01T03:00:00Z',
      createdAt: '2026-01-15T00:00:00Z',
      externalId: '',
      updatedAt: '2026-08-15T00:00:00Z',
      lockedUntil: null,
      failureCount: 0,
    },
    {
      id: 'u-ldap',
      username: 'ldap.user',
      displayName: 'ldap.user',
      role: 'readonly',
      status: 'active',
      authSource: 'ldap',
      externalId: 'cn=ldap.user,ou=people,dc=example,dc=com',
      lastLoginAt: '2026-09-22T09:15:00Z',
      createdAt: '2026-04-05T00:00:00Z',
      updatedAt: '2026-04-05T00:00:00Z',
      lockedUntil: null,
      failureCount: 0,
    },
    {
      id: 'u-oidc',
      username: 'oidc.user',
      displayName: 'oidc.user',
      role: 'admin',
      status: 'active',
      authSource: 'oidc',
      externalId: 'oidc|1234567890',
      lastLoginAt: null,
      createdAt: '2026-05-20T00:00:00Z',
      updatedAt: '2026-05-20T00:00:00Z',
      lockedUntil: null,
      failureCount: 0,
    },
  ]
}

let users: UserDetail[] = seedUsers()
let seq = 1

/** 列表响应只声明两态，不含 locked——；mock 数据本身也从不产生
 *  locked（全仓真实后端同样没有任何写入路径会产生这个值，见 的
 *  RecordUserLoginFailure 核查），这里的类型收窄和契约对齐，不是简化。 */
function toSummary(user: UserDetail): UserSummary {
  return {
    id: user.id,
    username: user.username,
    displayName: user.displayName,
    role: user.role,
    status: user.status === 'locked' ? 'active' : user.status,
    authSource: user.authSource,
    lastLoginAt: user.lastLoginAt,
    createdAt: user.createdAt,
  }
}

export async function listUsers(query: UserListQuery): Promise<Paginated<UserSummary>> {
  await delay(150)
  let filtered = users
  if (query.keyword) {
    const keyword = query.keyword.trim().toLowerCase()
    if (keyword.length > 0) {
      filtered = filtered.filter((item) => item.username.toLowerCase().includes(keyword))
    }
  }
  if (query.role) {
    filtered = filtered.filter((item) => item.role === query.role)
  }
  if (query.authSource) {
    filtered = filtered.filter((item) => item.authSource === query.authSource)
  }
  if (query.status) {
    filtered = filtered.filter((item) => item.status === query.status)
  }
  const total = filtered.length
  const start = (query.page - 1) * query.pageSize
  const items = filtered.slice(start, start + query.pageSize).map(toSummary)
  return { items, total, page: query.page, pageSize: query.pageSize }
}

export async function getUser(id: string): Promise<UserDetail> {
  await delay(120)
  const found = users.find((item) => item.id === id)
  if (!found) {
    throw new ApiError('用户不存在', 404, 'not_found')
  }
  return { ...found }
}

const userScopes = new Map<string, UserScopesResponse>()

export async function listUserScopes(id: string): Promise<UserScopesResponse> {
  await getUser(id)
  return structuredClone(userScopes.get(id) ?? { restricted: false, items: [] })
}

export async function replaceUserScopes(
  id: string,
  input: ReplaceUserScopesInput,
): Promise<UserScopesResponse> {
  await getUser(id)
  if (
    (!input.restricted && input.scopes.length > 0) ||
    input.scopes.length > 1000 ||
    input.scopes.some((scope) => scope.canWrite) ||
    new Set(input.scopes.map((scope) => `${scope.scope}:${scope.resourceId}`)).size !==
      input.scopes.length
  ) {
    throw new ApiError('Invalid resource scopes', 400, 'invalid_user_scopes')
  }
  const data: UserScopesResponse = {
    restricted: input.restricted,
    items: input.scopes.map((scope) => ({ ...scope, id: crypto.randomUUID(), canWrite: false })),
  }
  userScopes.set(id, structuredClone(data))
  return structuredClone(data)
}

export async function createUser(input: CreateUserInput): Promise<UserDetail> {
  await delay(200)
  if (!input.password) {
    throw new ApiError('密码不能为空', 400, 'invalid_password')
  }
  if (users.some((item) => item.username === input.username)) {
    throw new ApiError('用户名已存在', 409, 'username_conflict')
  }
  const now = new Date().toISOString()
  const record: UserDetail = {
    id: `user-mock-${String(seq).padStart(4, '0')}`,
    username: input.username,
    displayName: input.username,
    role: input.role,
    status: 'active',
    // 本端点只建本地账号——外部账号只能靠首次登录自动建号产生，。
    authSource: 'local',
    lastLoginAt: null,
    createdAt: now,
    externalId: '',
    updatedAt: now,
    lockedUntil: null,
    failureCount: 0,
  }
  seq += 1
  users = [...users, record]
  return record
}

export async function updateUser(id: string, input: UpdateUserInput): Promise<UserDetail> {
  await delay(200)
  const index = users.findIndex((item) => item.id === id)
  const existing = index >= 0 ? users[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('用户不存在', 404, 'not_found')
  }
  if (input.username !== undefined && existing.authSource !== 'local') {
    throw new ApiError(
      '外部账号的用户名由身份源管理，不可修改',
      400,
      'external_user_field_not_editable',
    )
  }
  if (input.role === 'readonly' && existing.role === 'admin' && id === getMockSessionUserId()) {
    throw new ApiError('不能修改自己的角色', 409, 'cannot_demote_self')
  }
  if (
    input.username !== undefined &&
    input.username !== existing.username &&
    users.some((item) => item.username === input.username)
  ) {
    throw new ApiError('用户名已存在', 409, 'username_conflict')
  }
  const updated: UserDetail = {
    ...existing,
    username: input.username ?? existing.username,
    role: input.role ?? existing.role,
    updatedAt: new Date().toISOString(),
  }
  users[index] = updated
  return updated
}

/** 204 语义：mock 不需要真的存密码（本来也没有地方存——UserDetail 类型里
 *  根本没有密码字段，见 web/src/api/users.ts 文件头说明），只做校验分支演示。 */
export async function resetUserPassword(id: string, input: ResetPasswordInput): Promise<void> {
  await delay(200)
  const existing = users.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('用户不存在', 404, 'not_found')
  }
  if (existing.authSource !== 'local') {
    throw new ApiError(
      '外部账号的密码由身份源管理，不可修改',
      400,
      'external_user_field_not_editable',
    )
  }
  if (!input.password) {
    throw new ApiError('密码不能为空', 400, 'invalid_password')
  }
}

export async function setUserStatus(id: string, input: SetUserStatusInput): Promise<UserDetail> {
  await delay(200)
  const index = users.findIndex((item) => item.id === id)
  const existing = index >= 0 ? users[index] : undefined
  if (index < 0 || !existing) {
    throw new ApiError('用户不存在', 404, 'not_found')
  }
  if (input.status === 'disabled' && id === getMockSessionUserId()) {
    throw new ApiError('不能禁用自己的账号', 409, 'cannot_disable_self')
  }
  const updated: UserDetail = {
    ...existing,
    status: input.status,
    updatedAt: new Date().toISOString(),
  }
  users[index] = updated
  return updated
}

/** 幂等：未锁定时调用也返回当前状态、不报错，——mock 里的账号本来
 *  就从不处于 locked（见 toSummary 的说明），这里直接原样返回即可演示这条
 *  幂等行为，不需要额外构造一个"已锁定"的种子账号。 */
export async function unlockUser(id: string): Promise<UserDetail> {
  await delay(150)
  const existing = users.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('用户不存在', 404, 'not_found')
  }
  return { ...existing, lockedUntil: null, failureCount: 0 }
}

/** 固定返回 2——演示用，不代表真实语义（真实撤销数取决于该用户当前有多少
 *  条活跃会话）。用固定值而不是随机数，是为了让断言"已强制下线 N 个会话"
 *  文案的测试可复现，不因为随机数偶发失败。 */
export async function revokeUserSessions(id: string): Promise<RevokeSessionsResult> {
  await delay(150)
  const existing = users.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('用户不存在', 404, 'not_found')
  }
  return { revokedCount: 2 }
}

export async function deleteUser(id: string): Promise<void> {
  await delay(200)
  const existing = users.find((item) => item.id === id)
  if (!existing) {
    throw new ApiError('用户不存在', 404, 'not_found')
  }
  if (id === getMockSessionUserId()) {
    throw new ApiError('不能删除自己的账号', 409, 'cannot_delete_self')
  }
  users = users.filter((item) => item.id !== id)
}
