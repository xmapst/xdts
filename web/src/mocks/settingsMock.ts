import type { SystemSettings } from '@/api/settings'
import { ApiError } from '@/api/client'
import { fetchMe } from './authMock'

export const backendWriteParity = {
  liveRepoWrites: ['UpdateRuntimeSettings'],
  note: 'PUT /settings calls UpdateRuntimeSettings in the same transaction as its audit.',
} as const
let current: SystemSettings = {
  logRetentionSeconds: 15 * 86400,
  metricsRetentionSeconds: 15 * 86400,
  eventRetentionSeconds: 30 * 86400,
  conflictRetentionSeconds: 30 * 86400,
  precheckValiditySeconds: 1800,
  version: '1'.repeat(64),
}
async function requireAdmin() {
  if ((await fetchMe()).user?.role !== 'admin')
    throw new ApiError('没有权限执行此操作', 403, 'forbidden')
}
export async function getSettings(): Promise<SystemSettings> {
  await requireAdmin()
  return { ...current }
}
export async function updateSettings(input: SystemSettings): Promise<SystemSettings> {
  await requireAdmin()
  for (const key of [
    'logRetentionSeconds',
    'metricsRetentionSeconds',
    'eventRetentionSeconds',
    'conflictRetentionSeconds',
  ] as const) {
    const value = input[key]
    if (!Number.isInteger(value) || value < 1 || value > 3650 * 86400)
      throw new ApiError('设置参数不合法', 400, 'invalid_settings')
  }
  if (
    !Number.isInteger(input.precheckValiditySeconds) ||
    input.precheckValiditySeconds < 1 ||
    input.precheckValiditySeconds > 86400
  )
    throw new ApiError('设置参数不合法', 400, 'invalid_settings')
  if (!/^[a-f0-9]{64}$/i.test(input.version))
    throw new ApiError('设置参数不合法', 400, 'invalid_settings')
  const values = (s: SystemSettings) =>
    JSON.stringify([
      s.logRetentionSeconds,
      s.metricsRetentionSeconds,
      s.eventRetentionSeconds,
      s.conflictRetentionSeconds,
      s.precheckValiditySeconds,
    ])
  if (values(input) === values(current)) return { ...current }
  if (input.version !== current.version)
    throw new ApiError('设置已被更新，请刷新后重试', 409, 'settings_version_conflict')
  current = { ...input, version: crypto.randomUUID().replaceAll('-', '').repeat(2) }
  return { ...current }
}
