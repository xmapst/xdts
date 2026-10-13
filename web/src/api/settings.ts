import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useCurrentUser } from './auth'
import * as generated from './generated'
import { isApiMockEnabled } from '@/mocks/config'
import * as settingsMock from '@/mocks/settingsMock'

export type SystemSettings = generated.GeneratedSystemSettings
export function getSettings(): Promise<SystemSettings> {
  return isApiMockEnabled() ? settingsMock.getSettings() : generated.getSettings()
}
export function updateSettings(settings: SystemSettings): Promise<SystemSettings> {
  return isApiMockEnabled()
    ? settingsMock.updateSettings(settings)
    : generated.updateSettings(settings)
}
export function useSettingsQuery() {
  const { role } = useCurrentUser()
  return useQuery({ queryKey: ['systemSettings'], queryFn: getSettings, enabled: role === 'admin' })
}
export function useSettingsMutation() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: updateSettings,
    onSuccess: (settings) => client.setQueryData(['systemSettings'], settings),
    meta: { suppressGlobalError: true },
  })
}
