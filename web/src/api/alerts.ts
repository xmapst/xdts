import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import * as generated from './generated'
import type { PageQuery, Paginated } from './types'
import { isApiMockEnabled } from '@/mocks/config'
import * as alertsMock from '@/mocks/alertsMock'

export const ALERT_DIMENSIONS = [
  'node_offline',
  'latency_exceeded',
  'pipeline_error',
  'queue_backlog',
  'federation_link_down',
  'unregistered_origin_uuid',
  'local_store_abnormal',
] as const
export type AlertDimension = (typeof ALERT_DIMENSIONS)[number]
export type AlertThreshold = generated.GeneratedAlertThreshold
export type AlertRule = generated.GeneratedAlertRule
export interface AlertRuleInput {
  name: string
  dimension: AlertDimension
  enabled: boolean
  threshold: AlertThreshold
  webhookUrl?: string
  suppressSeconds: number
}
export type AlertEntry = generated.GeneratedAlertEntry
export interface AlertQuery extends PageQuery {
  ruleId?: string
  status?: AlertEntry['status']
  level?: AlertEntry['level']
  since?: string
  until?: string
}
export function listAlertRules(
  query: PageQuery = { page: 1, pageSize: 20 },
): Promise<Paginated<AlertRule>> {
  if (isApiMockEnabled()) return alertsMock.listAlertRules(query)
  return generated.listAlertRules(query)
}
export function listAlerts(
  query: AlertQuery = { page: 1, pageSize: 20 },
): Promise<Paginated<AlertEntry>> {
  if (isApiMockEnabled()) return alertsMock.listAlerts(query)
  return generated.listAlerts(query)
}
export function createAlertRule(input: AlertRuleInput): Promise<AlertRule> {
  return isApiMockEnabled() ? alertsMock.createAlertRule(input) : generated.createAlertRule(input)
}
export function updateAlertRule(
  id: string,
  input: AlertRuleInput & { version: number },
): Promise<AlertRule> {
  return isApiMockEnabled()
    ? alertsMock.updateAlertRule(id, input)
    : generated.updateAlertRule(id, input)
}
export function deleteAlertRule(id: string, version: number): Promise<void> {
  return isApiMockEnabled()
    ? alertsMock.deleteAlertRule(id, version)
    : generated.deleteAlertRule(id, { version }).then(() => undefined)
}
export function useAlertRulesQuery(query: PageQuery) {
  return useQuery({
    queryKey: ['alertRules', query],
    queryFn: () => listAlertRules(query),
    refetchInterval: 15000,
  })
}
export function useAlertsQuery(query: AlertQuery) {
  return useQuery({
    queryKey: ['alerts', query],
    queryFn: () => listAlerts(query),
    refetchInterval: 15000,
  })
}
export function useSaveAlertRuleMutation() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: ({
      id,
      version,
      input,
    }: {
      id?: string
      version?: number
      input: AlertRuleInput
    }) => (id ? updateAlertRule(id, { ...input, version: version ?? 0 }) : createAlertRule(input)),
    onSettled: () => {
      void client.invalidateQueries({ queryKey: ['alertRules'] })
      void client.invalidateQueries({ queryKey: ['alerts'] })
    },
    meta: { suppressGlobalError: true },
  })
}
export function useDeleteAlertRuleMutation() {
  const client = useQueryClient()
  return useMutation({
    mutationFn: ({ id, version }: { id: string; version: number }) => deleteAlertRule(id, version),
    onSettled: () => {
      void client.invalidateQueries({ queryKey: ['alertRules'] })
      void client.invalidateQueries({ queryKey: ['alerts'] })
    },
    meta: { suppressGlobalError: true },
  })
}
