import { useQuery } from '@tanstack/react-query'
import { useCurrentUser } from './auth'
import * as generated from './generated'
import type { PageQuery, Paginated } from './types'
import { isApiMockEnabled } from '@/mocks/config'
import * as auditMock from '@/mocks/auditMock'

export type AuditLogEntry = generated.GeneratedAuditLogEntry
export interface AuditQuery extends PageQuery {
  actor?: string
  userId?: string
  action?: string
  targetType?: string
  targetId?: string
  since?: string
  until?: string
}
export function listAuditLogs(query: AuditQuery): Promise<Paginated<AuditLogEntry>> {
  if (isApiMockEnabled()) return auditMock.listAuditLogs(query)
  return generated.listAuditLogs(query)
}
export function useAuditLogsQuery(query: AuditQuery) {
  const { role } = useCurrentUser()
  return useQuery({
    queryKey: ['auditLogs', query],
    queryFn: () => listAuditLogs(query),
    enabled: role === 'admin',
  })
}
