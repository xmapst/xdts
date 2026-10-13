import type { AuditLogEntry, AuditQuery } from '@/api/audit'
import type { Paginated } from '@/api/types'
import { ApiError } from '@/api/client'
import { fetchMe } from './authMock'

const entries: readonly AuditLogEntry[] = [
  {
    id: '01a12001-0000-7000-8000-000000000003',
    ts: '2026-10-09T02:02:00Z',
    userId: '01990001-0000-7000-8000-000000000001',
    actor: 'admin',
    action: 'pipeline_start',
    targetType: 'pipeline',
    targetId: '01990002-0000-7000-8000-000000000001',
    success: true,
    detail: { username: 'admin', success: true, changes: { desiredState: 'running' } },
  },
  {
    id: '01a12001-0000-7000-8000-000000000002',
    ts: '2026-10-09T02:01:00Z',
    userId: null,
    actor: 'unknown-attempt',
    action: 'login_failure',
    targetType: 'user',
    targetId: null,
    success: false,
    detail: { username: 'unknown-attempt', success: false, reason: 'login rejected' },
  },
  {
    id: '01a12001-0000-7000-8000-000000000001',
    ts: '2026-10-09T02:00:00Z',
    userId: '01990001-0000-7000-8000-000000000001',
    actor: 'historical-admin',
    action: 'endpoint_change',
    targetType: 'endpoint',
    targetId: '01990003-0000-7000-8000-000000000001',
    success: null,
    detail: { username: 'historical-admin' },
  },
]
export async function listAuditLogs(query: AuditQuery): Promise<Paginated<AuditLogEntry>> {
  if ((await fetchMe()).user?.role !== 'admin')
    throw new ApiError('没有权限执行此操作', 403, 'forbidden')
  if (
    !Number.isInteger(query.page) ||
    query.page < 1 ||
    !Number.isInteger(query.pageSize) ||
    query.pageSize < 1 ||
    query.pageSize > 200 ||
    (query.page - 1) * query.pageSize > 1000000
  )
    throw new ApiError('审计分页参数不合法', 400, 'invalid_audit_query')
  const actor = query.actor?.trim()
  const items = entries.filter(
    (entry) =>
      (!actor || entry.actor === actor) &&
      (!query.userId || entry.userId === query.userId) &&
      (!query.action || entry.action === query.action) &&
      (!query.targetType || entry.targetType === query.targetType) &&
      (!query.targetId || entry.targetId === query.targetId) &&
      (!query.since || entry.ts >= query.since) &&
      (!query.until || entry.ts <= query.until),
  )
  const start = (query.page - 1) * query.pageSize
  return {
    items: structuredClone(items.slice(start, start + query.pageSize)),
    total: items.length,
    page: query.page,
    pageSize: query.pageSize,
  }
}
