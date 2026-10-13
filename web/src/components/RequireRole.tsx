import type { ReactNode } from 'react'
import type { UserRole } from '@/api/auth'
import { useCurrentUser } from '@/api/auth'

interface RequireRoleProps {
  role: UserRole | UserRole[]
  children: ReactNode
  fallback?: ReactNode
}

/**
 * 按角色隐藏写操作入口（需求 MGR-USR-08）。这里只做前端展示层隐藏，
 * 真正的写权限判定以后端返回的 403 为准；组件仅用于改善只读用户的体验。
 */
export function RequireRole({ role, children, fallback = null }: RequireRoleProps) {
  const { role: currentRole } = useCurrentUser()
  const allowed = Array.isArray(role) ? role : [role]
  if (!currentRole || !allowed.includes(currentRole)) {
    return <>{fallback}</>
  }
  return <>{children}</>
}
