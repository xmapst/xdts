import { Spin } from 'antd'
import { Navigate, Outlet, useLocation } from 'react-router-dom'
import { useCurrentUser } from '@/api/auth'

/**
 * 鉴权路由守卫：未登录跳转登录页（需求 MGR-USR-08 前置条件）。
 * 挂在需要登录才能访问的路由树根节点上，子路由通过 <Outlet /> 渲染。
 */
export function AuthGuard() {
  const { isLoading, isAuthenticated, user } = useCurrentUser()
  const location = useLocation()

  if (isLoading) {
    return (
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          height: '100vh',
        }}
      >
        <Spin size="large" />
      </div>
    )
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace state={{ from: location }} />
  }

  if (user?.mustChangePassword && location.pathname !== '/change-password') {
    return <Navigate to="/change-password" replace />
  }

  return <Outlet />
}
