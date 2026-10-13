import { Spin } from 'antd'
import type { ComponentType, LazyExoticComponent } from 'react'
import { Suspense } from 'react'
import { createBrowserRouter, Navigate } from 'react-router-dom'
import { AppLayout } from '@/components/layout/AppLayout'
import { AuthGuard } from '@/components/AuthGuard'
import LoginPage from '@/features/auth/LoginPage'
import ChangePasswordPage from '@/features/auth/ChangePasswordPage'
import {
  AlertsPage,
  AuditPage,
  CentersPage,
  ConflictsPage,
  DashboardPage,
  EndpointsPage,
  EventsPage,
  LogsPage,
  NodeDetailPage,
  NodesPage,
  NotFoundPage,
  PipelineCreatePage,
  PipelineDetailPage,
  PipelinesPage,
  SettingsPage,
  UsersPage,
} from '@/routes.lazy'

function pageFallback() {
  return (
    <div style={{ display: 'flex', justifyContent: 'center', padding: 48 }}>
      <Spin size="large" />
    </div>
  )
}

function withSuspense(Component: LazyExoticComponent<ComponentType>) {
  return (
    <Suspense fallback={pageFallback()}>
      <Component />
    </Suspense>
  )
}

// 登录页与主布局同步加载（首屏必需）；
// 各业务 feature 页面按路由懒加载（见 web/src/routes.lazy.tsx），避免随着后续
// 波次页面增多，单个 JS chunk 无限膨胀。
export const router = createBrowserRouter([
  { path: '/login', element: <LoginPage /> },
  {
    path: '/',
    element: <AuthGuard />,
    children: [
      { index: true, element: <Navigate to="/dashboard" replace /> },
      { path: 'change-password', element: <ChangePasswordPage /> },
      {
        element: <AppLayout />,
        children: [
          { path: 'dashboard', element: withSuspense(DashboardPage) },
          { path: 'nodes', element: withSuspense(NodesPage) },
          { path: 'nodes/:id', element: withSuspense(NodeDetailPage) },
          { path: 'centers', element: withSuspense(CentersPage) },
          { path: 'endpoints', element: withSuspense(EndpointsPage) },
          { path: 'strategies', element: <Navigate to="/pipelines" replace /> },
          { path: 'pipelines', element: withSuspense(PipelinesPage) },
          { path: 'pipelines/new', element: withSuspense(PipelineCreatePage) },
          { path: 'pipelines/:id', element: withSuspense(PipelineDetailPage) },

          { path: 'topologies', element: <Navigate to="/pipelines" replace /> },
          { path: 'topology', element: <Navigate to="/pipelines" replace /> },
          { path: 'logs', element: withSuspense(LogsPage) },
          { path: 'events', element: withSuspense(EventsPage) },
          { path: 'conflicts', element: withSuspense(ConflictsPage) },
          { path: 'alerts', element: withSuspense(AlertsPage) },
          { path: 'audit', element: withSuspense(AuditPage) },
          { path: 'settings', element: withSuspense(SettingsPage) },
          { path: 'users', element: withSuspense(UsersPage) },
        ],
      },
    ],
  },
  { path: '*', element: withSuspense(NotFoundPage) },
])
