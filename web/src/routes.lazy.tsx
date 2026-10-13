// 各业务 feature 页面的懒加载入口，从 web/src/routes.tsx 中拆出来是为了让
// web/src/routes.tsx 只导出 router（react-refresh 要求一个文件要么只导出组件，
// 要么只导出非组件值，混在一起会让 oxlint 的 react/only-export-components 报警）。
import { lazy } from 'react'

export const DashboardPage = lazy(() => import('@/features/dashboard/DashboardPage'))
export const NodesPage = lazy(() => import('@/features/nodes/NodesPage'))
export const NodeDetailPage = lazy(() => import('@/features/nodes/NodeDetailPage'))
export const CentersPage = lazy(() => import('@/features/centers/CentersPage'))
export const EndpointsPage = lazy(() => import('@/features/endpoints/EndpointsPage'))
export const PipelinesPage = lazy(() => import('@/features/pipelines/PipelinesPage'))
export const PipelineCreatePage = lazy(() => import('@/features/pipelines/PipelineCreatePage'))
export const PipelineDetailPage = lazy(() => import('@/features/pipelines/PipelineDetailPage'))
export const LogsPage = lazy(() => import('@/features/logs/LogsPage'))
export const EventsPage = lazy(() => import('@/features/logs/EventsPage'))
export const ConflictsPage = lazy(() => import('@/features/logs/ConflictsPage'))
export const AlertsPage = lazy(() => import('@/features/alerts/AlertsPage'))
export const AuditPage = lazy(() => import('@/features/audit/AuditPage'))
export const SettingsPage = lazy(() => import('@/features/settings/SettingsPage'))
export const UsersPage = lazy(() => import('@/features/users/UsersPage'))
export const NotFoundPage = lazy(() => import('@/features/misc/NotFoundPage'))
