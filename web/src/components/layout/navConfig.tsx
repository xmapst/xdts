import {
  ApartmentOutlined,
  AppstoreOutlined,
  AuditOutlined,
  BellOutlined,
  ClusterOutlined,
  DashboardOutlined,
  DatabaseOutlined,
  FileTextOutlined,
  MonitorOutlined,
  NodeIndexOutlined,
  SettingOutlined,
  ToolOutlined,
  UserOutlined,
  WarningOutlined,
} from '@ant-design/icons'
import type { ReactNode } from 'react'
import type { TranslationSchema } from '@/i18n/locales/zh-CN'
import type { UserRole } from '@/api/auth'

/** 侧边栏分组 key；未列出（目前只有仪表盘）表示不分组，置顶展示。 */
export type NavGroupKey = keyof TranslationSchema['nav']['groups']

export interface NavItem {
  key: string
  path: string
  // 排掉 'groups'：它是嵌套对象不是文案叶子，允许它意味着允许拼出
  // `t('nav.groups')` 这种取不到文案的 key。tsc 在 CustomTypeOptions 归属
  // 修好后会在 SideNav 那边报出来（见 web/src/i18n/react-i18next.d.ts）。
  labelKey: Exclude<keyof TranslationSchema['nav'], 'groups'>
  icon: ReactNode
  /** 未列出表示所有已登录角色可见；列出则仅这些角色可见（用于隐藏写操作入口）。 */
  roles?: UserRole[]
  /** 未列出表示不分组（目前只有仪表盘），置顶展示在所有分组之前。 */
  group?: NavGroupKey
}

export const navGroupOrder: {
  key: NavGroupKey
  labelKey: keyof TranslationSchema['nav']['groups']
  icon: ReactNode
}[] = [
  { key: 'resources', labelKey: 'resources', icon: <AppstoreOutlined /> },
  { key: 'observability', labelKey: 'observability', icon: <MonitorOutlined /> },
  { key: 'system', labelKey: 'system', icon: <ToolOutlined /> },
]

export const navItems: NavItem[] = [
  { key: 'dashboard', path: '/dashboard', labelKey: 'dashboard', icon: <DashboardOutlined /> },
  {
    key: 'centers',
    path: '/centers',
    labelKey: 'centers',
    icon: <ApartmentOutlined />,
    group: 'resources',
  },
  {
    key: 'nodes',
    path: '/nodes',
    labelKey: 'nodes',
    icon: <ClusterOutlined />,
    group: 'resources',
  },
  {
    key: 'dataEndpoints',
    path: '/endpoints',
    labelKey: 'dataEndpoints',
    icon: <DatabaseOutlined />,
    group: 'resources',
  },
  {
    key: 'pipelines',
    path: '/pipelines',
    labelKey: 'pipelines',
    icon: <NodeIndexOutlined />,
    group: 'resources',
  },

  {
    key: 'logs',
    path: '/logs',
    labelKey: 'logs',
    icon: <FileTextOutlined />,
    group: 'observability',
  },
  {
    key: 'events',
    path: '/events',
    labelKey: 'events',
    icon: <WarningOutlined />,
    group: 'observability',
  },
  {
    key: 'conflicts',
    path: '/conflicts',
    labelKey: 'conflicts',
    icon: <WarningOutlined />,
    group: 'observability',
  },
  {
    key: 'alerts',
    path: '/alerts',
    labelKey: 'alerts',
    icon: <BellOutlined />,
    group: 'observability',
  },
  {
    key: 'audit',
    path: '/audit',
    labelKey: 'audit',
    icon: <AuditOutlined />,
    roles: ['admin'],
    group: 'system',
  },
  {
    key: 'users',
    path: '/users',
    labelKey: 'users',
    icon: <UserOutlined />,
    roles: ['admin'],
    group: 'system',
  },
  {
    key: 'settings',
    path: '/settings',
    labelKey: 'settings',
    icon: <SettingOutlined />,
    roles: ['admin'],
    group: 'system',
  },
]
