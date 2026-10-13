import { Layout } from 'antd'
import { Outlet } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import { useUIStore } from '@/stores/uiStore'
import { SideNav } from './SideNav'
import { TopBar } from './TopBar'

const { Header, Sider, Content } = Layout

/** 登录后主布局：侧边导航 + 顶栏，子路由通过 <Outlet /> 渲染在 Content 中。 */
export function AppLayout() {
  const { t } = useTranslation()
  const sidebarCollapsed = useUIStore((state) => state.sidebarCollapsed)
  const setSidebarCollapsed = useUIStore((state) => state.setSidebarCollapsed)

  return (
    <Layout style={{ height: '100vh' }}>
      <Sider
        collapsible
        collapsed={sidebarCollapsed}
        onCollapse={setSidebarCollapsed}
        breakpoint="md"
        onBreakpoint={(broken) => {
          if (broken) setSidebarCollapsed(true)
        }}
      >
        {/* 菜单使用紧凑行距；短视口的溢出由侧栏独立滚动，不撑高页面。 */}
        <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
          <div
            style={{
              height: 40,
              margin: 12,
              flexShrink: 0,
              color: '#fff',
              fontWeight: 600,
              fontSize: sidebarCollapsed ? 14 : 16,
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
            }}
          >
            {sidebarCollapsed ? 'xdts' : t('common.appName')}
          </div>
          <div
            className="side-nav-scroll"
            style={{ flex: '1 1 auto', minHeight: 0, overflowY: 'auto', overflowX: 'hidden' }}
          >
            <SideNav collapsed={sidebarCollapsed} />
          </div>
        </div>
      </Sider>
      <Layout style={{ minHeight: 0 }}>
        <Header style={{ background: '#fff', padding: 0, flex: '0 0 auto' }}>
          <TopBar />
        </Header>
        <Content
          className="app-content"
          style={{
            flex: '1 1 auto',
            minHeight: 0,
            margin: 12,
            background: '#fff',
            padding: 12,
            overflow: 'auto',
          }}
        >
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  )
}
