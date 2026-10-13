// 测试用的通用渲染帮助函数：antd ConfigProvider（zh-CN locale，与生产 web/src/App.tsx 一致，
// 保证 Popconfirm / Table 分页等内置文案与真实界面一致）+ AntdApp（message/modal
// 上下文）+ 独立 QueryClient + MemoryRouter。抽出来是因为节点 / 中心页面测试都要用
// 到同一套 provider 组合，重复写会掩盖测试本身的意图。
import type { ReactElement } from 'react'
import { App as AntdApp, ConfigProvider } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'

export function renderWithProviders(ui: ReactElement, initialEntries: string[] = ['/']) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  })
  return render(
    <ConfigProvider locale={zhCN} button={{ autoInsertSpace: false }}>
      <AntdApp>
        <QueryClientProvider client={queryClient}>
          <MemoryRouter initialEntries={initialEntries}>{ui}</MemoryRouter>
        </QueryClientProvider>
      </AntdApp>
    </ConfigProvider>,
  )
}
