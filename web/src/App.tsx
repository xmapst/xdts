import { App as AntdApp, ConfigProvider } from 'antd'
import enUS from 'antd/locale/en_US'
import zhCN from 'antd/locale/zh_CN'
import type { Locale } from 'antd/es/locale'
import { QueryClientProvider } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { RouterProvider } from 'react-router-dom'
import { queryClient } from '@/api/queryClient'
import { router } from '@/routes'

const antdLocales: Record<string, Locale> = {
  'zh-CN': zhCN,
  'en-US': enUS,
}

export default function App() {
  const { i18n } = useTranslation()
  const locale = antdLocales[i18n.language] ?? zhCN

  return (
    <ConfigProvider
      locale={locale}
      componentSize="small"
      theme={{
        token: { controlHeightSM: 28, padding: 12, paddingLG: 16, marginLG: 16 },
        components: {
          Layout: { headerHeight: 48 },
          Card: { bodyPadding: 16, bodyPaddingSM: 12, headerHeight: 44, headerHeightSM: 36 },
          Form: { itemMarginBottom: 12, verticalLabelPadding: '0 0 4px' },
          Table: { cellPaddingBlock: 8, cellPaddingBlockSM: 6, cellPaddingInlineSM: 8 },
          Descriptions: { itemPaddingBottom: 8 },
        },
      }}
      button={{ autoInsertSpace: false }}
    >
      <AntdApp>
        <QueryClientProvider client={queryClient}>
          <RouterProvider router={router} />
        </QueryClientProvider>
      </AntdApp>
    </ConfigProvider>
  )
}
