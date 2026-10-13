import { DownloadOutlined } from '@ant-design/icons'
import { Button, Space, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import { exportRowsToCsv } from '@/utils/exportCsv'
import type { ManagedColumn } from '@/utils/tableColumns'

interface ExportButtonProps<T> {
  /** 不需要带 .csv 后缀。 */
  filename: string
  columns: ManagedColumn<T>[]
  rows: T[]
  /** 'page' 用于服务端分页表格（导出范围只是"当前页"这一部分）；'list' 用于
   * 当前本来就整表渲染、没有分页的表格（导出范围就是"当前列表"，不是阉割版，
   * 但仍然用同一套措辞保持一致，避免用户误以为分页表格的导出是"全部"）。 */
  scope: 'page' | 'list'
}

export function ExportButton<T>({ filename, columns, rows, scope }: ExportButtonProps<T>) {
  const { t } = useTranslation()
  const scopeText = t(scope === 'page' ? 'common.exportCurrentPage' : 'common.exportCurrentList', {
    count: rows.length,
  })

  return (
    <Space size={4}>
      <Button
        icon={<DownloadOutlined />}
        disabled={rows.length === 0}
        onClick={() => exportRowsToCsv(filename, columns, rows)}
      >
        {t('common.export')}
      </Button>
      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
        {scopeText}
      </Typography.Text>
    </Space>
  )
}
