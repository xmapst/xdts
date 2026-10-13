import { SettingOutlined } from '@ant-design/icons'
import { Button, Checkbox, Popover, Space } from 'antd'
import type { ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import type { ColumnPickerProps } from '@/utils/useColumnVisibility'

export function ColumnSettingsButton<T>({ columns, hiddenKeys, onChange }: ColumnPickerProps<T>) {
  const { t } = useTranslation()
  const hideableColumns = columns.filter((column) => column.hideable !== false)
  const checkedKeys = hideableColumns
    .filter((column) => !hiddenKeys.includes(column.key))
    .map((column) => column.key)

  const content = (
    <Checkbox.Group
      style={{ display: 'flex' }}
      value={checkedKeys}
      onChange={(nextChecked) => {
        const checkedSet = new Set(nextChecked as string[])
        const nextHidden = hideableColumns
          .filter((column) => !checkedSet.has(column.key))
          .map((column) => column.key)
        onChange(nextHidden)
      }}
    >
      <Space orientation="vertical">
        {hideableColumns.map((column) => (
          <Checkbox key={column.key} value={column.key}>
            {column.title as ReactNode}
          </Checkbox>
        ))}
      </Space>
    </Checkbox.Group>
  )

  return (
    <Popover
      content={content}
      title={t('common.columnSettings')}
      trigger="click"
      placement="bottomRight"
    >
      <Button icon={<SettingOutlined />}>{t('common.columnSettings')}</Button>
    </Popover>
  )
}
