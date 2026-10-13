import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { Button, Space } from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useCurrentUser } from '@/api/auth'
import {
  useDeleteStrategyMutation,
  useStrategiesQuery,
  type StrategySummary,
} from '@/api/strategies'
import { RequireRole } from '@/components/RequireRole'
import { RowActions } from '@/components/RowActions'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { StrategyFormDrawer } from './StrategyFormDrawer'
import { StrategyVersionsDrawer } from './StrategyVersionsDrawer'

export default function StrategiesPage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const { role } = useCurrentUser()
  const isAdmin = role === 'admin'
  const strategiesQuery = useStrategiesQuery()
  const deleteMutation = useDeleteStrategyMutation()

  const [formTarget, setFormTarget] = useState<{
    open: boolean
    strategy?: StrategySummary
  }>({
    open: false,
  })
  const [versionsTarget, setVersionsTarget] = useState<StrategySummary | undefined>()

  const handleDelete = (strategy: StrategySummary) => {
    deleteMutation.mutate(strategy.id, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      onError: (error) => void message.error(error.message),
    })
  }

  const columns: ManagedColumn<StrategySummary>[] = [
    {
      key: 'name',
      title: t('strategies.columns.name'),
      dataIndex: 'name',
      hideable: false,
    },
    {
      key: 'description',
      title: t('strategies.columns.description'),
      dataIndex: 'description',
      render: (value: string) => value || '—',
      exportValue: (record) => record.description || '—',
    },
    {
      key: 'currentVersion',
      title: t('strategies.columns.currentVersion'),
      dataIndex: 'currentVersion',
      render: (value: number) => (value > 0 ? `v${value}` : '—'),
      exportValue: (record) =>
        record.currentVersion > 0 ? `v${record.currentVersion}` : '—',
    },
    {
      key: 'updatedAt',
      title: t('strategies.columns.updatedAt'),
      dataIndex: 'updatedAt',
      render: (value: string) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.updatedAt),
    },
    {
      key: 'actions',
      title: t('common.actions'),
      hideable: false,

      render: (_value, record) => (
        <RowActions
          actions={[
            {
              key: 'versions',
              label: t('strategies.versions.title'),
              onClick: () => setVersionsTarget(record),
            },
            {
              key: 'edit',
              label: t('strategies.editMeta'),
              onClick: () => setFormTarget({ open: true, strategy: record }),
              visible: isAdmin,
            },
            {
              key: 'delete',
              label: t('common.delete'),
              danger: true,
              visible: isAdmin,
              onClick: () => handleDelete(record),
              confirm: { title: t('strategies.deleteConfirm') },
            },
          ]}
        />
      ),
    },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('strategies', columns)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      <Space
        style={{
          marginBottom: 16,
          width: '100%',
          justifyContent: 'space-between',
          flexShrink: 0,
        }}
      >
        <Space wrap>
          {strategiesQuery.isError && (
            <HintIcon type="error" content={<> {strategiesQuery.error.message} </>} />
          )}
          <ExportButton
            filename="strategies"
            columns={visibleColumns}
            rows={strategiesQuery.data ?? []}
            scope="list"
          />
          <ColumnSettingsButton {...pickerProps} />
        </Space>
        <RequireRole role="admin">
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setFormTarget({ open: true })}
          >
            {t('strategies.create')}
          </Button>
        </RequireRole>
      </Space>

      <div
        className="table-fill-container"
        style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
      >
        <ScrollTable<StrategySummary>
          rowKey="id"
          loading={strategiesQuery.isLoading}
          dataSource={strategiesQuery.data ?? []}
          pagination={false}
          columns={visibleColumns}
        />
      </div>

      <StrategyFormDrawer
        open={formTarget.open}
        strategy={formTarget.strategy}
        onClose={() => setFormTarget({ open: false })}
      />
      <StrategyVersionsDrawer
        open={versionsTarget !== undefined}
        strategy={versionsTarget}
        onClose={() => setVersionsTarget(undefined)}
      />
    </div>
  )
}
