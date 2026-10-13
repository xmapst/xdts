import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { useState } from 'react'
import { Button, Space } from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useCurrentUser } from '@/api/auth'
import { useCentersQuery, useDeleteCenterMutation, type CenterSummary } from '@/api/centers'
import { RequireRole } from '@/components/RequireRole'
import { RowActions } from '@/components/RowActions'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { CenterFormDrawer } from './CenterFormDrawer'
import { OriginUuidsDrawer } from './OriginUuidsDrawer'

export default function CentersPage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const { role } = useCurrentUser()
  const isAdmin = role === 'admin'
  const centersQuery = useCentersQuery()
  const deleteMutation = useDeleteCenterMutation()

  const [formTarget, setFormTarget] = useState<{ open: boolean; center?: CenterSummary }>({
    open: false,
  })
  const [uuidsTarget, setUuidsTarget] = useState<CenterSummary | undefined>()

  const handleDelete = (center: CenterSummary) => {
    deleteMutation.mutate(center.id, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      onError: (error) => void message.error(error.message),
    })
  }

  const columns: ManagedColumn<CenterSummary>[] = [
    { key: 'code', title: t('centers.columns.code'), dataIndex: 'code', hideable: false },
    { key: 'name', title: t('centers.columns.name'), dataIndex: 'name' },
    {
      key: 'region',
      title: t('centers.columns.region'),
      dataIndex: 'region',
      render: (v: string | null) => v ?? '—',
      exportValue: (record) => record.region ?? '—',
    },
    {
      key: 'originUuidCount',
      title: t('centers.columns.originUuidCount'),
      dataIndex: 'originUuidCount',
    },
    {
      key: 'actions',
      title: t('common.actions'),
      hideable: false,
      render: (_value, record) => (
        <RowActions
          maxInline={3}
          actions={[
            {
              key: 'originUuids',
              label: t('centers.originUuids.title'),
              onClick: () => setUuidsTarget(record),
            },
            {
              key: 'edit',
              label: t('common.edit'),
              onClick: () => setFormTarget({ open: true, center: record }),
              visible: isAdmin,
            },
            {
              key: 'delete',
              label: t('common.delete'),
              danger: true,
              visible: isAdmin,
              onClick: () => handleDelete(record),
              confirm: { title: t('centers.deleteConfirm') },
            },
          ]}
        />
      ),
    },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('centers', columns)

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
          <ExportButton
            filename="centers"
            columns={visibleColumns}
            rows={centersQuery.data ?? []}
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
            {t('centers.create')}
          </Button>
        </RequireRole>
      </Space>

      <div
        className="table-fill-container"
        style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
      >
        <ScrollTable<CenterSummary>
          rowKey="id"
          loading={centersQuery.isLoading}
          dataSource={centersQuery.data ?? []}
          pagination={false}
          columns={visibleColumns}
        />
      </div>

      <CenterFormDrawer
        open={formTarget.open}
        center={formTarget.center}
        onClose={() => setFormTarget({ open: false })}
      />
      <OriginUuidsDrawer
        open={uuidsTarget !== undefined}
        center={uuidsTarget}
        onClose={() => setUuidsTarget(undefined)}
      />
    </div>
  )
}
