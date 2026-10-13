import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { useMemo, useState } from 'react'
import { Button, Drawer, Popconfirm, Select, Space, Tag } from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import {
  useRegisterCodesQuery,
  useRevokeRegisterCodeMutation,
  type RegisterCode,
  type RegisterCodeCreated,
} from '@/api/nodes'
import { useCentersQuery } from '@/api/centers'
import { RequireRole } from '@/components/RequireRole'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { RegisterCodeGenerateDrawer } from './RegisterCodeGenerateDrawer'
import { RegisterCodeRevealDrawer } from './RegisterCodeRevealDrawer'

interface RegisterCodesDrawerProps {
  open: boolean
  onClose: () => void
  defaultCenterId?: string
}

type DerivedStatus = 'unused' | 'used' | 'expired' | 'revoked'

function deriveStatus(code: RegisterCode): DerivedStatus {
  if (code.revokedAt) {
    return 'revoked'
  }
  if (code.usedCount > 0) {
    return 'used'
  }
  if (code.expiresAt && new Date(code.expiresAt).getTime() < Date.now()) {
    return 'expired'
  }
  return 'unused'
}

const STATUS_COLOR: Record<DerivedStatus, string> = {
  unused: 'processing',
  used: 'success',
  expired: 'default',
  revoked: 'error',
}

/** 注册码管理抽屉：生成、列表、作废（MGR-NODE-01、MGR-NODE-02）。 */
export function RegisterCodesDrawer({
  open,
  onClose,
  defaultCenterId,
}: RegisterCodesDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [centerFilter, setCenterFilter] = useState<string | undefined>(defaultCenterId)
  const [generateOpen, setGenerateOpen] = useState(false)
  const [revealed, setRevealed] = useState<RegisterCodeCreated | null>(null)

  // 抽屉关闭时不发起查询，避免节点列表页仅仅渲染出这个（默认关闭的）抽屉就
  // 在后台悄悄多打两个请求。
  const centersQuery = useCentersQuery(open)
  const codesQuery = useRegisterCodesQuery(centerFilter, open)
  const revokeMutation = useRevokeRegisterCodeMutation()

  const centerNameById = useMemo(() => {
    const map = new Map<string, string>()
    for (const center of centersQuery.data ?? []) {
      map.set(center.id, `${center.name}（${center.code}）`)
    }
    return map
  }, [centersQuery.data])

  const handleRevoke = (id: string) => {
    revokeMutation.mutate(id, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      onError: (error) => void message.error(error.message),
    })
  }

  const columns: ManagedColumn<RegisterCode>[] = [
    {
      key: 'center',
      title: t('nodes.registerCodes.columns.center'),
      dataIndex: 'centerId',
      render: (centerId: string) => centerNameById.get(centerId) ?? centerId,
      exportValue: (record) => centerNameById.get(record.centerId) ?? record.centerId,
    },
    {
      key: 'presetName',
      title: t('nodes.registerCodes.columns.presetName'),
      dataIndex: 'presetName',
      render: (value: string | null) => value ?? t('common.unnamed'),
      exportValue: (record) => record.presetName ?? t('common.unnamed'),
    },
    {
      key: 'labels',
      title: t('nodes.registerCodes.columns.labels'),
      dataIndex: 'presetLabels',
      render: (labels: string[]) =>
        labels.length > 0 ? (
          <Space size={4} wrap>
            {labels.map((label) => (
              <Tag key={label}>{label}</Tag>
            ))}
          </Space>
        ) : (
          t('common.none')
        ),
      exportValue: (record) =>
        record.presetLabels.length > 0 ? record.presetLabels.join(', ') : t('common.none'),
    },
    {
      key: 'status',
      title: t('nodes.registerCodes.columns.status'),
      render: (_value, record) => {
        const status = deriveStatus(record)
        return (
          <Tag color={STATUS_COLOR[status]}>
            {t(`nodes.registerCodes.status.${status}`)}
          </Tag>
        )
      },
      exportValue: (record) => t(`nodes.registerCodes.status.${deriveStatus(record)}`),
    },
    {
      key: 'usedCount',
      title: t('nodes.registerCodes.columns.usedCount'),
      dataIndex: 'usedCount',
    },
    {
      key: 'expiresAt',
      title: t('nodes.registerCodes.columns.expiresAt'),
      dataIndex: 'expiresAt',
      render: (value: string | null) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.expiresAt),
    },
    {
      key: 'createdAt',
      title: t('nodes.registerCodes.columns.createdAt'),
      dataIndex: 'createdAt',
      hideable: false,
      render: (value: string) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.createdAt),
    },
    {
      key: 'actions',
      title: t('common.actions'),
      hideable: false,
      render: (_value, record) => {
        const status = deriveStatus(record)
        return (
          <RequireRole role="admin">
            <Popconfirm
              title={t('nodes.registerCodes.revokeConfirm')}
              disabled={status !== 'unused'}
              onConfirm={() => handleRevoke(record.id)}
            >
              <Button type="link" danger disabled={status !== 'unused'}>
                {t('nodes.registerCodes.revoke')}
              </Button>
            </Popconfirm>
          </RequireRole>
        )
      },
    },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('registerCodes', columns)

  return (
    <Drawer
      title={t('nodes.registerCodes.title')}
      open={open}
      onClose={onClose}
      size={720}
      className="register-codes-drawer"
      extra={
        <RequireRole role="admin">
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setGenerateOpen(true)}
          >
            {t('nodes.registerCodes.generate')}
          </Button>
        </RequireRole>
      }
    >
      {}
      <div
        style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}
      >
        <Space
          style={{
            marginBottom: 16,
            width: '100%',
            justifyContent: 'space-between',
            flexShrink: 0,
          }}
          wrap
        >
          <Select
            allowClear
            placeholder={t('nodes.registerCodes.center')}
            style={{ width: 240 }}
            value={centerFilter}
            onChange={setCenterFilter}
            loading={centersQuery.isLoading}
            options={(centersQuery.data ?? []).map((center) => ({
              value: center.id,
              label: `${center.name}（${center.code}）`,
            }))}
          />
          <Space wrap>
            <ExportButton
              filename="register-codes"
              columns={visibleColumns}
              rows={codesQuery.data ?? []}
              scope="list"
            />
            <ColumnSettingsButton {...pickerProps} />
          </Space>
        </Space>

        <div
          className="table-fill-container"
          style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
        >
          <ScrollTable<RegisterCode>
            rowKey="id"
            size="small"
            loading={codesQuery.isLoading}
            dataSource={codesQuery.data ?? []}
            locale={{ emptyText: t('nodes.registerCodes.empty') }}
            pagination={{ pageSize: 10, hideOnSinglePage: true }}
            columns={visibleColumns}
          />
        </div>
      </div>

      <RegisterCodeGenerateDrawer
        open={generateOpen}
        onClose={() => setGenerateOpen(false)}
        onCreated={setRevealed}
        defaultCenterId={centerFilter}
      />
      <RegisterCodeRevealDrawer created={revealed} onClose={() => setRevealed(null)} />
    </Drawer>
  )
}
