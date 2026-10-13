import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { Button, Input, Select, Space, Tag, Typography } from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useCurrentUser } from '@/api/auth'
import {
  useEndpointsQuery,
  useDeleteEndpointMutation,
  type EndpointListQuery,
  type EndpointSummary,
  type EndpointUpdateResult,
} from '@/api/endpoints'
import { useCentersQuery } from '@/api/centers'
import { RequireRole } from '@/components/RequireRole'
import { RowActions } from '@/components/RowActions'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { EndpointFormDrawer } from './EndpointFormDrawer'
import { EndpointAffectedPipelinesDrawer } from './EndpointAffectedPipelinesDrawer'
import { ProbeConnectionDrawer } from './ProbeConnectionDrawer'
import { EndpointCatalogDrawer } from './EndpointCatalogDrawer'

const DEFAULT_PAGE_SIZE = 20
const PAGE_SIZE_OPTIONS = ['10', '20', '50', '100']
/** 与 ProbeResultPanel 的过期阈值保持一致，供列表页「最近探测」列做同样的展示。 */
const STALE_THRESHOLD_MS = 3600_000

export default function EndpointsPage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const { role } = useCurrentUser()
  const isAdmin = role === 'admin'
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE)
  const [centerId, setCenterId] = useState<string | undefined>()
  const [keyword, setKeyword] = useState('')

  const [formTarget, setFormTarget] = useState<{
    open: boolean
    endpoint?: EndpointSummary
  }>({
    open: false,
  })
  /**
   * 编辑保存成功后要展示的"受影响线路"（MGR-DS-05 后半）。放在页面而不是表单
   * 弹窗里，同 NodesPage 的 blocked 状态：表单弹窗保存成功就关闭了，后续弹窗
   * 由页面开。
   */
  const [affected, setAffected] = useState<{
    endpointName: string
    result: EndpointUpdateResult
  } | null>(null)
  const [probeTarget, setProbeTarget] = useState<EndpointSummary | undefined>()
  const [catalogTarget, setCatalogTarget] = useState<EndpointSummary | undefined>()

  const query: EndpointListQuery = {
    page,
    pageSize,
    centerId,
    keyword: keyword || undefined,
  }
  const endpointsQuery = useEndpointsQuery(query)
  const centersQuery = useCentersQuery()
  const deleteMutation = useDeleteEndpointMutation()

  const centerNameById = new Map(
    (centersQuery.data ?? []).map((c) => [c.id, `${c.name}（${c.code}）`]),
  )

  const handleDelete = (endpoint: EndpointSummary) => {
    deleteMutation.mutate(endpoint.id, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      onError: (error) => void message.error(error.message),
    })
  }

  const columns: ManagedColumn<EndpointSummary>[] = [
    {
      key: 'name',
      title: t('dataEndpoints.columns.name'),
      dataIndex: 'name',
      hideable: false,
    },
    {
      key: 'center',
      title: t('dataEndpoints.columns.center'),
      dataIndex: 'centerId',
      render: (id: string) => centerNameById.get(id) ?? id,
      exportValue: (record) => centerNameById.get(record.centerId) ?? record.centerId,
    },
    {
      key: 'address',
      title: t('dataEndpoints.columns.address'),
      render: (_value, record) => `${record.host}:${record.port}`,
      exportValue: (record) => `${record.host}:${record.port}`,
    },
    {
      key: 'version',
      title: t('dataEndpoints.columns.version'),
      dataIndex: 'version',
      render: (value: string) => value || '—',
      exportValue: (record) => record.version || '—',
    },
    {
      key: 'serverUuid',
      title: t('dataEndpoints.columns.serverUuid'),
      dataIndex: 'serverUuid',
      render: (value: string | null) => (
        <Typography.Text code={Boolean(value)}>{value ?? '—'}</Typography.Text>
      ),
      exportValue: (record) => record.serverUuid ?? '—',
    },
    {
      key: 'lastProbe',
      title: t('dataEndpoints.columns.lastProbe'),
      dataIndex: 'lastProbeAt',
      render: (value: string | null) => {
        if (!value) {
          return <Tag>{t('dataEndpoints.freshness.never')}</Tag>
        }
        const stale = Date.now() - new Date(value).getTime() > STALE_THRESHOLD_MS
        return (
          <Space size={4}>
            <span>{formatDateTime(value)}</span>
            {stale && <Tag color="warning">{t('dataEndpoints.freshness.staleSuffix')}</Tag>}
          </Space>
        )
      },
      exportValue: (record) =>
        record.lastProbeAt
          ? formatDateTime(record.lastProbeAt)
          : t('dataEndpoints.freshness.never'),
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
              key: 'probe',
              label: t('dataEndpoints.probe.action'),
              onClick: () => setProbeTarget(record),
            },
            {
              key: 'catalog',
              label: t('dataEndpoints.catalog.title'),
              onClick: () => setCatalogTarget(record),
            },
            {
              key: 'edit',
              label: t('common.edit'),
              onClick: () => setFormTarget({ open: true, endpoint: record }),
              visible: isAdmin,
            },
            {
              key: 'delete',
              label: t('common.delete'),
              danger: true,
              visible: isAdmin,
              onClick: () => handleDelete(record),
              confirm: { title: t('dataEndpoints.deleteConfirm') },
            },
          ]}
        />
      ),
    },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('endpoints', columns)

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
          <Select
            allowClear
            placeholder={t('dataEndpoints.filterCenter')}
            style={{ width: 200 }}
            value={centerId}
            onChange={(value) => {
              setCenterId(value)
              setPage(1)
            }}
            loading={centersQuery.isLoading}
            options={(centersQuery.data ?? []).map((center) => ({
              value: center.id,
              label: `${center.name}（${center.code}）`,
            }))}
          />
          <Input.Search
            allowClear
            placeholder={t('dataEndpoints.filterKeyword')}
            style={{ width: 260 }}
            onSearch={(value) => {
              setKeyword(value)
              setPage(1)
            }}
          />
        </Space>
        <Space wrap>
          {endpointsQuery.isError && (
            <HintIcon type="error" content={<> {endpointsQuery.error.message} </>} />
          )}
          <ExportButton
            filename="endpoints"
            columns={visibleColumns}
            rows={endpointsQuery.data?.items ?? []}
            scope="page"
          />
          <ColumnSettingsButton {...pickerProps} />
          <RequireRole role="admin">
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setFormTarget({ open: true })}
            >
              {t('dataEndpoints.create')}
            </Button>
          </RequireRole>
        </Space>
      </Space>

      <div
        className="table-fill-container"
        style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
      >
        <ScrollTable<EndpointSummary>
          rowKey="id"
          loading={endpointsQuery.isLoading}
          dataSource={endpointsQuery.data?.items ?? []}
          pagination={{
            current: page,
            pageSize,
            total: endpointsQuery.data?.total ?? 0,
            showSizeChanger: true,
            showQuickJumper: true,
            pageSizeOptions: PAGE_SIZE_OPTIONS,
            onChange: (nextPage, nextPageSize) => {
              setPage(nextPage)
              setPageSize(nextPageSize)
            },
          }}
          columns={visibleColumns}
        />
      </div>

      <EndpointFormDrawer
        open={formTarget.open}
        endpoint={formTarget.endpoint}
        onClose={() => setFormTarget({ open: false })}
        onUpdated={(result) => setAffected({ endpointName: result.name, result })}
      />
      <EndpointAffectedPipelinesDrawer
        open={affected !== null}
        endpointName={affected?.endpointName ?? ''}
        affected={affected?.result.affectedPipelines ?? []}
        // `=== true` 不是多余：后端该字段是 omitempty，为假时整个键不出现，
        // 而 undefined 在这里必须当成"不是未知"，不能靠隐式真值。
        affectedUnknown={affected?.result.affectedUnknown === true}
        onClose={() => setAffected(null)}
      />
      <ProbeConnectionDrawer
        open={probeTarget !== undefined}
        endpoint={probeTarget}
        onClose={() => setProbeTarget(undefined)}
      />
      <EndpointCatalogDrawer
        open={catalogTarget !== undefined}
        endpoint={catalogTarget}
        onClose={() => setCatalogTarget(undefined)}
      />
    </div>
  )
}
