import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { Button, Input, Select, Space, Tag } from 'antd'
import { KeyOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { Link } from 'react-router-dom'
import {
  useNodesQuery,
  useUpdateNodeMutation,
  useRevokeNodeTokenMutation,
  useDeregisterNodeMutation,
  parseNodeDeregisterBlockers,
  type NodeDeregisterBlocker,
  type NodeListQuery,
  type NodeStatus,
  type NodeSummary,
} from '@/api/nodes'
import { ApiError } from '@/api/client'
import { useCentersQuery } from '@/api/centers'
import { RequireRole } from '@/components/RequireRole'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { NodeStatusTag } from './nodeStatusTag'
import { RegisterCodesDrawer } from './RegisterCodesDrawer'
import { NodeRowActions } from './NodeRowActions'
import { NodeEditDrawer } from './NodeEditDrawer'
import { NodeDeregisterBlockedNotice } from './NodeDeregisterBlockedNotice'
import { enumLabel } from '@/utils/i18nEnum'

const PAGE_SIZE_OPTIONS = ['10', '20', '50', '100']

const NODE_STATUSES: NodeStatus[] = [
  'registering',
  'online',
  'offline',
  'disabled',
  'revoked',
]
const DEFAULT_PAGE_SIZE = 20

export default function NodesPage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE)
  const [centerId, setCenterId] = useState<string | undefined>()
  const [status, setStatus] = useState<NodeStatus | undefined>()
  const [keyword, setKeyword] = useState('')
  const [registerCodesOpen, setRegisterCodesOpen] = useState(false)
  const [editingNode, setEditingNode] = useState<NodeSummary | null>(null)
  // 注销被 409 挡住时的阻断清单；null 表示没有待展示的阻断（弹窗关闭）。
  const [blocked, setBlocked] = useState<{
    nodeName: string
    blockers: NodeDeregisterBlocker[]
  } | null>(null)

  const query: NodeListQuery = {
    page,
    pageSize,
    centerId,
    status,
    keyword: keyword || undefined,
  }
  const nodesQuery = useNodesQuery(query)
  const centersQuery = useCentersQuery()
  const updateMutation = useUpdateNodeMutation()
  const revokeMutation = useRevokeNodeTokenMutation()
  const deregisterMutation = useDeregisterNodeMutation()

  const centerNameById = new Map(
    (centersQuery.data ?? []).map((c) => [c.id, `${c.name}（${c.code}）`]),
  )

  const handleToggleDisabled = (node: NodeSummary) => {
    updateMutation.mutate(
      { id: node.id, input: { disabled: node.status !== 'disabled' } },
      {
        onSuccess: () => void message.success(t('common.operationSucceeded')),
        onError: (error) => void message.error(error.message),
      },
    )
  }

  const handleRevokeToken = (node: NodeSummary) => {
    revokeMutation.mutate(node.id, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      onError: (error) => void message.error(error.message),
    })
  }

  const handleDeregister = (node: NodeSummary) => {
    deregisterMutation.mutate(node.id, {
      // 成功是 HTTP 202——第二步资源回收还在后台跑，这里不能说"已删除"
      // （见 web/src/api/nodes.ts NodeDeregisterResult 的注释）。文案里带上后端返回
      // 的 cleanupState，而不是前端写死 "pending"：那个值是后端说了算的。
      onSuccess: (result) =>
        void message.success(
          t('nodes.deregister.accepted', {
            node: node.name,
            // cleanupState 由后端 DELETE /nodes/{id} 的响应体给出。
            state: enumLabel(t, 'nodes.deregister.cleanupState', result.cleanupState),
          }),
        ),
      onError: (error) => {
        const blockers = parseNodeDeregisterBlockers(
          error instanceof ApiError ? error.details : undefined,
        )
        if (blockers.length > 0) {
          setBlocked({ nodeName: node.name, blockers })
          return
        }
        // 清单解析不出来（或根本不是 409）时退回普通错误提示，不弹一个空
        // 表格——空表格会被读成"没有阻断者"，而事实是"我们没拿到清单"。
        void message.error(error.message)
      },
    })
  }

  const columns: ManagedColumn<NodeSummary>[] = [
    {
      key: 'name',
      title: t('nodes.columns.name'),
      dataIndex: 'name',
      hideable: false,
      render: (name: string, record) => <Link to={`/nodes/${record.id}`}>{name}</Link>,
    },
    {
      key: 'status',
      title: t('nodes.columns.status'),
      dataIndex: 'status',
      render: (value: NodeStatus) => <NodeStatusTag status={value} />,
      // status 来自后端 model.Node.status（render 那一路走 nodeStatusTag）。
      exportValue: (record) => enumLabel(t, 'nodes.status', record.status),
    },
    {
      key: 'center',
      title: t('nodes.columns.center'),
      dataIndex: 'centerId',
      render: (id: string) => centerNameById.get(id) ?? id,
      exportValue: (record) => centerNameById.get(record.centerId) ?? record.centerId,
    },
    {
      key: 'version',
      title: t('nodes.columns.version'),
      dataIndex: 'version',
      render: (value: string) => value || '—',
    },
    {
      key: 'hostname',
      title: t('nodes.columns.hostname'),
      dataIndex: 'hostname',
      render: (value: string) => value || '—',
    },
    {
      key: 'labels',
      title: t('nodes.columns.labels'),
      dataIndex: 'labels',
      render: (labels: string[]) => (
        <Space size={4} wrap>
          {labels.map((label) => (
            <Tag key={label}>{label}</Tag>
          ))}
        </Space>
      ),
      exportValue: (record) => record.labels.join(', '),
    },
    {
      key: 'maxPipelines',
      title: t('nodes.quota.label'),
      dataIndex: 'maxPipelines',
      render: (value: number) => (value === 0 ? t('nodes.quota.unlimited') : value),
      exportValue: (record) =>
        record.maxPipelines === 0 ? t('nodes.quota.unlimited') : record.maxPipelines,
    },
    {
      key: 'lastHeartbeat',
      title: t('nodes.columns.lastHeartbeat'),
      dataIndex: 'lastHeartbeatAt',
      render: (value: string | null) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.lastHeartbeatAt),
    },
    {
      key: 'actions',
      title: t('common.actions'),
      hideable: false,
      render: (_value, record) => (
        <NodeRowActions
          node={record}
          onToggleDisabled={() => handleToggleDisabled(record)}
          onRevokeToken={() => handleRevokeToken(record)}
          onDeregister={() => handleDeregister(record)}
          onEdit={() => setEditingNode(record)}
        />
      ),
    },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('nodes', columns)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      <NodeEditDrawer node={editingNode} onClose={() => setEditingNode(null)} />
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
            placeholder={t('nodes.filterCenter')}
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
          <Select
            allowClear
            placeholder={t('nodes.filterStatus')}
            style={{ width: 160 }}
            value={status}
            onChange={(value) => {
              setStatus(value)
              setPage(1)
            }}
            options={NODE_STATUSES.map((item) => ({
              value: item,
              label: t(`nodes.status.${item}`),
            }))}
          />
          <Input.Search
            allowClear
            placeholder={t('nodes.filterKeyword')}
            style={{ width: 260 }}
            onSearch={(value) => {
              setKeyword(value)
              setPage(1)
            }}
          />
        </Space>
        <Space wrap>
          {nodesQuery.isError && (
            <HintIcon type="error" content={<> {nodesQuery.error.message} </>} />
          )}
          <ExportButton
            filename="nodes"
            columns={visibleColumns}
            rows={nodesQuery.data?.items ?? []}
            scope="page"
          />
          <ColumnSettingsButton {...pickerProps} />
          <RequireRole role="admin">
            <Button icon={<KeyOutlined />} onClick={() => setRegisterCodesOpen(true)}>
              {t('nodes.actions.registerCodes')}
            </Button>
          </RequireRole>
        </Space>
      </Space>

      <div
        className="table-fill-container"
        style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
      >
        <ScrollTable<NodeSummary>
          rowKey="id"
          loading={nodesQuery.isLoading}
          dataSource={nodesQuery.data?.items ?? []}
          pagination={{
            current: page,
            pageSize,
            total: nodesQuery.data?.total ?? 0,
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

      <RegisterCodesDrawer
        open={registerCodesOpen}
        onClose={() => setRegisterCodesOpen(false)}
        defaultCenterId={centerId}
      />

      <NodeDeregisterBlockedNotice
        open={blocked !== null}
        nodeName={blocked?.nodeName ?? ''}
        blockers={blocked?.blockers ?? []}
        onClose={() => setBlocked(null)}
      />
    </div>
  )
}
