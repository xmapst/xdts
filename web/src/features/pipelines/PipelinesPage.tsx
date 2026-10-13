import { ActionDrawer } from '@/components/ActionDrawer'
import { ScrollTable } from '@/components/ScrollTable'
import { PipelineNameDrawer } from './PipelineNameDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { Button, Input, Space, Tag, Tabs, Typography } from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { Link, useNavigate } from 'react-router-dom'
import { RequireRole } from '@/components/RequireRole'
import {
  usePipelinesQuery,
  useStartPipelineMutation,
  usePausePipelineMutation,
  useResumePipelineMutation,
  useStopPipelineMutation,
  useDeletePipelineMutation,
  type PipelineListQuery,
  type PipelineSummary,
} from '@/api/pipelines'
import { useNodesQuery } from '@/api/nodes'
import { useEndpointsQuery } from '@/api/endpoints'
import { useCurrentUser } from '@/api/auth'
import { RowActions, type RowAction } from '@/components/RowActions'
import { isPipelinePrecheckError } from '@/api/pipelineResume'
import { canDeletePipeline, canResumePipeline } from './pipelineStateRules'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { PipelineActualStateTag, PipelineDesiredStateTag } from './pipelineStatusTags'
import { initModeLabel } from './pipelineLabels'
import { actualStateLabel, desiredStateLabel } from './pipelineLabels'
import PipelineCleanupTable from './PipelineCleanupTable'
import { PipelineCloneDrawer } from './PipelineCloneDrawer'
import {
  useBatchPipelinesMutation,
  type PipelineBatchAction,
  type PipelineBatchResult,
} from '@/api/pipelineOperations'

const PAGE_SIZE_OPTIONS = ['10', '20', '50', '100']
const DEFAULT_PAGE_SIZE = 20
// 节点 / 端点列表按 id 建名称查找表用，容量对齐服务端 `maxListPageSize`
// （见 internal/manager/http/pagination.go）——这不是"拉全量"，只是把查找表的
// 上限设到服务端本来就允许的单页最大值；超出这个量的节点/端点在名称列会退化
// 显示原始 id（见下方 `?? record.xxxId` 兜底），不会报错或显示空白。
const LOOKUP_PAGE_SIZE = 200

export default function PipelinesPage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const navigate = useNavigate()
  const { role } = useCurrentUser()
  const isAdmin = role === 'admin'
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE)
  const [keyword, setKeyword] = useState('')
  const [tab, setTab] = useState('active')
  const [renamePipeline, setRenamePipeline] = useState<PipelineSummary>()
  const [clonePipeline, setClonePipeline] = useState<PipelineSummary>()
  const [selectedIds, setSelectedIds] = useState<string[]>([])
  const [batchResults, setBatchResults] = useState<PipelineBatchResult[]>()
  const [batchRequest, setBatchRequest] = useState<{
    action: PipelineBatchAction
    ids: string[]
  }>()
  const batchMutation = useBatchPipelinesMutation()

  const query: PipelineListQuery = { page, pageSize, keyword: keyword || undefined }
  const pipelinesQuery = usePipelinesQuery(query)
  const nodesQuery = useNodesQuery({ page: 1, pageSize: LOOKUP_PAGE_SIZE })
  const endpointsQuery = useEndpointsQuery({ page: 1, pageSize: LOOKUP_PAGE_SIZE })

  const startMutation = useStartPipelineMutation()
  const pauseMutation = usePausePipelineMutation()
  const resumeMutation = useResumePipelineMutation()
  const stopMutation = useStopPipelineMutation()
  const deleteMutation = useDeletePipelineMutation()

  const nodeNameById = new Map((nodesQuery.data?.items ?? []).map((n) => [n.id, n.name]))
  const endpointNameById = new Map(
    (endpointsQuery.data?.items ?? []).map((e) => [e.id, e.name]),
  )

  const formatSide = (endpointId: string, nodeId: string) =>
    `${endpointNameById.get(endpointId) ?? endpointId} @ ${nodeNameById.get(nodeId) ?? nodeId}`

  const onMutationSettled = {
    onSuccess: () => void message.success(t('common.operationSucceeded')),
    onError: (error: Error) => void message.error(error.message),
  }

  const submitBatch = (action: PipelineBatchAction) => {
    setBatchRequest({ action, ids: [...selectedIds] })
  }

  const buildActions = (record: PipelineSummary): RowAction[] => {
    const lifecycle: RowAction[] =
      record.desiredState === 'stopped'
        ? [
            {
              key: 'start',
              label: t('pipelines.actions.start'),
              visible: isAdmin,
              onClick: () => startMutation.mutate(record.id, onMutationSettled),
              confirm: { title: t('pipelines.actions.startConfirm') },
            },
          ]
        : record.desiredState === 'paused'
          ? [
              {
                key: 'resume',
                label: t('pipelines.actions.resume'),
                visible: isAdmin,
                disabled: !canResumePipeline(record) || resumeMutation.isPending,
                onClick: () =>
                  resumeMutation.mutate(record.id, {
                    ...onMutationSettled,
                    onError: (error) => {
                      onMutationSettled.onError(error)
                      if (isPipelinePrecheckError(error))
                        navigate(`/pipelines/${record.id}#precheck`)
                    },
                  }),
                confirm: { title: t('pipelines.actions.resumeConfirm') },
              },
              {
                key: 'stop',
                label: t('pipelines.actions.stop'),
                visible: isAdmin,
                onClick: () => stopMutation.mutate(record.id, onMutationSettled),
                confirm: { title: t('pipelines.actions.stopConfirm') },
              },
            ]
          : [
              {
                key: 'pause',
                label: t('pipelines.actions.pause'),
                visible: isAdmin,
                onClick: () => pauseMutation.mutate(record.id, onMutationSettled),
                confirm: { title: t('pipelines.actions.pauseConfirm') },
              },
              {
                key: 'stop',
                label: t('pipelines.actions.stop'),
                visible: isAdmin,
                onClick: () => stopMutation.mutate(record.id, onMutationSettled),
                confirm: { title: t('pipelines.actions.stopConfirm') },
              },
            ]
    return [
      ...lifecycle,
      {
        key: 'rename',
        label: t('pipelines.rename.title'),
        visible: isAdmin,
        onClick: () => setRenamePipeline(record),
      },
      {
        key: 'clone',
        label: t('pipelines.clone.title'),
        visible: isAdmin,
        onClick: () => setClonePipeline(record),
      },
      {
        key: 'precheck',
        label: (
          <Link to={`/pipelines/${record.id}#precheck`}>
            {t('pipelines.precheck.open')}
          </Link>
        ),
      },
      {
        key: 'delete',
        disabled: !canDeletePipeline(record) || deleteMutation.isPending,
        label: t('common.delete'),
        danger: true,
        visible: isAdmin,
        onClick: () => deleteMutation.mutate(record.id, onMutationSettled),
        confirm: { title: t('pipelines.actions.deleteConfirm') },
      },
    ]
  }

  const columns: ManagedColumn<PipelineSummary>[] = [
    {
      key: 'name',
      title: t('pipelines.columns.name'),
      dataIndex: 'name',
      hideable: false,
      render: (name: string, record) => <Link to={`/pipelines/${record.id}`}>{name}</Link>,
    },
    {
      key: 'source',
      title: t('pipelines.columns.source'),
      render: (_value, record) => formatSide(record.sourceEndpointId, record.sourceNodeId),
      exportValue: (record) => formatSide(record.sourceEndpointId, record.sourceNodeId),
    },
    {
      key: 'target',
      title: t('pipelines.columns.target'),
      render: (_value, record) => formatSide(record.targetEndpointId, record.targetNodeId),
      exportValue: (record) => formatSide(record.targetEndpointId, record.targetNodeId),
    },
    {
      key: 'initMode',
      title: t('pipelines.columns.initMode'),
      dataIndex: 'initMode',
      // 值来自后端，走兜底：后端加一个 InitMode 而前端 union 还不知道时，
      // 这一列和**导出的 CSV** 里都不会出现一串 `pipelines.initMode.xxx`。
      render: (value: PipelineSummary['initMode']) => initModeLabel(value, t),
      exportValue: (record) => initModeLabel(record.initMode, t),
    },
    {
      key: 'desiredState',
      title: t('pipelines.columns.desiredState'),
      dataIndex: 'desiredState',
      render: (value: PipelineSummary['desiredState']) => (
        <PipelineDesiredStateTag state={value} />
      ),
      // 值来自后端 model.Pipeline.desired_state。
      exportValue: (record) => desiredStateLabel(record.desiredState, t),
    },
    {
      key: 'displayState',
      title: t('pipelines.columns.displayState'),
      dataIndex: 'displayState',
      // 这一列是两侧聚合出来的展示值，只有一个格子却对应两个在上报的
      // 节点。所以两侧取值不同时必须标出它取自哪一侧——否则一条 error 挂在
      // 这里，运维不知道该登哪台机器；而两侧相同时标出来只是噪音。
      render: (value: PipelineSummary['displayState'], record) => (
        <Space size={4} wrap>
          <PipelineActualStateTag state={value} />
          {record.sourceActualState !== record.targetActualState && (
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              {t('pipelines.columns.displayStateFrom', {
                side: t(`pipelines.side.${record.displayStateSide}`),
              })}
            </Typography.Text>
          )}
          {record.recoveryExhausted && (
            <Tag color="error">{t('pipelines.columns.recoveryExhausted')}</Tag>
          )}
        </Space>
      ),
      exportValue: (record) => {
        // displayState 是后端两侧聚合出来的实际状态，取值域由后端定。
        const base = actualStateLabel(record.displayState, t)
        const sided =
          record.sourceActualState === record.targetActualState
            ? base
            : `${base}（${t('pipelines.columns.displayStateFrom', {
                side: t(`pipelines.side.${record.displayStateSide}`),
              })}）`
        // 导出物会离开系统、没有上下文可供纠正，所以"需人工"这一笔在 CSV 里
        // 也要带上，不能只画在屏幕上。
        return record.recoveryExhausted
          ? `${sided}｜${t('pipelines.columns.recoveryExhausted')}`
          : sided
      },
    },
    {
      key: 'updatedAt',
      title: t('pipelines.columns.updatedAt'),
      dataIndex: 'updatedAt',
      render: (value: string) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.updatedAt),
    },
    {
      key: 'actions',
      title: t('common.actions'),
      hideable: false,
      render: (_value, record) => <RowActions actions={buildActions(record)} />,
    },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('pipelines', columns)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      <Tabs
        activeKey={tab}
        onChange={(value) => {
          setTab(value)
          setSelectedIds([])
        }}
        items={[
          { key: 'active', label: t('pipelines.cleanup.active') },
          { key: 'cleanup', label: t('pipelines.cleanup.title') },
        ]}
      />
      {tab === 'active' && (
        <>
          <Space
            style={{
              marginBottom: 16,
              width: '100%',
              justifyContent: 'space-between',
              flexShrink: 0,
            }}
          >
            <Space wrap>
              <Input.Search
                allowClear
                placeholder={t('pipelines.filterKeyword')}
                style={{ width: 260 }}
                onSearch={(value) => {
                  setKeyword(value)
                  setPage(1)
                  setSelectedIds([])
                }}
              />
            </Space>
            <Space wrap>
              {pipelinesQuery.isError && (
                <HintIcon type="error" content={pipelinesQuery.error.message} />
              )}
              <ExportButton
                filename="pipelines"
                columns={visibleColumns}
                rows={pipelinesQuery.data?.items ?? []}
                scope="page"
              />
              <ColumnSettingsButton {...pickerProps} />
              <RequireRole role="admin">
                <Link to="/pipelines/new">
                  <Button type="primary" icon={<PlusOutlined />}>
                    {t('pipelines.create')}
                  </Button>
                </Link>
              </RequireRole>
            </Space>
          </Space>

          {isAdmin && selectedIds.length > 0 && (
            <Space wrap style={{ marginBottom: 12 }}>
              <Typography.Text>
                {t('pipelines.batch.selected', { count: selectedIds.length })}
              </Typography.Text>
              {(['start', 'pause', 'resume', 'stop'] as const).map((action) => (
                <Button
                  key={action}
                  disabled={batchMutation.isPending}
                  onClick={() => submitBatch(action)}
                >
                  {t(`pipelines.batch.${action}`)}
                </Button>
              ))}
            </Space>
          )}

          <div
            className="table-fill-container"
            style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
          >
            <ScrollTable<PipelineSummary>
              rowKey="id"
              rowSelection={
                isAdmin
                  ? {
                      selectedRowKeys: selectedIds,
                      onChange: (keys) => setSelectedIds(keys.map(String)),
                    }
                  : undefined
              }
              loading={pipelinesQuery.isLoading}
              dataSource={pipelinesQuery.data?.items ?? []}
              pagination={{
                current: page,
                pageSize,
                total: pipelinesQuery.data?.total ?? 0,
                showSizeChanger: true,
                showQuickJumper: true,
                pageSizeOptions: PAGE_SIZE_OPTIONS,
                onChange: (nextPage, nextPageSize) => {
                  setPage(nextPage)
                  setPageSize(nextPageSize)
                  setSelectedIds([])
                },
              }}
              columns={visibleColumns}
            />
          </div>
        </>
      )}
      {tab === 'cleanup' && (
        <div
          className="table-fill-container"
          style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
        >
          <PipelineCleanupTable />
        </div>
      )}
      {isAdmin && (
        <>
          {renamePipeline && (
            <PipelineNameDrawer
              pipeline={renamePipeline}
              onClose={() => setRenamePipeline(undefined)}
            />
          )}
          <PipelineCloneDrawer
            pipeline={clonePipeline}
            onClose={() => setClonePipeline(undefined)}
          />
        </>
      )}
      <ActionDrawer
        open={batchRequest !== undefined}
        title={
          <span className="section-title">
            {batchRequest &&
              t('pipelines.batch.confirm', {
                action: t(`pipelines.actions.${batchRequest.action}`),
                count: batchRequest.ids.length,
              })}
            <HintIcon content={t('pipelines.batch.description')} />
          </span>
        }
        onClose={() => setBatchRequest(undefined)}
        confirmLoading={batchMutation.isPending}
        onConfirm={async () => {
          if (!batchRequest) return
          try {
            const response = await batchMutation.mutateAsync(batchRequest)
            setBatchRequest(undefined)
            setBatchResults(response.results)
            setSelectedIds([])
          } catch (error) {
            void message.error(
              error instanceof Error ? error.message : t('common.operationFailed'),
            )
          }
        }}
        destroyOnHidden
      />
      <ActionDrawer
        fillBody
        open={Boolean(batchResults)}
        title={t('pipelines.batch.results')}
        footer={null}
        onClose={() => setBatchResults(undefined)}
        size={960}
      >
        <ScrollTable<PipelineBatchResult>
          rowKey="id"
          dataSource={batchResults}
          pagination={false}
          columns={[
            {
              title: t('pipelines.columns.name'),
              render: (_value, item) => (
                <Link to={`/pipelines/${item.id}`}>
                  {pipelinesQuery.data?.items.find((p) => p.id === item.id)?.name ??
                    item.id}
                </Link>
              ),
            },
            {
              title: t('pipelines.batch.result'),
              render: (_value, item) => (
                <Tag color={item.success ? 'success' : 'error'}>
                  {t(item.success ? 'pipelines.batch.accepted' : 'pipelines.batch.failed')}
                </Tag>
              ),
            },
            {
              title: t('pipelines.batch.message'),
              dataIndex: 'message',
              render: (value: string) => <HintIcon content={value} />,
            },
          ]}
        />
      </ActionDrawer>
    </div>
  )
}
