import { PipelineDataCheckTab } from './PipelineDataCheckTab'
import { ScrollTable } from '@/components/ScrollTable'
import { PipelineNameDrawer } from './PipelineNameDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { Breadcrumb, Card, Descriptions, Space, Tabs, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import { Link, useLocation, useNavigate, useParams } from 'react-router-dom'
import {
  usePipelineQuery,
  usePipelineReleasesQuery,
  useStartPipelineMutation,
  usePausePipelineMutation,
  useResumePipelineMutation,
  useStopPipelineMutation,
  useDeletePipelineMutation,
  useRetryPipelineMutation,
  type PipelineRelease,
} from '@/api/pipelines'
import { useNodeQuery } from '@/api/nodes'
import { useEndpointQuery } from '@/api/endpoints'
import { useCurrentUser } from '@/api/auth'
import { RowActions, type RowAction } from '@/components/RowActions'
import { formatDateTime } from '@/utils/format'
import {
  PipelineActualStateTag,
  PipelineDesiredStateTag,
  PipelineReleaseStatusTag,
} from './pipelineStatusTags'
import {
  actionRequiredSides,
  canDeletePipeline,
  canResumePipeline,
  divergentSides,
} from './pipelineStateRules'
import { isPipelinePrecheckError } from '@/api/pipelineResume'
import { PipelineSideStatus, PipelineRecoveryExhaustedAlert } from './PipelineSideStatus'
import { initModeLabel } from './pipelineLabels'
import { desiredStateLabel } from './pipelineLabels'
import { TimestampPositionPanel } from './TimestampPositionPanel'
import { PipelineReinitDrawer } from './PipelineReinitDrawer'
import { PipelinePrecheckPanel } from './PipelinePrecheckPanel'
import { PipelineRulesPanel } from './PipelineRulesPanel'
import { PipelineSchemaPlanDrawer } from './PipelineSchemaPlanDrawer'
import { PipelineSnapshotPanel } from './PipelineSnapshotPanel'
import { PipelinePositionPanel } from './PipelinePositionPanel'
import { PipelineMetricsPanel } from './PipelineMetricsPanel'
import { usePipelinePrecheckAuthorization } from '@/api/precheckQueries'

const RELEASES_PAGE_SIZE = 20

/** PipelineCreatePage 在"创建成功但启动失败"时通过 router state 带过来的
 * 提示文案；只在本次导航生效，不做持久化——刷新页面后这条提示自然消失，
 * 线路本身的状态（Draft，可从这里重新点"启动"重试）才是权威信息来源。 */
interface PipelineDetailLocationState {
  startFailedNotice?: string
}

export default function PipelineDetailPage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [renameOpen, setRenameOpen] = useState(false)
  const navigate = useNavigate()
  const location = useLocation()
  const { id } = useParams<{ id: string }>()
  const { role } = useCurrentUser()
  const isAdmin = role === 'admin'

  // 见文件头 PipelineDetailLocationState 注释：来自创建向导"创建成功但启动
  // 失败"的一次性提示。放进组件自己的 state 而不是每次渲染都读
  // location.state，是为了让用户点掉之后横幅不再重新出现——location.state
  // 在同一次导航生成的历次重渲染里是同一个引用，不清空的话横幅会一直挂着。
  const [startFailedNotice, setStartFailedNotice] = useState(
    (location.state as PipelineDetailLocationState | null)?.startFailedNotice,
  )

  const pipelineQuery = usePipelineQuery(id)
  const pipeline = pipelineQuery.data
  const tabKeys = [
    'overview',
    'sync-records',
    'rules',
    'position',
    'precheck',
    'releases',
    'data-checks',
  ]
  const requestedTab = location.hash.slice(1)
  const activeTab = tabKeys.includes(requestedTab) ? requestedTab : 'overview'
  const canStart = usePipelinePrecheckAuthorization(id, pipeline?.configVersion)

  const [releasesPage, setReleasesPage] = useState(1)
  const [reinitOpen, setReinitOpen] = useState(false)
  const [schemaPlanOpen, setSchemaPlanOpen] = useState(false)
  const releasesQuery = usePipelineReleasesQuery(
    id,
    {
      page: releasesPage,
      pageSize: RELEASES_PAGE_SIZE,
    },
    activeTab === 'releases',
  )

  const sourceNodeQuery = useNodeQuery(pipeline?.sourceNodeId)
  const targetNodeQuery = useNodeQuery(pipeline?.targetNodeId)
  const sourceEndpointQuery = useEndpointQuery(pipeline?.sourceEndpointId)
  const targetEndpointQuery = useEndpointQuery(pipeline?.targetEndpointId)

  const startMutation = useStartPipelineMutation()
  const pauseMutation = usePausePipelineMutation()
  const resumeMutation = useResumePipelineMutation()
  const stopMutation = useStopPipelineMutation()
  const deleteMutation = useDeletePipelineMutation()
  const retryMutation = useRetryPipelineMutation()

  if (pipelineQuery.isLoading || !pipeline) {
    return (
      <div style={{ padding: 24 }}>
        <HintIcon
          content={
            <>{pipelineQuery.isError ? pipelineQuery.error.message : t('common.loading')}</>
          }
        />
      </div>
    )
  }

  const onMutationSettled = {
    onSuccess: () => void message.success(t('common.operationSucceeded')),
    onError: (error: Error) => {
      void message.error(error.message)
      if (isPipelinePrecheckError(error)) navigate(`/pipelines/${pipeline.id}#precheck`)
    },
  }

  const lifecycle: RowAction[] =
    pipeline.desiredState === 'stopped'
      ? [
          {
            key: 'start',
            label: t('pipelines.actions.start'),
            visible: isAdmin,
            disabled: !canStart || startMutation.isPending,
            onClick: () => startMutation.mutate(pipeline.id, onMutationSettled),
            confirm: { title: t('pipelines.actions.startConfirm') },
          },
        ]
      : pipeline.desiredState === 'paused'
        ? [
            {
              key: 'resume',
              label: t('pipelines.actions.resume'),
              visible: isAdmin,
              disabled: !canResumePipeline(pipeline) || resumeMutation.isPending,
              onClick: () => resumeMutation.mutate(pipeline.id, onMutationSettled),
              confirm: { title: t('pipelines.actions.resumeConfirm') },
            },
            {
              key: 'stop',
              label: t('pipelines.actions.stop'),
              visible: isAdmin,
              onClick: () => stopMutation.mutate(pipeline.id, onMutationSettled),
              confirm: { title: t('pipelines.actions.stopConfirm') },
            },
          ]
        : [
            {
              key: 'pause',
              label: t('pipelines.actions.pause'),
              visible: isAdmin,
              onClick: () => pauseMutation.mutate(pipeline.id, onMutationSettled),
              confirm: { title: t('pipelines.actions.pauseConfirm') },
            },
            {
              key: 'stop',
              label: t('pipelines.actions.stop'),
              visible: isAdmin,
              onClick: () => stopMutation.mutate(pipeline.id, onMutationSettled),
              confirm: { title: t('pipelines.actions.stopConfirm') },
            },
          ]
  const actions: RowAction[] = [
    ...lifecycle,
    {
      key: 'rename',
      label: t('pipelines.rename.title'),
      visible: isAdmin,
      onClick: () => setRenameOpen(true),
    },
    {
      key: 'schemaPlan',
      label: t('pipelines.schemaPlan.title'),
      onClick: () => setSchemaPlanOpen(true),
    },
    {
      key: 'reinit',
      label: t('pipelines.reinit.title'),
      onClick: () => setReinitOpen(true),
    },
    {
      key: 'delete',
      disabled: !canDeletePipeline(pipeline) || deleteMutation.isPending,
      label: t('common.delete'),
      danger: true,
      visible: isAdmin,
      onClick: () =>
        deleteMutation.mutate(pipeline.id, {
          onSuccess: () => {
            void message.success(t('common.operationSucceeded'))
            navigate('/pipelines')
          },
          onError: (error) => void message.error(error.message),
        }),
      confirm: { title: t('pipelines.actions.deleteConfirm') },
    },
  ]

  // 按侧算，不看聚合值——理由见下方提示横幅处的注释。
  const divergentNotice = divergentSides(pipeline)
  const stuckAtGate = actionRequiredSides(pipeline)

  const sourceNodeName = sourceNodeQuery.data?.name ?? pipeline.sourceNodeId
  const targetNodeName = targetNodeQuery.data?.name ?? pipeline.targetNodeId
  const sourceEndpointName = sourceEndpointQuery.data?.name ?? pipeline.sourceEndpointId
  const targetEndpointName = targetEndpointQuery.data?.name ?? pipeline.targetEndpointId

  return (
    <div className="pipeline-detail-page">
      {renameOpen && (
        <PipelineNameDrawer pipeline={pipeline} onClose={() => setRenameOpen(false)} />
      )}
      {reinitOpen && (
        <PipelineReinitDrawer
          key={pipeline.id}
          pipeline={pipeline}
          canWrite={isAdmin}
          onClose={() => setReinitOpen(false)}
        />
      )}
      <Breadcrumb
        style={{ marginBottom: 8, flexShrink: 0 }}
        items={[
          { title: <Link to="/pipelines">{t('pipelines.detail.backToList')}</Link> },
          { title: pipeline.name },
        ]}
      />

      <Space
        className="pipeline-detail-heading"
        wrap
        style={{
          marginBottom: 8,
          width: '100%',
          justifyContent: 'space-between',
          flexShrink: 0,
        }}
      >
        <Typography.Title level={4} style={{ margin: 0 }}>
          {pipeline.name}
        </Typography.Title>
        <Space wrap>
          {startFailedNotice && (
            <Alert
              type="error"
              showIcon
              closable
              title={startFailedNotice}
              onClose={() => setStartFailedNotice(undefined)}
            />
          )}

          <RowActions actions={actions} />
        </Space>
      </Space>

      {schemaPlanOpen && (
        <PipelineSchemaPlanDrawer
          key={pipeline.id}
          pipeline={pipeline}
          canWrite={isAdmin}
          onClose={() => setSchemaPlanOpen(false)}
        />
      )}
      <Space style={{ marginBottom: 8, flexShrink: 0 }} wrap>
        <Typography.Text type="secondary">
          {t('pipelines.detail.desiredState')}：
        </Typography.Text>
        <PipelineDesiredStateTag state={pipeline.desiredState} />
        <Typography.Text type="secondary" style={{ marginLeft: 16 }}>
          {t('pipelines.detail.displayState')}：
        </Typography.Text>
        <PipelineActualStateTag state={pipeline.displayState} />
        {pipeline.recoveryExhausted && (
          <PipelineRecoveryExhaustedAlert
            canRetry={isAdmin}
            retrying={retryMutation.isPending}
            onRetry={() => retryMutation.mutate(pipeline.id, onMutationSettled)}
          />
        )}
        {stuckAtGate.length > 0 && (
          <Alert
            type="info"
            showIcon
            title={t('pipelines.detail.stateStuckAtGateTitle')}
            description={
              <Space orientation="vertical" size={0}>
                {stuckAtGate.map((side) => (
                  <span key={side}>
                    {t('pipelines.detail.stateStuckAtGateSide', {
                      side: t(`pipelines.side.${side}`),
                      actual: t(
                        `pipelines.actualState.${
                          side === 'source'
                            ? pipeline.sourceActualState
                            : pipeline.targetActualState
                        }`,
                      ),
                    })}
                  </span>
                ))}
                <span>{t('pipelines.detail.stateStuckAtGateHint')}</span>
              </Space>
            }
          />
        )}

        {divergentNotice.length > 0 && (
          <Alert
            type="info"
            showIcon
            title={t('pipelines.detail.stateNotConvergedTitle', {
              desired: desiredStateLabel(pipeline.desiredState, t),
            })}
            description={
              <Space orientation="vertical" size={0}>
                {divergentNotice.map((side) => (
                  <span key={side}>
                    {t('pipelines.detail.stateNotConvergedSide', {
                      side: t(`pipelines.side.${side}`),
                      actual: t(
                        `pipelines.actualState.${
                          side === 'source'
                            ? pipeline.sourceActualState
                            : pipeline.targetActualState
                        }`,
                      ),
                    })}
                  </span>
                ))}
              </Space>
            }
          />
        )}

        <Typography.Text type="secondary">
          {t('pipelines.detail.displayStateFrom', {
            side: t(`pipelines.side.${pipeline.displayStateSide}`),
          })}
        </Typography.Text>
      </Space>

      <Tabs
        className="pipeline-detail-tabs"
        activeKey={activeTab}
        destroyOnHidden
        onChange={(key) =>
          navigate(
            {
              pathname: location.pathname,
              search: location.search,
              hash: key === 'overview' ? '' : `#${key}`,
            },
            { state: location.state },
          )
        }
        items={[
          {
            key: 'data-checks',
            label: t('dataChecks.title'),
            children: (
              <div className="pipeline-tab-pane">
                <PipelineDataCheckTab pipelineId={pipeline.id} canWrite={isAdmin} />
              </div>
            ),
          },
          {
            key: 'overview',
            label: t('pipelines.detail.tabs.overview'),
            children: (
              <div className="pipeline-tab-pane">
                <PipelineSideStatus
                  pipeline={pipeline}
                  sourceNodeName={sourceNodeName}
                  targetNodeName={targetNodeName}
                />

                <Card
                  title={t('pipelines.detail.basicInfo')}
                  size="small"
                  style={{ marginBottom: 16 }}
                >
                  <Descriptions column={2} size="small">
                    <Descriptions.Item label={t('pipelines.detail.sourceEndpoint')}>
                      {sourceEndpointName}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('pipelines.detail.targetEndpoint')}>
                      {targetEndpointName}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('pipelines.detail.sourceNode')}>
                      {sourceNodeName}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('pipelines.detail.targetNode')}>
                      {targetNodeName}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('pipelines.detail.initMode')}>
                      {initModeLabel(pipeline.initMode, t)}
                    </Descriptions.Item>
                    {/*
            `configVersion === 0` 是"创建后、第一次配置同步之前"的合法初值，不是
            缺失值。**不能渲染成 `—` 或空**：那会把"一种情况缺解释"变成"两种
            情况长得一样"（`—` 在本仓一律表示"这个值没拿到"，见 utils/format.ts），
            而这一格恰恰是拿到了、就是 0。
            但光显示"0"也不行：本页下方的下发记录表里版本号是从 1 起的，两者并排
            时运维会以为数据不对。所以 0 时补一句说明，非 0 时原样显示数字。
          */}
                    <Descriptions.Item label={t('pipelines.detail.configVersion')}>
                      {pipeline.configVersion === 0
                        ? t('pipelines.detail.configVersionNotSynced', {
                            version: pipeline.configVersion,
                          })
                        : pipeline.configVersion}
                    </Descriptions.Item>
                    {/* Actual position changes use the independently confirmed panel. */}
                    <Descriptions.Item
                      label={
                        <span className="field-label-hint">
                          {t('pipelines.detail.startPosition')}
                          <HintIcon content={t('pipelines.detail.startPositionNote')} />
                        </span>
                      }
                      span={2}
                    >
                      {pipeline.startPosition !== undefined ? (
                        <Typography.Text code style={{ overflowWrap: 'anywhere' }}>
                          {JSON.stringify(pipeline.startPosition)}
                        </Typography.Text>
                      ) : (
                        t('pipelines.detail.startPositionEmpty')
                      )}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('pipelines.detail.createdAt')}>
                      {formatDateTime(pipeline.createdAt)}
                    </Descriptions.Item>
                    <Descriptions.Item label={t('pipelines.detail.updatedAt')}>
                      {formatDateTime(pipeline.updatedAt)}
                    </Descriptions.Item>
                  </Descriptions>
                </Card>
                {['full_and_incremental', 'full_only'].includes(pipeline.initMode) && (
                  <PipelineSnapshotPanel
                    key={`snapshot-${pipeline.id}`}
                    pipeline={pipeline}
                    canWrite={isAdmin}
                  />
                )}
              </div>
            ),
          },
          {
            key: 'sync-records',
            label: t('pipelines.detail.tabs.records'),
            children: (
              <div className="pipeline-tab-pane">
                <PipelineMetricsPanel
                  key={`metrics-${pipeline.id}`}
                  pipeline={pipeline}
                  sourceNodeName={sourceNodeName}
                  targetNodeName={targetNodeName}
                />
              </div>
            ),
          },
          {
            key: 'rules',
            label: t('pipelines.detail.tabs.rules'),
            children: (
              <div className="pipeline-tab-pane">
                <PipelineRulesPanel pipeline={pipeline} canWrite={isAdmin} />
              </div>
            ),
          },
          {
            key: 'position',
            label: t('pipelines.detail.tabs.position'),
            children: (
              <div className="pipeline-tab-pane">
                <PipelinePositionPanel
                  key={`position-${pipeline.id}-${pipeline.configVersion}`}
                  pipeline={pipeline}
                  canWrite={isAdmin}
                />

                {pipeline.initMode === 'incremental_only_timestamp' &&
                  !pipeline.positionRevision &&
                  isAdmin && <TimestampPositionPanel key={pipeline.id} pipeline={pipeline} />}
              </div>
            ),
          },
          {
            key: 'precheck',
            label: t('pipelines.detail.tabs.precheck'),
            children: (
              <div className="pipeline-tab-pane">
                <PipelinePrecheckPanel
                  key={pipeline.id}
                  pipeline={pipeline}
                  canWrite={isAdmin}
                />
              </div>
            ),
          },
          {
            key: 'releases',
            label: t('pipelines.detail.tabs.releases'),
            children: (
              <div className="pipeline-tab-pane">
                <Card size="small" className="pipeline-fill-card">
                  <div className="table-fill-container">
                    {releasesQuery.isError ? (
                      <HintIcon
                        type="error"
                        content={<> {releasesQuery.error.message} </>}
                      />
                    ) : (
                      <ScrollTable<PipelineRelease>
                        size="small"
                        rowKey="id"
                        loading={releasesQuery.isLoading}
                        dataSource={releasesQuery.data?.items ?? []}
                        locale={{ emptyText: t('pipelines.detail.releasesEmpty') }}
                        pagination={{
                          current: releasesPage,
                          pageSize: RELEASES_PAGE_SIZE,
                          total: releasesQuery.data?.total ?? 0,
                          hideOnSinglePage: true,
                          onChange: (page) => setReleasesPage(page),
                        }}
                        columns={[
                          {
                            key: 'configVersion',
                            title: (
                              <span className="field-label-hint">
                                {t('pipelines.detail.configVersion')}
                                <HintIcon
                                  content={t('pipelines.detail.releasesScopeNote')}
                                />
                              </span>
                            ),
                            dataIndex: 'configVersion',
                            width: 96,
                          },
                          {
                            key: 'side',
                            title: t('pipelines.detail.releasesSide'),
                            render: (_, record) =>
                              record.side === 'source'
                                ? t('pipelines.columns.source')
                                : t('pipelines.columns.target'),
                          },
                          {
                            key: 'node',
                            title: t('pipelines.detail.releasesNode'),
                            // release.nodeId 与 pipeline 自身 source/target 节点一一对应
                            // （节点创建后不可变），直接按 side 复用已经解析好的名称，不为
                            // 每一行单独发起一次节点查询。
                            render: (_, record) =>
                              record.side === 'source' ? sourceNodeName : targetNodeName,
                          },
                          {
                            key: 'status',
                            title: t('pipelines.detail.releasesStatus'),
                            render: (_, record) => (
                              <PipelineReleaseStatusTag status={record.status} />
                            ),
                          },
                          {
                            key: 'message',
                            title: t('pipelines.detail.releasesMessage'),
                            width: 280,
                            render: (_, record) =>
                              record.message ? (
                                <Typography.Paragraph
                                  style={{ marginBottom: 0 }}
                                  ellipsis={{
                                    rows: 2,
                                    expandable: true,
                                    symbol: t('common.expand'),
                                  }}
                                >
                                  {record.message}
                                </Typography.Paragraph>
                              ) : (
                                '—'
                              ),
                          },
                          {
                            key: 'createdAt',
                            title: t('pipelines.detail.releasesCreatedAt'),
                            render: (_, record) => formatDateTime(record.createdAt),
                          },
                          {
                            key: 'appliedAt',
                            title: t('pipelines.detail.releasesAppliedAt'),
                            render: (_, record) =>
                              record.appliedAt ? formatDateTime(record.appliedAt) : '—',
                          },
                        ]}
                      />
                    )}
                  </div>
                </Card>
              </div>
            ),
          },
        ]}
      />
    </div>
  )
}
