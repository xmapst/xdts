import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import {
  Breadcrumb,
  Card,
  Col,
  Descriptions,
  Row,
  Space,
  Statistic,
  Tabs,
  Tag,
  Typography,
} from 'antd'
import type { ColumnType } from 'antd/es/table'
import { useTranslation } from 'react-i18next'
import { Link, useNavigate, useParams } from 'react-router-dom'
import {
  useNodeQuery,
  useRevokeNodeTokenMutation,
  useUpdateNodeMutation,
  useDeregisterNodeMutation,
  parseNodeDeregisterBlockers,
  type NodeDeregisterBlocker,
  type NodePipelineRef,
} from '@/api/nodes'
import { ApiError } from '@/api/client'
import { useCentersQuery } from '@/api/centers'
import { formatBytes, formatCount, formatDateTime, formatPercent } from '@/utils/format'
import { formatMemoryUsage } from './nodeRuntimeFormat'
import { NodeStatusTag } from './nodeStatusTag'
import { NodeRowActions } from './NodeRowActions'
import { NodeEditDrawer } from './NodeEditDrawer'
import { NodeDeregisterBlockedNotice } from './NodeDeregisterBlockedNotice'
import { NodeLogLevelForm } from './NodeLogLevelForm'
import { NodeLogSearchPanel } from './NodeLogSearchPanel'
import { enumLabel } from '@/utils/i18nEnum'

/* 「承载的线路」表格的列类型。与 antd 的 `ColumnType<NodePipelineRef>` 的唯一
区别是把 `dataIndex` 从宽松的 `string | number | …` 收窄成
`keyof NodePipelineRef`——那个收窄就是这个类型存在的全部理由，见下方
columns 处的注释（antd 原类型不会因为字段名写错而报错，实测确认过）。 */
type NodePipelineColumn = Omit<ColumnType<NodePipelineRef>, 'dataIndex'> & {
  dataIndex: keyof NodePipelineRef
}

/* A repeated pipelineId identifies a pipeline hosted on both sides of this node. */
function selfLoopPipelineIds(refs: NodePipelineRef[]): Set<string> {
  const seen = new Set<string>()
  const duplicated = new Set<string>()
  for (const ref of refs) {
    if (seen.has(ref.pipelineId)) {
      duplicated.add(ref.pipelineId)
    } else {
      seen.add(ref.pipelineId)
    }
  }
  return duplicated
}

function formatUptime(seconds: number | null): string {
  if (seconds === null) {
    return '—'
  }
  const days = Math.floor(seconds / 86_400)
  const hours = Math.floor((seconds % 86_400) / 3600)
  return `${days}d ${hours}h`
}

export default function NodeDetailPage() {
  const [editOpen, setEditOpen] = useState(false)
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const nodeQuery = useNodeQuery(id)
  const centersQuery = useCentersQuery()
  const updateMutation = useUpdateNodeMutation()
  const revokeMutation = useRevokeNodeTokenMutation()
  const deregisterMutation = useDeregisterNodeMutation()
  // 注销被 409 挡住时的阻断清单；null 表示弹窗关闭。**必须声明在下面那个
  // 「加载中就提前 return」之前**——Hook 不能放在条件返回之后。
  const [blockers, setBlockers] = useState<NodeDeregisterBlocker[] | null>(null)

  if (nodeQuery.isLoading || !nodeQuery.data) {
    return (
      <div style={{ padding: 24 }}>
        <HintIcon
          type={nodeQuery.isError ? 'error' : 'loading'}
          content={nodeQuery.isError ? nodeQuery.error.message : t('common.loading')}
        />
      </div>
    )
  }

  const node = nodeQuery.data
  const selfLoopIds = selfLoopPipelineIds(node.pipelines)
  const center = (centersQuery.data ?? []).find((item) => item.id === node.centerId)

  const handleToggleDisabled = () => {
    updateMutation.mutate(
      { id: node.id, input: { disabled: node.status !== 'disabled' } },
      {
        onSuccess: () => void message.success(t('common.operationSucceeded')),
        onError: (error) => void message.error(error.message),
      },
    )
  }

  const handleRevokeToken = () => {
    revokeMutation.mutate(node.id, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      onError: (error) => void message.error(error.message),
    })
  }

  const handleDeregister = () => {
    deregisterMutation.mutate(node.id, {
      // 成功是 HTTP 202，资源回收在后台继续（见 web/src/api/nodes.ts
      // NodeDeregisterResult 注释）——文案说"已受理"，不说"已删除"。
      // 随即回列表：这个节点已经软删除，留在详情页只会在下一次刷新时 404。
      onSuccess: (result) => {
        void message.success(
          t('nodes.deregister.accepted', {
            node: node.name,
            // cleanupState 由后端 DELETE /nodes/{id} 的响应体给出。
            state: enumLabel(t, 'nodes.deregister.cleanupState', result.cleanupState),
          }),
        )
        void navigate('/nodes')
      },
      onError: (error) => {
        const parsed = parseNodeDeregisterBlockers(
          error instanceof ApiError ? error.details : undefined,
        )
        if (parsed.length > 0) {
          setBlockers(parsed)
          return
        }
        void message.error(error.message)
      },
    })
  }

  return (
    <div>
      <Breadcrumb
        style={{ marginBottom: 16 }}
        items={[
          { title: <Link to="/nodes">{t('nodes.detail.backToList')}</Link> },
          { title: node.name },
        ]}
      />

      <Space style={{ marginBottom: 16, width: '100%', justifyContent: 'space-between' }}>
        <Space>
          <Typography.Title level={4} style={{ margin: 0 }}>
            {node.name}
          </Typography.Title>
          <NodeStatusTag status={node.status} />
        </Space>
        <NodeRowActions
          node={node}
          onToggleDisabled={handleToggleDisabled}
          onRevokeToken={handleRevokeToken}
          onDeregister={handleDeregister}
          onEdit={() => setEditOpen(true)}
        />
      </Space>

      <Card title={t('nodes.detail.basicInfo')} style={{ marginBottom: 16 }} size="small">
        <NodeEditDrawer node={editOpen ? node : null} onClose={() => setEditOpen(false)} />
        <Descriptions column={2} size="small">
          <Descriptions.Item label={t('nodes.quota.label')}>
            {node.maxPipelines === 0 ? t('nodes.quota.unlimited') : node.maxPipelines}
          </Descriptions.Item>
          <Descriptions.Item label={t('nodes.detail.center')}>
            {center ? `${center.name}（${center.code}）` : node.centerId}
          </Descriptions.Item>
          <Descriptions.Item label={t('nodes.detail.version')}>
            {node.version || '—'}
          </Descriptions.Item>
          {/* `|| '—'` 而非 `??`：后端恒返回空串（见 api/nodes.ts 的 hostname 注释），
              而 `?? ` 不捕空串 ⇒ 整格渲染成空白，与"这一项不存在"长得一样。
              写法对齐下方 ips 那行的既有约定。 */}
          <Descriptions.Item label={t('nodes.detail.hostname')}>
            {node.hostname || '—'}
          </Descriptions.Item>
          <Descriptions.Item label={t('nodes.detail.osArch')}>
            {node.osArch || '—'}
          </Descriptions.Item>
          <Descriptions.Item label={t('nodes.detail.ips')}>
            {node.ips.join(', ') || '—'}
          </Descriptions.Item>
          <Descriptions.Item label={t('nodes.detail.labels')}>
            {node.labels.length > 0 ? (
              <Space size={4} wrap>
                {node.labels.map((label) => (
                  <Tag key={label}>{label}</Tag>
                ))}
              </Space>
            ) : (
              t('common.none')
            )}
          </Descriptions.Item>
          <Descriptions.Item label={t('nodes.detail.registeredAt')}>
            {formatDateTime(node.registeredAt)}
          </Descriptions.Item>
          <Descriptions.Item label={t('nodes.detail.lastHeartbeatAt')}>
            {formatDateTime(node.lastHeartbeatAt)}
          </Descriptions.Item>
        </Descriptions>
      </Card>

      <Card
        title={t('nodes.detail.resourceUsage')}
        style={{ marginBottom: 16 }}
        size="small"
      >
        <Row gutter={24}>
          <Col span={6}>
            <Statistic
              title={t('nodes.detail.cpu')}
              value={formatPercent(node.runtime.cpuPercent)}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title={t('nodes.detail.mem')}
              // 并排三值而不是只显示百分比：memRssBytes/memTotalBytes 加进协议
              // 的唯一理由是让运维能在界面上**验算**，只显示百分比它们就白加了。
              value={formatMemoryUsage(
                node.runtime.memRssBytes,
                node.runtime.memTotalBytes,
                node.runtime.memPercent,
              )}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title={t('nodes.detail.stateDbSize')}
              value={formatBytes(node.runtime.stateDbSizeBytes)}
            />
          </Col>
          <Col span={3}>
            <Statistic
              title={t('nodes.detail.pipelineCount')}
              // 三态：null（不知道）→ `—`，0（确实 0 条）→ `0`。裸值直出时
              // antd 会把 null 画成空白，而空白和"这一格坏了"分不开。
              value={formatCount(node.runtime.pipelineCount)}
            />
          </Col>
          <Col span={3}>
            <Statistic
              title={t('nodes.detail.uptime')}
              value={formatUptime(node.runtime.uptimeSeconds)}
            />
          </Col>
        </Row>
      </Card>

      <Tabs
        items={[
          {
            key: 'pipelines',
            label: t('nodes.detail.hostedPipelines'),
            children: (
              <ScrollTable<NodePipelineRef>
                rowKey={(ref) => `${ref.pipelineId}:${ref.role}`}
                size="small"
                dataSource={node.pipelines}
                locale={{ emptyText: t('nodes.detail.noPipelines') }}
                pagination={false}
                columns={
                  [
                    {
                      title: t('nodes.detail.pipelineName'),
                      dataIndex: 'pipelineName',
                      render: (name: NodePipelineRef['pipelineName'], record) => (
                        <Space size={4}>
                          <Typography.Text>{name}</Typography.Text>
                          {selfLoopIds.has(record.pipelineId) ? (
                            // 没有这个标记，自环在界面上就是"同一条线路莫名其妙
                            // 出现了两次"——看起来像重复数据的 bug。标记把它
                            // 说明成一个事实：这个节点在这条线路上两侧都担任。
                            <Tag color="gold">{t('nodes.detail.selfLoop')}</Tag>
                          ) : null}
                        </Space>
                      ),
                    },
                    {
                      title: t('nodes.detail.pipelineRole'),
                      dataIndex: 'role',
                      render: (role: NodePipelineRef['role']) => (
                        <Tag color={role === 'source' ? 'blue' : 'purple'}>
                          {t(
                            role === 'source'
                              ? 'nodes.detail.roleSource'
                              : 'nodes.detail.roleTarget',
                          )}
                        </Tag>
                      ),
                    },
                    {
                      title: t('nodes.detail.sideActualState'),
                      dataIndex: 'sideActualState',
                    },
                  ] satisfies NodePipelineColumn[]
                }
              />
            ),
          },
          {
            key: 'logLevel',
            label: t('nodes.detail.logLevelTab'),
            children: <NodeLogLevelForm node={node} />,
          },
          {
            key: 'logSearch',
            label: t('nodes.detail.logSearchTab'),
            children: <NodeLogSearchPanel nodeId={node.id} />,
          },
        ]}
      />

      <NodeDeregisterBlockedNotice
        open={blockers !== null}
        nodeName={node.name}
        blockers={blockers ?? []}
        onClose={() => setBlockers(null)}
      />
    </div>
  )
}
