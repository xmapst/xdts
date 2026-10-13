import { ScrollTable } from '@/components/ScrollTable'
import { HintIcon } from '@/components/HintIcon'
import { ActionDrawer } from '@/components/ActionDrawer'
import { Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import type { NodeDeregisterBlocker } from '@/api/nodes'
import { formatDateTime } from '@/utils/format'
import type { TFunction } from 'i18next'
import { enumLabel } from '@/utils/i18nEnum'

export interface NodeDeregisterBlockedNoticeProps {
  open: boolean
  nodeName: string
  blockers: NodeDeregisterBlocker[]
  onClose: () => void
}

/* deletedAt distinguishes active dependencies from deleted pipelines with pending cleanup. */
function describeBlockerReason(blocker: NodeDeregisterBlocker, t: TFunction): string {
  if (blocker.deletedAt === null) {
    return t('nodes.deregister.blockerInUse')
  }
  return t('nodes.deregister.blockerCleanupPending', {
    // cleanupState 来自后端 model.CleanupState。
    state: enumLabel(t, 'nodes.deregister.cleanupState', blocker.cleanupState),
  })
}

export function NodeDeregisterBlockedNotice({
  open,
  nodeName,
  blockers,
  onClose,
}: NodeDeregisterBlockedNoticeProps) {
  const { t } = useTranslation()

  if (!open) return null
  return (
    <ActionDrawer
      open
      title={t('nodes.deregister.blockedTitle')}
      onClose={onClose}
      fillBody
      size={960}
      footer={null}
      destroyOnHidden
    >
      <HintIcon
        type="error"
        content={
          <>
            <div>
              {t('nodes.deregister.blockedSummary', {
                node: nodeName,
                count: blockers.length,
              })}
            </div>
            <div>{t('nodes.deregister.blockedHint')}</div>
          </>
        }
      />
      <ScrollTable<NodeDeregisterBlocker>
        rowKey="pipelineId"
        size="small"
        pagination={false}
        dataSource={blockers}
        columns={[
          {
            title: t('nodes.deregister.blockerPipeline'),
            dataIndex: 'pipelineName',
            render: (name: string) => (
              // 不做成 <Link>：这个弹窗是从节点列表/详情弹出来的，点进去会
              // 把用户从「我正在注销这个节点」的上下文里带走，而他多半还要
              // 回来看清单剩下几条。给出线路名与 id，让他自己决定什么时候去
              // 处理——id 单独列出来是因为线路重名是允许的，光有名字定位不到。
              <Typography.Text>{name}</Typography.Text>
            ),
          },
          {
            title: t('nodes.deregister.blockerPipelineId'),
            dataIndex: 'pipelineId',
            render: (id: string) => <Typography.Text code>{id}</Typography.Text>,
          },
          {
            title: t('nodes.deregister.blockerReason'),
            key: 'reason',
            render: (_value, record) => describeBlockerReason(record, t),
          },
          {
            title: t('nodes.deregister.blockerDeletedAt'),
            dataIndex: 'deletedAt',
            render: (value: string | null) =>
              value === null ? '—' : formatDateTime(value),
          },
        ]}
      />
    </ActionDrawer>
  )
}
