import { ActionDrawer } from '@/components/ActionDrawer'
import { ScrollTable } from '@/components/ScrollTable'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintIcon } from '@/components/HintIcon'
import { Tag } from 'antd'
import { useTranslation } from 'react-i18next'
import type { AffectedPipeline } from '@/api/endpoints'
// Reuse the shared side-state labels for colors and translations.
import {
  PipelineActualStateTag,
  PipelineDesiredStateTag,
} from '@/features/pipelines/pipelineStatusTags'
import { sideRestartNeed, type SideRestartNeed } from './affectedRestart'

export interface EndpointAffectedPipelinesDrawerProps {
  open: boolean
  endpointName: string
  affected: AffectedPipeline[]
  /** 后端 `affectedUnknown`：清单查询失败，**不是**"没有受影响的线路"。 */
  affectedUnknown: boolean
  onClose: () => void
}

const RESTART_NEED_COLOR: Record<SideRestartNeed, string> = {
  needed: 'warning',
  not_needed: 'default',
  unknown: 'error',
}

export function EndpointAffectedPipelinesDrawer({
  open,
  endpointName,
  affected,
  affectedUnknown,
  onClose,
}: EndpointAffectedPipelinesDrawerProps) {
  const { t } = useTranslation()

  return (
    <ActionDrawer
      fillBody
      open={open}
      title={
        <span className="section-title">
          {t('dataEndpoints.affected.title', { name: endpointName })}
          <Alert
            type={affectedUnknown ? 'warning' : 'info'}
            title={t(
              affectedUnknown
                ? 'dataEndpoints.affected.unknownTitle'
                : 'dataEndpoints.affected.savedNotEffective',
            )}
            description={t(
              affectedUnknown
                ? 'dataEndpoints.affected.unknownDesc'
                : 'dataEndpoints.affected.restartHint',
            )}
          />
        </span>
      }
      onClose={onClose}
      onConfirm={onClose}
      confirmText={t('common.confirm')}
      hideCancel
      size={960}
      destroyOnHidden
    >
      {!affectedUnknown && (
        <>
          <ScrollTable<AffectedPipeline>
            rowKey="id"
            size="small"
            pagination={false}
            dataSource={affected}
            locale={{ emptyText: t('dataEndpoints.affected.none') }}
            columns={[
              {
                key: 'name',
                title: t('dataEndpoints.affected.columns.name'),
                dataIndex: 'name',
              },
              {
                key: 'desiredState',
                title: t('dataEndpoints.affected.columns.desiredState'),
                dataIndex: 'desiredState',
                width: 110,
                render: (_, record) => (
                  <PipelineDesiredStateTag state={record.desiredState} />
                ),
              },
              {
                // 约束②：源端、目标端各占一列，各自带自己的"要不要重启"判定。
                key: 'source',
                title: t('dataEndpoints.affected.columns.source'),
                dataIndex: 'sourceActualState',
                width: 210,
                render: (_, record) => <SideCell state={record.sourceActualState} />,
              },
              {
                key: 'target',
                title: t('dataEndpoints.affected.columns.target'),
                dataIndex: 'targetActualState',
                width: 210,
                render: (_, record) => <SideCell state={record.targetActualState} />,
              },
            ]}
          />
        </>
      )}
    </ActionDrawer>
  )
}

/**
 * 一侧的状态 + 该侧要不要重启。两个信息并排给，而不是只给结论：结论是本文件
 * 按状态推出来的（见 web/src/features/endpoints/affectedRestart.ts），把原状态一并放出来，运维才能在结论
 * 看着不对时自己判断。
 */
function SideCell({ state }: { state: AffectedPipeline['sourceActualState'] }) {
  const { t } = useTranslation()
  const need = sideRestartNeed(state)
  return (
    <>
      <PipelineActualStateTag state={state} />
      <Tag color={RESTART_NEED_COLOR[need]}>
        {t(`dataEndpoints.affected.restart.${need}`)}
      </Tag>
      {need === 'unknown' && (
        <HintIcon content={<>{t('dataEndpoints.affected.restartUnknownHint')}</>} />
      )}
    </>
  )
}
