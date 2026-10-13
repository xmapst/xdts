import { HintIcon } from '@/components/HintIcon'
import { HintAlert as Alert } from '@/components/HintAlert'
import { Button, Space, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import type { PipelineSide, PipelineSummary } from '@/api/pipelines'
import { formatDateTime } from '@/utils/format'
import { PipelineActualStateTag } from './pipelineStatusTags'

export interface PipelineSideStatusProps {
  pipeline: PipelineSummary
  sourceNodeName: string
  targetNodeName: string
}

interface SideView {
  side: PipelineSide
  nodeName: string
  state: PipelineSummary['sourceActualState']
  lastError: string | undefined
  lastHeartbeatAt: string | null
}

export function PipelineSideStatus({
  pipeline,
  sourceNodeName,
  targetNodeName,
}: PipelineSideStatusProps) {
  const { t } = useTranslation()

  const sides: SideView[] = [
    {
      side: 'source',
      nodeName: sourceNodeName,
      state: pipeline.sourceActualState,
      lastError: pipeline.sourceLastError,
      lastHeartbeatAt: pipeline.sourceNodeLastHeartbeatAt,
    },
    {
      side: 'target',
      nodeName: targetNodeName,
      state: pipeline.targetActualState,
      lastError: pipeline.targetLastError,
      lastHeartbeatAt: pipeline.targetNodeLastHeartbeatAt,
    },
  ]

  return (
    <Space wrap size={12} style={{ width: '100%', marginBottom: 12 }}>
      {sides.map((view) => (
        <Space key={view.side} wrap size={8}>
          <Typography.Text type="secondary">
            {t(`pipelines.side.${view.side}`)}（{view.nodeName}）：
          </Typography.Text>
          <PipelineActualStateTag state={view.state} />
          {view.lastError && (
            <HintIcon
              key={`${view.side}-error`}
              type="error"
              content={
                <>
                  {view.state === 'unknown'
                    ? t('pipelines.detail.sideStaleError', {
                        side: t(`pipelines.side.${view.side}`),
                      })
                    : t('pipelines.detail.sideLastError', {
                        side: t(`pipelines.side.${view.side}`),
                      })}
                  ：{view.lastError}
                </>
              }
            />
          )}
          {/* 该侧离线时，把"从什么时候起失联"一并说出来。离线是按节点判的
            （后端心跳超时 → 把该侧标 unknown），所以这里给的就是那个节点的
            最后心跳时间——不是"这一侧最后一次上报 pipeline 状态的时间"，
            后者今天不存在这份数据，编一个出来只会让人以为它更精确。 */}
          {view.state === 'unknown' && (
            <Typography.Text type="secondary">
              {view.lastHeartbeatAt === null
                ? t('pipelines.detail.sideOfflineNoHeartbeat')
                : t('pipelines.detail.sideOfflineSince', {
                    at: formatDateTime(view.lastHeartbeatAt),
                  })}
            </Typography.Text>
          )}
        </Space>
      ))}
    </Space>
  )
}

/* 按钮调的是 `POST /pipelines/:id/retry`，不是 start/resume：走到这里的
pipeline 其 desiredState 通常本来就是 running，再点"启动"什么都不会发生。 */
export interface PipelineRecoveryExhaustedAlertProps {
  onRetry: () => void
  retrying: boolean
  canRetry: boolean
}

export function PipelineRecoveryExhaustedAlert({
  onRetry,
  retrying,
  canRetry,
}: PipelineRecoveryExhaustedAlertProps) {
  const { t } = useTranslation()
  return (
    <Alert
      type="error"
      showIcon
      title={t('pipelines.detail.recoveryExhaustedTitle')}
      description={t('pipelines.detail.recoveryExhaustedHint')}
      action={
        canRetry ? (
          <Button size="small" danger loading={retrying} onClick={onRetry}>
            {t('pipelines.actions.retryRecovery')}
          </Button>
        ) : undefined
      }
    />
  )
}
