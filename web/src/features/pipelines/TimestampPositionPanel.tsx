import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { useState } from 'react'
import { Button, Card, DatePicker, Descriptions, Space } from 'antd'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import dayjs, { type Dayjs } from 'dayjs'
import { useTranslation } from 'react-i18next'
import {
  confirmTimestampPosition,
  getTimestampPosition,
  previewTimestampPosition,
  type PositionPreview,
} from '@/api/positionPreview'
import { type PipelineSummary } from '@/api/pipelines'

function initialTimestamp(position: unknown): Dayjs | null {
  if (!position || typeof position !== 'object') return null
  const value =
    'source_ts_ms' in position
      ? position.source_ts_ms
      : 'sourceTsMs' in position
        ? position.sourceTsMs
        : undefined
  const timestamp =
    typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN
  return Number.isFinite(timestamp) && timestamp > 0 ? dayjs(timestamp) : null
}

export function TimestampPositionPanel({ pipeline }: { pipeline: PipelineSummary }) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const queryClient = useQueryClient()
  const [timestamp, setTimestamp] = useState<Dayjs | null>(() =>
    initialTimestamp(pipeline.startPosition),
  )
  const [preview, setPreview] = useState<PositionPreview | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [confirmOpen, setConfirmOpen] = useState(false)
  const stopped = pipeline.desiredState === 'stopped'
  const position = pipeline.startPosition
  const confirmed =
    !!position &&
    typeof position === 'object' &&
    (('timestamp_confirmed' in position && position.timestamp_confirmed === true) ||
      ('timestampConfirmed' in position && position.timestampConfirmed === true))

  const savedQuery = useQuery({
    queryKey: ['timestampPosition', pipeline.id, pipeline.configVersion],
    queryFn: () => getTimestampPosition(pipeline.id),
    enabled: confirmed,
    retry: false,
  })
  const restoring = confirmed && savedQuery.isPending
  const boundary = preview ?? savedQuery.data
  const visibleError =
    error ?? (confirmed && savedQuery.isError ? savedQuery.error.message : null)

  const runPreview = async () => {
    if (!timestamp || busy || restoring || confirmed || !stopped) return
    setBusy(true)
    setError(null)
    setPreview(null)
    try {
      setPreview(await previewTimestampPosition(pipeline.id, timestamp.valueOf()))
    } catch (error) {
      setError(error instanceof Error ? error.message : t('common.operationFailed'))
    } finally {
      setBusy(false)
    }
  }
  const confirm = async () => {
    if (!preview || busy || !stopped) return
    setBusy(true)
    setError(null)
    try {
      setPreview(await confirmTimestampPosition(pipeline.id, preview.digest))
      setConfirmOpen(false)
      await queryClient.invalidateQueries({ queryKey: ['pipelines'] })
      void message.success(t('pipelines.positionPreview.confirmed'))
    } catch (error) {
      setError(error instanceof Error ? error.message : t('common.operationFailed'))
    } finally {
      setBusy(false)
    }
  }
  return (
    <Card
      title={
        <span className="section-title">
          {t('pipelines.positionPreview.title')}
          <Alert type="info" title={t('pipelines.positionPreview.hint')} />
        </span>
      }
    >
      <Space orientation="vertical" style={{ width: '100%' }}>
        <Space wrap>
          <DatePicker
            showTime
            format="YYYY-MM-DD HH:mm:ss.SSS"
            value={timestamp}
            aria-label={t('pipelines.wizard.startPositionTimestamp')}
            disabled={!stopped || busy || restoring || confirmed || !!boundary?.confirmedAt}
            onChange={(value) => {
              setTimestamp(value)
              setPreview(null)
              setError(null)
            }}
          />
          <Button
            onClick={() => void runPreview()}
            loading={busy || restoring}
            disabled={!stopped || !timestamp || confirmed || !!boundary?.confirmedAt}
          >
            {t('pipelines.positionPreview.preview')}
          </Button>
          {visibleError && <Alert type="error" title={visibleError} />}
        </Space>
        {boundary && (
          <Descriptions column={1} size="small" bordered>
            <Descriptions.Item label={t('pipelines.wizard.startPositionTimestamp')}>
              {dayjs(boundary.requestedTsMs).format('YYYY-MM-DD HH:mm:ss.SSS Z')}
            </Descriptions.Item>
            <Descriptions.Item label={t('pipelines.positionPreview.commitTime')}>
              {dayjs(boundary.commitTsMs).format('YYYY-MM-DD HH:mm:ss.SSS Z')}
            </Descriptions.Item>
            <Descriptions.Item label={t('pipelines.wizard.startPositionFile')}>
              {boundary.file}
            </Descriptions.Item>
            <Descriptions.Item label={t('pipelines.wizard.startPositionPos')}>
              {boundary.offset}
            </Descriptions.Item>
            <Descriptions.Item
              label={
                <span className="field-label-hint">
                  {t('pipelines.wizard.startPositionGtidSet')}
                  {boundary.confirmedAt && (
                    <Alert
                      type="success"
                      title={t('pipelines.positionPreview.confirmed')}
                    />
                  )}
                </span>
              }
            >
              {boundary.gtidSet || t('pipelines.positionPreview.emptySet')}
            </Descriptions.Item>
          </Descriptions>
        )}
        {boundary && !boundary.confirmedAt && (
          <Button
            type="primary"
            disabled={!stopped || busy}
            onClick={() => setConfirmOpen(true)}
          >
            {t('pipelines.positionPreview.confirm')}
          </Button>
        )}
      </Space>
      <ActionDrawer
        open={confirmOpen}
        title={t('pipelines.positionPreview.confirmTitle')}
        onClose={() => setConfirmOpen(false)}
        onConfirm={() => void confirm()}
        confirmLoading={busy}
        confirmText={t('pipelines.positionPreview.confirm')}
        cancelText={t('common.cancel')}
      >
        {t('pipelines.positionPreview.confirmHint')}
      </ActionDrawer>
    </Card>
  )
}
