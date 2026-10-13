import { ActionDrawer } from '@/components/ActionDrawer'
import { HintIcon } from '@/components/HintIcon'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { useEffect, useState } from 'react'
import {
  Button,
  Card,
  Checkbox,
  DatePicker,
  Descriptions,
  Form,
  Input,
  InputNumber,
  Select,
  Space,
  Typography,
} from 'antd'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import dayjs, { type Dayjs } from 'dayjs'
import { useTranslation } from 'react-i18next'
import {
  getPipelinePositions,
  getPositionChange,
  previewPositionChange,
  confirmPositionChange,
  parsePositionBoundary,
  type PositionKind,
  type PositionInput,
  type PositionChange,
  type PositionBoundary,
} from '@/api/positionChange'
import type { PipelineSummary } from '@/api/pipelines'
import { isApiMockEnabled } from '@/mocks/config'
import { formatDateTime } from '@/utils/format'

function configuredPosition(pipeline: PipelineSummary) {
  const kind: PositionKind =
    pipeline.initMode === 'incremental_only_gtid_set'
      ? 'gtid_set'
      : pipeline.initMode === 'incremental_only_binlog_pos'
        ? 'binlog_pos'
        : pipeline.initMode === 'incremental_only_timestamp'
          ? 'timestamp'
          : 'latest'
  const pos =
    pipeline.startPosition && typeof pipeline.startPosition === 'object'
      ? (pipeline.startPosition as Record<string, unknown>)
      : {}
  const gtid = pos.gtid_set ?? pos.gtidSet
  const file = pos.source_file ?? pos.sourceFile
  const rawOffset = pos.source_offset ?? pos.sourceOffset
  const offset =
    typeof rawOffset === 'number'
      ? rawOffset
      : typeof rawOffset === 'string'
        ? Number(rawOffset)
        : NaN
  const rawTime = pos.source_ts_ms ?? pos.sourceTsMs
  const timestamp =
    typeof rawTime === 'number'
      ? rawTime
      : typeof rawTime === 'string'
        ? Number(rawTime)
        : NaN
  return {
    kind,
    gtid: typeof gtid === 'string' ? gtid : '',
    file: typeof file === 'string' ? file : '',
    offset: Number.isSafeInteger(offset) && offset >= 4 ? offset : null,
    timestamp: Number.isSafeInteger(timestamp) && timestamp > 0 ? dayjs(timestamp) : null,
  }
}

export function PipelinePositionPanel({
  pipeline,
  canWrite,
}: {
  pipeline: PipelineSummary
  canWrite: boolean
}) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const client = useQueryClient()
  const [initial] = useState(() => configuredPosition(pipeline))
  const [kind, setKind] = useState<PositionKind>(initial.kind)
  const [gtid, setGTID] = useState(initial.gtid)
  const [file, setFile] = useState(initial.file)
  const [offset, setOffset] = useState<number | null>(initial.offset)
  const [timestamp, setTimestamp] = useState<Dayjs | null>(initial.timestamp)
  const [preview, setPreview] = useState<PositionChange | null>(null)
  const [edited, setEdited] = useState(false)
  const [boundary, setBoundary] = useState<PositionBoundary | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [forwardConfirmed, setForwardConfirmed] = useState(false)
  const [now, setNow] = useState(Date.now)
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [])
  const positions = useQuery({
    queryKey: ['pipelines', 'positions', pipeline.id],
    queryFn: ({ signal }) => getPipelinePositions(pipeline.id, signal),
    refetchInterval: 5000,
  })
  const saved = useQuery({
    queryKey: ['pipelines', 'positionChange', pipeline.id, pipeline.configVersion],
    queryFn: () => getPositionChange(pipeline.id),
    retry: false,
  })
  const view = preview ?? (edited ? null : saved.data)
  const stopped =
    pipeline.desiredState === 'stopped' &&
    [pipeline.sourceActualState, pipeline.targetActualState].every(
      (state) => state === 'stopped' || state === 'draft',
    )
  const enabled =
    canWrite && stopped && !busy && !isApiMockEnabled() && !!positions.data?.editable
  const current =
    !!view &&
    !!positions.data &&
    view.runId === positions.data.runId &&
    view.configVersion === positions.data.configVersion &&
    view.positionRevision === positions.data.positionRevision &&
    !view.confirmedAt &&
    now < Date.parse(view.expiresAt)
  const valid =
    kind === 'latest' ||
    kind === 'gtid_set' ||
    (kind === 'binlog_pos' &&
      file.trim().length > 0 &&
      offset !== null &&
      Number.isInteger(offset) &&
      offset >= 4 &&
      offset <= 4294967295) ||
    (kind === 'timestamp' &&
      timestamp !== null &&
      Number.isSafeInteger(timestamp.valueOf()) &&
      timestamp.valueOf() > 0)
  const resetPreview = () => {
    setEdited(true)
    setPreview(null)
    setBoundary(null)
    setError(null)
    setConfirmOpen(false)
    setForwardConfirmed(false)
  }
  const runPreview = async () => {
    if (!enabled || !valid) return
    setBusy(true)
    resetPreview()
    const input: PositionInput =
      kind === 'latest'
        ? { kind }
        : kind === 'gtid_set'
          ? { kind, gtidSet: gtid }
          : kind === 'binlog_pos'
            ? { kind, file: file.trim(), offset: offset! }
            : { kind, tsMs: timestamp!.valueOf() }
    try {
      setPreview(await previewPositionChange(pipeline.id, input))
      await positions.refetch()
    } catch (error) {
      setError(error instanceof Error ? error.message : t('common.operationFailed'))
      setBoundary(parsePositionBoundary(error))
    } finally {
      setBusy(false)
    }
  }
  const confirm = async () => {
    if (!enabled || !current || !view || (view.forwardJump && !forwardConfirmed)) return
    setBusy(true)
    setError(null)
    try {
      setPreview(await confirmPositionChange(pipeline.id, view.digest, forwardConfirmed))
      setConfirmOpen(false)
      await client.invalidateQueries({ queryKey: ['pipelines'] })
      void message.success(t('pipelines.position.saved'))
    } catch (error) {
      setError(error instanceof Error ? error.message : t('common.operationFailed'))
      setConfirmOpen(false)
      setPreview(null)
      await saved.refetch()
      await positions.refetch()
    } finally {
      setBusy(false)
    }
  }
  return (
    <Card
      id="position"
      size="small"
      style={{ marginBottom: 12 }}
      extra={
        (positions.isError || saved.isError) && (
          <Space>
            {positions.isError && <Alert type="error" title={positions.error.message} />}
            {saved.isError && <Alert type="error" title={saved.error.message} />}
          </Space>
        )
      }
    >
      <Space orientation="vertical" style={{ width: '100%' }}>
        {positions.data && (
          <Descriptions column={{ xs: 1, md: 2 }} size="small" bordered>
            <Descriptions.Item
              label={
                <span className="field-label-hint">
                  {t('pipelines.position.revision')}
                  <HintIcon content={t('pipelines.position.hint')} />
                </span>
              }
              span={2}
            >
              {positions.data.positionRevision}
            </Descriptions.Item>
            {(['source', 'target'] as const).map((role) => {
              const row = positions.data!.observations.find((row) => row.role === role)
              const stale = row && now - Date.parse(row.observedAt) > 15000
              return (
                <Descriptions.Item key={role} label={t(`pipelines.position.${role}`)}>
                  {row ? (
                    <Space wrap style={{ width: '100%' }}>
                      {(role === 'source'
                        ? (['readGtidSet', 'publishedGtidSet'] as const)
                        : (['appliedGtidSet'] as const)
                      ).map((key) => (
                        <div key={key}>
                          <Typography.Text>
                            {t(`pipelines.position.${key}`)}:{' '}
                          </Typography.Text>
                          <HintIcon
                            content={
                              <>
                                {row.values[key] || t('pipelines.positionPreview.emptySet')}
                              </>
                            }
                          />
                        </div>
                      ))}
                      <Typography.Text type={stale ? 'warning' : 'secondary'}>
                        {formatDateTime(row.observedAt)}
                        {stale ? ` · ${t('pipelines.position.stale')}` : ''}
                      </Typography.Text>
                    </Space>
                  ) : (
                    t('pipelines.position.unknown')
                  )}
                </Descriptions.Item>
              )
            })}
          </Descriptions>
        )}
        {canWrite && (
          <Form layout="vertical" disabled={!enabled}>
            <HintForm label={t('pipelines.position.kind')}>
              <Select
                aria-label={t('pipelines.position.kind')}
                value={kind}
                onChange={(value) => {
                  setKind(value)
                  resetPreview()
                }}
                options={(['latest', 'gtid_set', 'binlog_pos', 'timestamp'] as const).map(
                  (value) => ({ value, label: t(`pipelines.position.kinds.${value}`) }),
                )}
              />
            </HintForm>
            {kind === 'gtid_set' && (
              <HintForm label={t('pipelines.wizard.startPositionGtidSet')}>
                <Input.TextArea
                  aria-label={t('pipelines.wizard.startPositionGtidSet')}
                  value={gtid}
                  maxLength={1048576}
                  onChange={(event) => {
                    setGTID(event.target.value)
                    resetPreview()
                  }}
                />
              </HintForm>
            )}
            {kind === 'binlog_pos' && (
              <Space wrap align="start">
                <HintForm label={t('pipelines.wizard.startPositionFile')}>
                  <Input
                    aria-label={t('pipelines.wizard.startPositionFile')}
                    maxLength={255}
                    value={file}
                    onChange={(event) => {
                      setFile(event.target.value)
                      resetPreview()
                    }}
                  />
                </HintForm>
                <HintForm label={t('pipelines.wizard.startPositionPos')}>
                  <InputNumber
                    aria-label={t('pipelines.wizard.startPositionPos')}
                    min={4}
                    max={4294967295}
                    precision={0}
                    value={offset}
                    onChange={(value) => {
                      setOffset(value)
                      resetPreview()
                    }}
                  />
                </HintForm>
              </Space>
            )}
            {kind === 'timestamp' && (
              <HintForm label={t('pipelines.wizard.startPositionTimestamp')}>
                <DatePicker
                  aria-label={t('pipelines.wizard.startPositionTimestamp')}
                  showTime
                  format="YYYY-MM-DD HH:mm:ss.SSS"
                  value={timestamp}
                  onChange={(value) => {
                    setTimestamp(value)
                    resetPreview()
                  }}
                />
              </HintForm>
            )}
            <Space wrap>
              <Button
                disabled={!enabled || !valid || saved.isPending}
                loading={busy}
                onClick={() => void runPreview()}
              >
                {t('pipelines.position.preview')}
              </Button>
              {error && <Alert showIcon type="error" title={error} />}
              {boundary && (
                <Alert
                  type="warning"
                  title={t('pipelines.position.boundary', {
                    file: boundary.file,
                    requested: boundary.requested,
                    preceding: boundary.preceding,
                    following: boundary.following ?? t('pipelines.position.unknown'),
                  })}
                />
              )}
            </Space>
          </Form>
        )}
        {view && (
          <>
            <Descriptions
              column={1}
              size="small"
              bordered
              styles={{ content: { overflowWrap: 'anywhere' } }}
            >
              <Descriptions.Item
                label={
                  <span className="field-label-hint">
                    {t('pipelines.position.actual')}
                    {(view.warnings.length > 0 ||
                      view.forwardJump ||
                      (!view.confirmedAt && !current)) && (
                      <HintIcon
                        type="warning"
                        content={
                          <>
                            {view.warnings.map((warning, i) => (
                              <div key={i}>{warning}</div>
                            ))}
                            {view.forwardJump && (
                              <div>{t('pipelines.position.forwardRisk')}</div>
                            )}
                            {!view.confirmedAt && !current && (
                              <div>{t('pipelines.position.changed')}</div>
                            )}
                          </>
                        }
                      />
                    )}
                    {view.confirmedAt && (
                      <Alert type="success" title={t('pipelines.position.saved')} />
                    )}
                  </span>
                }
              >
                {view.gtidSet || t('pipelines.positionPreview.emptySet')}
              </Descriptions.Item>
              <Descriptions.Item label={t('pipelines.position.baseline')}>
                {view.publishedGtidSet === null
                  ? t('pipelines.position.unknown')
                  : view.publishedGtidSet || t('pipelines.positionPreview.emptySet')}
              </Descriptions.Item>
              <Descriptions.Item label={t('pipelines.position.epoch')}>
                {view.checkpointEpoch}
              </Descriptions.Item>
              {view.file && (
                <Descriptions.Item label={t('pipelines.wizard.startPositionFile')}>
                  {view.file}:{view.offset}
                </Descriptions.Item>
              )}
              {view.commitTsMs > 0 && (
                <Descriptions.Item label={t('pipelines.positionPreview.commitTime')}>
                  {dayjs(view.commitTsMs).format('YYYY-MM-DD HH:mm:ss.SSS Z')}
                </Descriptions.Item>
              )}
              <Descriptions.Item label={t('pipelines.position.expires')}>
                {formatDateTime(view.expiresAt)}
              </Descriptions.Item>
            </Descriptions>
            {!view.confirmedAt && (
              <>
                {canWrite && (
                  <Button
                    type="primary"
                    disabled={!enabled || !current}
                    onClick={() => {
                      setForwardConfirmed(false)
                      setConfirmOpen(true)
                    }}
                  >
                    {t('pipelines.position.confirm')}
                  </Button>
                )}
              </>
            )}
          </>
        )}
      </Space>
      <ActionDrawer
        open={confirmOpen}
        title={
          <span className="section-title">
            {t('pipelines.position.confirmTitle')}
            <HintIcon content={t('pipelines.position.confirmHint')} />
          </span>
        }
        onClose={() => setConfirmOpen(false)}
        onConfirm={() => void confirm()}
        confirmLoading={busy}
        confirmText={t('pipelines.position.confirm')}
        cancelText={t('common.cancel')}
        confirmButtonProps={{
          disabled: !enabled || !current || (!!view?.forwardJump && !forwardConfirmed),
        }}
      >
        <Descriptions column={1} size="small">
          <Descriptions.Item label={t('pipelines.position.actual')}>
            <HintIcon content={view?.gtidSet || t('pipelines.positionPreview.emptySet')} />
          </Descriptions.Item>
        </Descriptions>
        {view?.forwardJump && (
          <Checkbox
            checked={forwardConfirmed}
            onChange={(event) => setForwardConfirmed(event.target.checked)}
          >
            {t('pipelines.position.forwardConfirm')}
          </Checkbox>
        )}
      </ActionDrawer>
    </Card>
  )
}
