import { ScrollTable } from '@/components/ScrollTable'
import { HintIcon } from '@/components/HintIcon'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { useEffect, useState } from 'react'
import {
  Button,
  Checkbox,
  Descriptions,
  Drawer,
  Form,
  Input,
  Radio,
  Space,
  Tag,
  Typography,
} from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { ApiError } from '@/api/client'
import {
  confirmReinit,
  getReinit,
  previewReinit,
  reinitQueryKey,
  type ReinitPreviewInput,
  type ReinitRecord,
  type ReinitTargetHandling,
  type SchemaPlanStep,
} from '@/api/reinit'
import { pipelineDetailQueryKey, type PipelineSummary } from '@/api/pipelines'
import { formatDateTime } from '@/utils/format'

export function PipelineReinitDrawer({
  pipeline,
  canWrite,
  onClose,
}: {
  pipeline: PipelineSummary
  canWrite: boolean
  onClose: () => void
}) {
  const { t } = useTranslation()
  const client = useQueryClient()
  const saved = useQuery({
    queryKey: reinitQueryKey(pipeline.id),
    queryFn: ({ signal }) => getReinit(pipeline.id, signal),
    retry: false,
    staleTime: 0,
    meta: { suppressGlobalError: true },
    refetchInterval: (query) => {
      const record = query.state.data
      return record?.confirmedAt && (!record.sourceApplied || !record.targetApplied)
        ? 3000
        : false
    },
  })
  const previewMutation = useMutation({
    mutationFn: (input: ReinitPreviewInput) => previewReinit(pipeline.id, input),
    meta: { suppressGlobalError: true },
    onSuccess: (record) => client.setQueryData(reinitQueryKey(pipeline.id), record),
  })
  const confirmMutation = useMutation({
    mutationFn: (record: Pick<ReinitRecord, 'fromRunId' | 'digest'>) =>
      confirmReinit(pipeline.id, record),
    meta: { suppressGlobalError: true },
    onSuccess: (record) => {
      client.setQueryData(reinitQueryKey(pipeline.id), record)
      void client.invalidateQueries({ queryKey: pipelineDetailQueryKey(pipeline.id) })
      void client.invalidateQueries({ queryKey: ['pipelines', 'list'] })
      void client.invalidateQueries({ queryKey: ['pipelines', 'precheck', pipeline.id] })
      void client.invalidateQueries({ queryKey: ['pipelines', 'releases', pipeline.id] })
    },
  })
  const [schemas, setSchemas] = useState('')
  const [handling, setHandling] = useState<ReinitTargetHandling>()
  const [schemaMigration, setSchemaMigration] = useState(false)
  const [definerFallback, setDefinerFallback] = useState('')
  const [local, setLocal] = useState<{ record: ReinitRecord; version: number } | null>(null)
  const [edited, setEdited] = useState(false)
  const [consent, setConsent] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const busy = previewMutation.isPending || confirmMutation.isPending
  const stopped =
    pipeline.desiredState === 'stopped' &&
    ['draft', 'stopped'].includes(pipeline.sourceActualState) &&
    ['draft', 'stopped'].includes(pipeline.targetActualState)
  const latestConfirmation = saved.data?.confirmedAt
    ? saved.data
    : local?.record.confirmedAt
      ? local.record
      : null
  const pending =
    !!latestConfirmation &&
    (!latestConfirmation.sourceApplied || !latestConfirmation.targetApplied)
  const editable =
    canWrite && stopped && !pending && !busy && !saved.isPending && !saved.isError
  const record =
    saved.data?.confirmedAt && saved.data.digest === local?.record.digest
      ? saved.data
      : (local?.record ?? (!edited ? saved.data : null))
  const currentPreview =
    !!local &&
    !local.record.confirmedAt &&
    local.version === pipeline.configVersion &&
    local.record.configVersion === pipeline.configVersion &&
    !saved.data?.confirmedAt &&
    (!saved.data || saved.data.digest === local.record.digest)
  const confirmable =
    currentPreview && editable && !local?.record.document.schemaPlan?.blocked
  const schemaList = schemas
    .split(/[,\n]/)
    .map((value) => value.trim())
    .filter(Boolean)

  useEffect(() => {
    // Polling refreshes actual lifecycle state independently of REINIT acks.
    if (saved.data?.confirmedAt)
      void client.invalidateQueries({ queryKey: pipelineDetailQueryKey(pipeline.id) })
  }, [saved.dataUpdatedAt, pipeline.id, client, saved.data?.confirmedAt])

  const change = () => {
    setLocal(null)
    setEdited(true)
    setConsent(false)
    setError(null)
  }
  const preview = async () => {
    if (!editable || !handling || schemaList.length === 0 || schemaList.length > 100) return
    setLocal(null)
    setEdited(true)
    setConsent(false)
    setError(null)
    try {
      await client.cancelQueries({ queryKey: reinitQueryKey(pipeline.id) })
      const result = await previewMutation.mutateAsync({
        schemas: schemaList,
        targetHandling: handling,
        schemaMigration,
        ...(schemaMigration && definerFallback.trim()
          ? { definerFallback: definerFallback.trim() }
          : {}),
      })
      setLocal({ record: result, version: pipeline.configVersion })
      setEdited(false)
    } catch (error) {
      setError(error instanceof Error ? error.message : t('common.operationFailed'))
    }
  }
  const confirm = async () => {
    if (!local || !confirmable || !consent) return
    setError(null)
    try {
      await client.cancelQueries({ queryKey: reinitQueryKey(pipeline.id) })
      const result = await confirmMutation.mutateAsync(local.record)
      setLocal({ record: result, version: pipeline.configVersion })
      setConsent(false)
    } catch (error) {
      setError(error instanceof Error ? error.message : t('common.operationFailed'))
      if (error instanceof ApiError && error.status === 409) {
        setLocal(null)
        setEdited(true)
        setConsent(false)
        void client.invalidateQueries({ queryKey: pipelineDetailQueryKey(pipeline.id) })
      }
    }
  }
  const handlingHint = (mode: ReinitTargetHandling) => t(`pipelines.reinit.${mode}Hint`)
  return (
    <Drawer
      open
      title={
        <span className="section-title">
          {t('pipelines.reinit.title')}
          <HintIcon content={t('pipelines.reinit.hint')} />
          {!stopped && canWrite && (
            <HintIcon content={t('pipelines.reinit.stopRequired')} />
          )}
        </span>
      }
      onClose={onClose}
      size="min(880px, 100vw)"
      mask={{ closable: !busy }}
      closable={!busy}
      keyboard={!busy}
    >
      {(error || saved.isError) && (
        <HintIcon type="error" content={error ?? saved.error?.message} />
      )}
      <Space orientation="vertical" size="middle" style={{ width: '100%', minWidth: 0 }}>
        <Space wrap>
          <Button
            aria-label={t('common.refresh')}
            onClick={() => void saved.refetch()}
            loading={saved.isFetching}
            disabled={busy}
          >
            {t('common.refresh')}
          </Button>
          {!record && !saved.isPending && !saved.isError && (
            <HintIcon content={<> {t('pipelines.reinit.noPreview')} </>} />
          )}
        </Space>
        {canWrite && (
          <Form layout="vertical">
            <HintForm label={t('pipelines.reinit.schemas')}>
              <Input.TextArea
                rows={2}
                value={schemas}
                disabled={!editable}
                aria-label={t('pipelines.reinit.schemas')}
                placeholder={t('pipelines.reinit.schemasPlaceholder')}
                onChange={(event) => {
                  change()
                  setSchemas(event.target.value)
                }}
              />
            </HintForm>
            <HintForm
              label={t('pipelines.reinit.targetHandling')}
              extra={handling ? handlingHint(handling) : undefined}
            >
              <Radio.Group
                value={handling}
                disabled={!editable}
                onChange={(event) => {
                  change()
                  setHandling(event.target.value as ReinitTargetHandling)
                }}
              >
                <Radio value="keep">{t('pipelines.reinit.keep')}</Radio>
                <Radio value="clear">{t('pipelines.reinit.clear')}</Radio>
              </Radio.Group>
            </HintForm>
            <HintForm>
              <Checkbox
                checked={schemaMigration}
                disabled={!editable}
                onChange={(event) => {
                  change()
                  setSchemaMigration(event.target.checked)
                }}
              >
                {t('pipelines.reinit.schemaMigration')}
              </Checkbox>
            </HintForm>
            {schemaMigration && (
              <HintForm label={t('pipelines.reinit.definerFallback')}>
                <Input
                  value={definerFallback}
                  disabled={!editable}
                  aria-label={t('pipelines.reinit.definerFallback')}
                  onChange={(event) => {
                    change()
                    setDefinerFallback(event.target.value)
                  }}
                />
              </HintForm>
            )}
            <Button
              onClick={() => void preview()}
              loading={previewMutation.isPending}
              disabled={
                !editable || !handling || schemaList.length === 0 || schemaList.length > 100
              }
            >
              {t('pipelines.reinit.preview')}
            </Button>
          </Form>
        )}
        {saved.isPending && <Typography.Text>{t('common.loading')}</Typography.Text>}
        {record && (
          <>
            <Descriptions bordered column={1} size="small">
              <Descriptions.Item label={t('pipelines.reinit.oldRun')}>
                {record.fromRunId}
              </Descriptions.Item>
              <Descriptions.Item
                label={
                  <span className="field-label-hint">
                    {t('pipelines.detail.configVersion')}
                    {!record.confirmedAt && !currentPreview && (
                      <Alert type="info" title={t('pipelines.reinit.previewRequired')} />
                    )}
                  </span>
                }
              >
                {record.configVersion}
              </Descriptions.Item>
              <Descriptions.Item label={t('pipelines.reinit.schemas')}>
                {record.document.schemas.join(', ')}
              </Descriptions.Item>
              <Descriptions.Item label={t('pipelines.reinit.targetHandling')}>
                {t(`pipelines.reinit.${record.document.targetHandling}`)}
                <br />
                {handlingHint(record.document.targetHandling)}
              </Descriptions.Item>
            </Descriptions>
            <ScrollTable
              size="small"
              dataSource={record.document.tables}
              rowKey={(item) => JSON.stringify([item.sourceSchema, item.sourceTable])}
              pagination={{ pageSize: 10 }}
              scroll={{ x: 600 }}
              columns={[
                {
                  title: t('pipelines.reinit.sourceTable'),
                  render: (_, item) => `${item.sourceSchema}.${item.sourceTable}`,
                },
                {
                  title: t('pipelines.reinit.targetTable'),
                  render: (_, item) => `${item.schema}.${item.table}`,
                },
                {
                  title: t('pipelines.reinit.targetDefinition'),
                  render: (_, item) =>
                    item.expectedDdl ? (
                      <Typography.Paragraph
                        code
                        style={{
                          maxWidth: 320,
                          whiteSpace: 'pre-wrap',
                          overflowWrap: 'anywhere',
                        }}
                      >
                        {item.expectedDdl}
                      </Typography.Paragraph>
                    ) : (
                      t('pipelines.reinit.newTable')
                    ),
                },
              ]}
            />
            {record.document.schemaPlan && (
              <>
                <Typography.Title level={5}>
                  <span className="section-title">
                    {t('pipelines.reinit.schemaPlan')}
                    {record.document.schemaPlan.blocked && (
                      <Alert type="error" title={t('pipelines.reinit.blocked')} />
                    )}
                  </span>
                </Typography.Title>
                <ScrollTable<SchemaPlanStep>
                  size="small"
                  rowKey="seq"
                  dataSource={record.document.schemaPlan.steps}
                  pagination={{ pageSize: 10 }}
                  scroll={{ x: 600 }}
                  expandable={{
                    expandedRowRender: (item) => (
                      <Space orientation="vertical" style={{ width: '100%' }}>
                        <Typography.Text
                          code
                          style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}
                        >
                          {item.ddl}
                        </Typography.Text>
                        {item.differences?.map((diff, index) => (
                          <Typography.Text
                            key={index}
                          >{`${diff.path}: ${diff.expected} → ${diff.actual}`}</Typography.Text>
                        ))}
                      </Space>
                    ),
                  }}
                  columns={[
                    {
                      title: t('pipelines.reinit.object'),
                      render: (_, item) => `${item.schema}.${item.name}`,
                    },
                    {
                      title: t('pipelines.reinit.kind'),
                      dataIndex: 'kind',
                      render: (value: SchemaPlanStep['kind']) =>
                        t(`pipelines.reinit.kinds.${value}`),
                    },
                    {
                      title: t('pipelines.reinit.action'),
                      dataIndex: 'action',
                      render: (value: SchemaPlanStep['action']) =>
                        t(`pipelines.reinit.actions.${value}`),
                    },
                    {
                      title: t('pipelines.reinit.execution'),
                      render: (_, item) =>
                        item.afterSnapshot
                          ? t('pipelines.reinit.afterSnapshot')
                          : t('pipelines.reinit.beforeSnapshot'),
                    },
                    {
                      title: t('pipelines.reinit.blockReason'),
                      dataIndex: 'blockReason',
                      render: (reason: string) => (
                        <HintIcon type="error" content={reason} />
                      ),
                    },
                  ]}
                />
              </>
            )}
            {!record.confirmedAt && (
              <>
                {canWrite && (
                  <Space orientation="vertical">
                    <Checkbox
                      checked={consent}
                      disabled={!confirmable}
                      onChange={(event) => setConsent(event.target.checked)}
                    >
                      <span className="field-label-hint">
                        {t('pipelines.reinit.consent', {
                          handling: t(`pipelines.reinit.${record.document.targetHandling}`),
                        })}
                      </span>
                    </Checkbox>
                    <Button
                      type="primary"
                      danger={record.document.targetHandling === 'clear'}
                      loading={confirmMutation.isPending}
                      disabled={!confirmable || !consent}
                      onClick={() => void confirm()}
                    >
                      {t('pipelines.reinit.confirm')}
                    </Button>
                  </Space>
                )}
              </>
            )}
            {record.confirmedAt && (
              <>
                <Descriptions column={1} size="small" bordered>
                  <Descriptions.Item
                    label={
                      <span className="field-label-hint">
                        {t('pipelines.reinit.newRun')}{' '}
                        <Alert
                          type="info"
                          showIcon
                          title={t('pipelines.reinit.accepted')}
                          description={t('pipelines.reinit.actualStateHint')}
                        />
                      </span>
                    }
                  >
                    {record.newRunId}
                  </Descriptions.Item>
                  <Descriptions.Item label={t('pipelines.reinit.snapshotUuid')}>
                    <Typography.Text code>{record.snapshotUuid}</Typography.Text>
                  </Descriptions.Item>
                  {record.schemaUuid && (
                    <Descriptions.Item label={t('pipelines.reinit.schemaUuid')}>
                      <Typography.Text code>{record.schemaUuid}</Typography.Text>
                    </Descriptions.Item>
                  )}
                  {record.prepareUuid && (
                    <Descriptions.Item label={t('pipelines.reinit.prepareUuid')}>
                      <Typography.Text code>{record.prepareUuid}</Typography.Text>
                    </Descriptions.Item>
                  )}
                  <Descriptions.Item label={t('pipelines.reinit.confirmedAt')}>
                    {formatDateTime(record.confirmedAt)}
                  </Descriptions.Item>
                  <Descriptions.Item label={t('pipelines.reinit.sourceAck')}>
                    <Tag>
                      {t(
                        record.sourceApplied
                          ? 'pipelines.reinit.acked'
                          : 'pipelines.reinit.waiting',
                      )}
                    </Tag>
                  </Descriptions.Item>
                  <Descriptions.Item label={t('pipelines.reinit.targetAck')}>
                    <Tag>
                      {t(
                        record.targetApplied
                          ? 'pipelines.reinit.acked'
                          : 'pipelines.reinit.waiting',
                      )}
                    </Tag>
                  </Descriptions.Item>
                </Descriptions>
              </>
            )}
          </>
        )}
      </Space>
    </Drawer>
  )
}
