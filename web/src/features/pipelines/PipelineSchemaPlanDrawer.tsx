import { ScrollTable } from '@/components/ScrollTable'
import { HintIcon } from '@/components/HintIcon'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { schemaPlanDDLURL } from '@/api/generated'
import { useState } from 'react'
import {
  Button,
  Checkbox,
  Descriptions,
  Drawer,
  Form,
  Input,
  Space,
  Tag,
  Typography,
} from 'antd'
import { useTranslation } from 'react-i18next'
import {
  useSchemaPlanQuery,
  useSchemaPlanPreviewMutation,
  useSchemaPlanConfirmMutation,
} from '@/api/initializationQueries'
import type { PipelineSummary } from '@/api/pipelines'
import type { SchemaPlanStep } from '@/api/reinit'
import { formatDateTime } from '@/utils/format'

export function PipelineSchemaPlanDrawer({
  pipeline,
  canWrite,
  onClose,
}: {
  pipeline: PipelineSummary
  canWrite: boolean
  onClose: () => void
}) {
  const { t } = useTranslation()
  const query = useSchemaPlanQuery(pipeline.id)
  const preview = useSchemaPlanPreviewMutation(pipeline.id)
  const confirm = useSchemaPlanConfirmMutation(pipeline.id)
  const [schemas, setSchemas] = useState('')
  const [fallback, setFallback] = useState('')
  const [reviewedDigest, setReviewedDigest] = useState('')
  const [consentDigest, setConsentDigest] = useState('')
  const record = query.data
  const busy = preview.isPending || confirm.isPending
  const stopped =
    pipeline.desiredState === 'stopped' &&
    ['draft', 'stopped'].includes(pipeline.sourceActualState) &&
    ['draft', 'stopped'].includes(pipeline.targetActualState)
  const editable = canWrite && stopped && !record?.confirmedAt && !busy
  const current =
    !!record &&
    !query.isError &&
    !query.isFetching &&
    record.configVersion === pipeline.configVersion &&
    reviewedDigest === record.digest
  const confirmable = editable && current && !record.plan.blocked
  const scope = schemas
    .split(/[,\n]/)
    .map((value) => value.trim())
    .filter(Boolean)
  const invalidateConsent = () => {
    setReviewedDigest('')
    setConsentDigest('')
    confirm.reset()
  }
  const error = preview.error ?? confirm.error ?? query.error
  return (
    <Drawer
      open
      title={
        <span className="section-title">
          {t('pipelines.schemaPlan.title')}
          <HintIcon content={t('pipelines.schemaPlan.description')} />
          {!stopped && canWrite && (
            <HintIcon content={t('pipelines.schemaPlan.stopFirst')} />
          )}
        </span>
      }
      onClose={onClose}
      size={900}
    >
      {error && <HintIcon type="error" content={error.message} />}
      <Space orientation="vertical" style={{ width: '100%' }}>
        <Space wrap>
          <Button
            loading={query.isFetching}
            disabled={busy}
            onClick={() => {
              invalidateConsent()
              void query.refetch()
            }}
          >
            {t('common.refresh')}
          </Button>
          {!query.isLoading && !record && !query.isError && (
            <HintIcon content={<> {t('pipelines.schemaPlan.empty')} </>} />
          )}
        </Space>
        {canWrite && !record?.confirmedAt && (
          <Form layout="vertical">
            <HintForm label={t('pipelines.reinit.schemas')}>
              <Input.TextArea
                aria-label={t('pipelines.reinit.schemas')}
                value={schemas}
                disabled={!editable}
                rows={2}
                placeholder={t('pipelines.reinit.schemasPlaceholder')}
                onChange={(event) => {
                  setSchemas(event.target.value)
                  invalidateConsent()
                }}
              />
            </HintForm>
            <HintForm label={t('pipelines.reinit.definerFallback')}>
              <Input
                aria-label={t('pipelines.reinit.definerFallback')}
                value={fallback}
                disabled={!editable}
                onChange={(event) => {
                  setFallback(event.target.value)
                  invalidateConsent()
                }}
              />
            </HintForm>
            <Button
              loading={preview.isPending}
              disabled={
                !editable || query.isFetching || scope.length === 0 || scope.length > 100
              }
              onClick={() => {
                invalidateConsent()
                preview.mutate(
                  {
                    schemas: scope,
                    ...(fallback.trim() ? { definerFallback: fallback.trim() } : {}),
                  },
                  { onSuccess: (result) => setReviewedDigest(result.digest) },
                )
              }}
            >
              {t('pipelines.schemaPlan.preview')}
            </Button>
          </Form>
        )}
        {query.isLoading && <Typography.Text>{t('common.loading')}</Typography.Text>}
        {record && (
          <>
            <Descriptions
              column={1}
              size="small"
              items={[
                {
                  key: 'run',
                  label: (
                    <span className="field-label-hint">
                      {t('pipelines.snapshot.run')}{' '}
                      <Alert
                        showIcon
                        type={
                          record.plan.blocked
                            ? 'error'
                            : record.confirmedAt
                              ? 'success'
                              : 'info'
                        }
                        title={t(
                          record.plan.blocked
                            ? 'pipelines.reinit.blocked'
                            : record.confirmedAt
                              ? 'pipelines.schemaPlan.confirmed'
                              : 'pipelines.schemaPlan.review',
                        )}
                      />
                      {record.confirmedAt && (
                        <HintIcon content={t('pipelines.schemaPlan.confirmedHint')} />
                      )}
                    </span>
                  ),
                  children: record.runId,
                },
                {
                  key: 'version',
                  label: t('pipelines.detail.configVersion'),
                  children: record.configVersion,
                },
                {
                  key: 'digest',
                  label: t('pipelines.schemaPlan.digest'),
                  children: (
                    <Typography.Text code style={{ overflowWrap: 'anywhere' }}>
                      {record.digest}
                    </Typography.Text>
                  ),
                },
                ...(record.confirmedAt
                  ? [
                      {
                        key: 'confirmed',
                        label: t('pipelines.reinit.confirmedAt'),
                        children: formatDateTime(record.confirmedAt),
                      },
                    ]
                  : []),
              ]}
            />
            <ScrollTable<SchemaPlanStep>
              size="small"
              rowKey="seq"
              dataSource={record.plan.steps}
              pagination={{ pageSize: 10 }}
              scroll={{ x: 650 }}
              expandable={{
                expandedRowRender: (item) => (
                  <Space orientation="vertical" style={{ width: '100%' }}>
                    <Typography.Text type="secondary">
                      {t('pipelines.schemaPlan.sourceDDL')}
                    </Typography.Text>
                    <pre style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>
                      {item.sourceDdl}
                    </pre>
                    <Typography.Text type="secondary">
                      {t('pipelines.schemaPlan.targetDDL')}
                    </Typography.Text>
                    <pre style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>
                      {item.ddl}
                    </pre>
                    {item.differences?.map((diff, index) => (
                      <Typography.Text
                        key={index}
                      >{`${diff.path}: ${diff.expected} → ${diff.actual}`}</Typography.Text>
                    ))}
                  </Space>
                ),
              }}
              columns={[
                { title: t('pipelines.schemaPlan.sequence'), dataIndex: 'seq', width: 56 },
                {
                  title: t('pipelines.reinit.object'),
                  render: (_, item) => `${item.schema}.${item.name}`,
                },
                {
                  title: t('pipelines.reinit.kind'),
                  render: (_, item) => t(`pipelines.reinit.kinds.${item.kind}`),
                },
                {
                  title: t('pipelines.reinit.action'),
                  render: (_, item) => (
                    <Tag
                      color={
                        item.action === 'block'
                          ? 'error'
                          : item.action === 'create'
                            ? 'processing'
                            : 'default'
                      }
                    >
                      {t(`pipelines.reinit.actions.${item.action}`)}
                    </Tag>
                  ),
                },
                {
                  title: t('pipelines.reinit.execution'),
                  render: (_, item) =>
                    t(
                      item.afterSnapshot
                        ? 'pipelines.reinit.afterSnapshot'
                        : 'pipelines.reinit.beforeSnapshot',
                    ),
                },
                {
                  title: t('pipelines.reinit.blockReason'),
                  dataIndex: 'blockReason',
                  render: (reason: string) => <HintIcon type="error" content={reason} />,
                },
              ]}
            />
            {!record.plan.blocked && (
              <Button href={schemaPlanDDLURL(pipeline.id)} download="schema-migration.sql">
                {t('pipelines.schemaPlan.export')}
              </Button>
            )}
            {!record.confirmedAt && canWrite && (
              <Space wrap>
                <Checkbox
                  checked={consentDigest === record.digest}
                  disabled={!confirmable}
                  onChange={(event) =>
                    setConsentDigest(event.target.checked ? record.digest : '')
                  }
                >
                  <span className="field-label-hint">
                    {t('pipelines.schemaPlan.consent')}
                    {!current && (
                      <Alert
                        type="info"
                        title={t('pipelines.schemaPlan.previewRequired')}
                      />
                    )}
                  </span>
                </Checkbox>
                <Button
                  type="primary"
                  loading={confirm.isPending}
                  disabled={!confirmable || consentDigest !== record.digest}
                  onClick={() =>
                    confirm.mutate(record.digest, { onSettled: invalidateConsent })
                  }
                >
                  {t('pipelines.schemaPlan.confirm')}
                </Button>
              </Space>
            )}
          </>
        )}
      </Space>
    </Drawer>
  )
}
