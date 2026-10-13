import { ScrollTable } from '@/components/ScrollTable'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintIcon } from '@/components/HintIcon'
import { useEffect, useState } from 'react'
import { Button, Card, Checkbox, Descriptions, Space, Tag, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import { type PipelineSummary } from '@/api/pipelines'
import type { PrecheckItem } from '@/api/precheck'
import {
  usePipelinePrecheckMutation,
  usePipelinePrecheckQuery,
} from '@/api/precheckQueries'
import { formatDateTime } from '@/utils/format'
import { canPrecheckPipeline } from './pipelineStateRules'
import { PipelineRiskPanel } from './PipelineRiskPanel'

export function PipelinePrecheckPanel({
  pipeline,
  canWrite,
}: {
  pipeline: PipelineSummary
  canWrite: boolean
}) {
  const { t } = useTranslation()
  const query = usePipelinePrecheckQuery(pipeline.id)
  const run = usePipelinePrecheckMutation(pipeline.id)
  const confirm = usePipelinePrecheckMutation(pipeline.id, true)
  const [selected, setSelected] = useState<{ batchId: string; ids: string[] }>({
    batchId: '',
    ids: [],
  })
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [])
  const result = query.data
  const busy = run.isPending || confirm.isPending
  const current =
    !!result &&
    result.complete &&
    result.configVersion === pipeline.configVersion &&
    Date.parse(result.expiresAt) > now &&
    !query.isError
  const blocked =
    result?.items.some((item) => item.level === 'blocking' && item.status !== 'pass') ??
    true
  const canConfirm =
    canWrite &&
    current &&
    !blocked &&
    !busy &&
    !query.isFetching &&
    result?.reason === 'precheck_confirmation_required'
  const ids =
    selected.batchId === result?.batchId
      ? selected.ids.filter((id) =>
          result.items.some(
            (item) =>
              item.id === id &&
              item.level === 'warning' &&
              item.status === 'fail' &&
              !item.confirmed,
          ),
        )
      : []
  const inactive = canPrecheckPipeline(pipeline)
  const detail = (item: PrecheckItem) => {
    const value = item.detail
    if (
      typeof value === 'object' &&
      value !== null &&
      'message' in value &&
      typeof value.message === 'string'
    )
      return value.message
    return typeof value === 'string' ? value : ''
  }
  const error = run.error ?? confirm.error ?? query.error
  const reason =
    result?.configVersion !== pipeline.configVersion && result
      ? 'precheck_changed'
      : result && Date.parse(result.expiresAt) <= now
        ? 'precheck_expired'
        : result?.reason
  const reasonLabel =
    reason &&
    t(`pipelines.precheck.reasons.${reason}`, {
      defaultValue: t('pipelines.precheck.unavailable'),
    })
  return (
    <div id="precheck" className="pipeline-precheck-panel">
      <Card size="small" className="pipeline-fill-card">
        <Space wrap>
          <Space size={6}>
            <Button
              aria-label={t('common.refresh')}
              onClick={() => void query.refetch()}
              loading={query.isFetching}
              disabled={busy}
            >
              {t('common.refresh')}
            </Button>
            {error && <Alert type="error" showIcon title={error.message} />}
            {!query.isLoading && !result && !query.isError && (
              <HintIcon content={t('pipelines.precheck.empty')} />
            )}
          </Space>
          <Space size={6}>
            {canWrite && (
              <Button
                type="primary"
                loading={run.isPending}
                disabled={!inactive || busy || query.isFetching}
                onClick={() => {
                  setSelected({ batchId: '', ids: [] })
                  confirm.reset()
                  run.mutate(undefined)
                }}
              >
                {t('pipelines.precheck.run')}
              </Button>
            )}
            <HintIcon
              content={
                <>
                  <div>{t('pipelines.precheck.responsibility')}</div>
                  {!inactive && canWrite && (
                    <div>{t('pipelines.precheck.stopFirst')}</div>
                  )}
                </>
              }
            />
          </Space>
          <PipelineRiskPanel pipelineId={pipeline.id} canWrite={canWrite} />
        </Space>
        {query.isLoading && <Typography.Text>{t('common.loading')}</Typography.Text>}
        {result && (
          <>
            <Descriptions
              size="small"
              column={{ xs: 1, sm: 2 }}
              items={[
                {
                  key: 'batch',
                  label: (
                    <span className="field-label-hint">
                      {t('pipelines.precheck.batch')}
                      <Alert
                        type={
                          busy || !result.complete
                            ? 'info'
                            : current && result.canStart
                              ? 'success'
                              : 'error'
                        }
                        title={
                          busy || !result.complete
                            ? t('pipelines.precheck.pending')
                            : current && result.canStart
                              ? t('pipelines.precheck.ready')
                              : reasonLabel || t('pipelines.precheck.unavailable')
                        }
                      />
                    </span>
                  ),
                  children: <Typography.Text copyable>{result.batchId}</Typography.Text>,
                },
                {
                  key: 'version',
                  label: t('pipelines.detail.configVersion'),
                  children: result.configVersion,
                },
                {
                  key: 'checked',
                  label: t('pipelines.precheck.checkedAt'),
                  children: formatDateTime(result.checkedAt),
                },
                {
                  key: 'expires',
                  label: t('pipelines.precheck.expiresAt'),
                  children: formatDateTime(result.expiresAt),
                },
              ]}
            />
            <div className="table-fill-container">
              <ScrollTable<PrecheckItem>
                rowKey="id"
                dataSource={result.items}
                pagination={false}
                size="small"
                scroll={{ x: 720 }}
                columns={[
                  {
                    key: 'executor',
                    title: t('pipelines.precheck.executor'),
                    render: (_value, item) =>
                      t(`pipelines.precheck.executors.${item.item.split('/')[0]}`, {
                        defaultValue: item.item.split('/')[0] ?? item.item,
                      }),
                  },
                  {
                    key: 'item',
                    title: t('pipelines.precheck.item'),
                    render: (_value, item) =>
                      item.item.split('/').slice(1).join('/') || item.item,
                  },
                  {
                    key: 'level',
                    title: t('pipelines.precheck.level'),
                    render: (_value, item) =>
                      t(`pipelines.precheck.levels.${item.level}`, {
                        defaultValue: item.level,
                      }),
                  },
                  {
                    key: 'status',
                    title: t('pipelines.precheck.status'),
                    render: (_value, item) => (
                      <Tag
                        color={
                          item.status === 'pass'
                            ? 'success'
                            : item.status === 'fail'
                              ? 'error'
                              : 'default'
                        }
                      >
                        {t(`pipelines.precheck.statuses.${item.status}`, {
                          defaultValue: item.status,
                        })}
                      </Tag>
                    ),
                  },
                  {
                    key: 'detail',
                    title: t('pipelines.precheck.detail'),
                    render: (_value, item) => (
                      <Space size={6}>
                        {item.status === 'fail' && (
                          <HintIcon type="error" content={detail(item)} />
                        )}
                        {item.detail !== undefined && (
                          <HintIcon
                            content={
                              <pre
                                style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}
                              >
                                {JSON.stringify(item.detail, null, 2)}
                              </pre>
                            }
                          />
                        )}
                      </Space>
                    ),
                  },
                  {
                    key: 'confirmation',
                    title: t('pipelines.precheck.confirmation'),
                    render: (_value, item) =>
                      item.confirmed ? (
                        <span>
                          {t('pipelines.precheck.confirmed')}{' '}
                          {formatDateTime(item.confirmedAt)}
                        </span>
                      ) : item.level === 'warning' && item.status === 'fail' && canWrite ? (
                        <Checkbox
                          aria-label={t('pipelines.precheck.select', { item: item.item })}
                          checked={ids.includes(item.id)}
                          disabled={!canConfirm}
                          onChange={(event) =>
                            setSelected({
                              batchId: result.batchId,
                              ids: event.target.checked
                                ? [...ids, item.id]
                                : ids.filter((id) => id !== item.id),
                            })
                          }
                        />
                      ) : (
                        '—'
                      ),
                  },
                ]}
              />
            </div>
            {canWrite && (
              <Space>
                <Button
                  loading={confirm.isPending}
                  disabled={!canConfirm || ids.length === 0}
                  onClick={() =>
                    confirm.mutate(
                      { batchId: result.batchId, items: ids },
                      { onSuccess: () => setSelected({ batchId: '', ids: [] }) },
                    )
                  }
                >
                  {t('pipelines.precheck.confirm')}
                </Button>
              </Space>
            )}
          </>
        )}
      </Card>
    </div>
  )
}
