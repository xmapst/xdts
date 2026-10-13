import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintIcon } from '@/components/HintIcon'
import { useEffect, useState } from 'react'
import { Button, Card, Descriptions, Progress, Space, Tag } from 'antd'
import { useTranslation } from 'react-i18next'
import { useNavigate } from 'react-router-dom'
import type { PipelineSummary } from '@/api/pipelines'
import { usePausePipelineMutation, useResumePipelineMutation } from '@/api/pipelines'
import { usePipelineSnapshotQuery } from '@/api/initializationQueries'
import { isPipelinePrecheckError } from '@/api/pipelineResume'
import { canResumePipeline } from './pipelineStateRules'
import type { SnapshotTable } from '@/api/snapshot'
import { formatDateTime } from '@/utils/format'

export function PipelineSnapshotPanel({
  pipeline,
  canWrite,
}: {
  pipeline: PipelineSummary
  canWrite: boolean
}) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const navigate = useNavigate()
  const [page, setPage] = useState(1)
  const query = usePipelineSnapshotQuery(pipeline.id, { page, pageSize: 20 })
  const pause = usePausePipelineMutation()
  const resume = useResumePipelineMutation()
  const canResume = canResumePipeline(pipeline)
  const run = query.data
  const busy = pause.isPending || resume.isPending
  const active =
    pipeline.desiredState === 'running' &&
    run?.status !== 'converged' &&
    run?.status !== 'failed'
  const [now, setNow] = useState(Date.now)
  useEffect(() => {
    if (!active) return
    const timer = window.setInterval(() => setNow(Date.now()), 5000)
    return () => window.clearInterval(timer)
  }, [active])
  const formatCount = (value: number | null) =>
    value === null ? '—' : value.toLocaleString()
  const rate = (table: SnapshotTable) =>
    active &&
    table.status === 'reading' &&
    table.observedAt &&
    now - Date.parse(table.observedAt) <= 15000
      ? table.rowsPerSec
      : null
  const eta = (table: SnapshotTable) => {
    if (table.status === 'done') return '0 s'
    const speed = rate(table)
    return speed && speed > 0 && table.estimatedRows > table.doneRows
      ? `${Math.ceil((table.estimatedRows - table.doneRows) / speed).toLocaleString()} s`
      : '—'
  }
  if (!run && !query.isError) return null
  return (
    <Card
      id="snapshot"
      size="small"
      title={
        <span className="field-label-hint">
          {t('pipelines.snapshot.title')}
          <HintIcon content={t('pipelines.snapshot.estimateHint')} />
        </span>
      }
      style={{ marginBottom: 16 }}
    >
      <Space orientation="vertical" style={{ width: '100%' }}>
        <Space wrap>
          <Button loading={query.isFetching} onClick={() => void query.refetch()}>
            {t('common.refresh')}
          </Button>
          {canWrite &&
            run &&
            run.status !== 'converged' &&
            run.status !== 'failed' &&
            (pipeline.desiredState === 'paused' ? (
              <Button
                disabled={!canResume || busy}
                loading={resume.isPending}
                onClick={() =>
                  resume.mutate(pipeline.id, {
                    onError: (error) => {
                      void message.error(error.message)
                      if (isPipelinePrecheckError(error))
                        navigate(`/pipelines/${pipeline.id}#precheck`)
                    },
                  })
                }
              >
                {t('pipelines.snapshot.resume')}
              </Button>
            ) : (
              <Button
                disabled={!active || busy}
                loading={pause.isPending}
                onClick={() =>
                  pause.mutate(pipeline.id, {
                    onError: (error) => void message.error(error.message),
                  })
                }
              >
                {t('pipelines.snapshot.pause')}
              </Button>
            ))}
          {query.isError && <Alert type="error" title={query.error.message} />}
        </Space>
        {run && (
          <>
            <Descriptions
              size="small"
              column={{ xs: 1, sm: 2 }}
              items={[
                { key: 'run', label: t('pipelines.snapshot.run'), children: run.runId },
                {
                  key: 'state',
                  label: t('pipelines.snapshot.state'),
                  children: <Tag>{t(`pipelines.snapshot.states.${run.status}`)}</Tag>,
                },
                {
                  key: 'chunks',
                  label: t('pipelines.snapshot.chunks'),
                  children: `${formatCount(run.doneChunks)} / ${formatCount(run.totalChunks)}`,
                },
                {
                  key: 'catchup',
                  label: t('pipelines.snapshot.catchup'),
                  children: formatCount(run.catchupRemainingTxns),
                },
                {
                  key: 'updated',
                  label: t('pipelines.snapshot.updatedAt'),
                  children: formatDateTime(run.updatedAt),
                },
                {
                  key: 'finished',
                  label: t('pipelines.snapshot.finishedAt'),
                  children: formatDateTime(run.finishedAt),
                },
              ]}
            />
            {run.totalChunks > 0 && (
              <Progress
                percent={Math.floor((run.doneChunks / run.totalChunks) * 100)}
                status={
                  run.status === 'failed'
                    ? 'exception'
                    : run.status === 'converged'
                      ? 'success'
                      : 'normal'
                }
              />
            )}
            <ScrollTable<SnapshotTable>
              size="small"
              rowKey={(table) => JSON.stringify([table.schema, table.table])}
              dataSource={run.tables.items}
              scroll={{ x: 720 }}
              pagination={{
                current: page,
                pageSize: 20,
                total: run.tables.total,
                showSizeChanger: false,
                onChange: setPage,
              }}
              columns={[
                {
                  title: t('pipelines.reinit.sourceTable'),
                  render: (_, table) => `${table.schema}.${table.table}`,
                },
                {
                  title: t('pipelines.snapshot.estimatedRows'),
                  render: (_, table) => formatCount(table.estimatedRows),
                },
                {
                  title: t('pipelines.snapshot.doneRows'),
                  render: (_, table) => formatCount(table.doneRows),
                },
                {
                  title: t('pipelines.snapshot.chunks'),
                  render: (_, table) =>
                    `${formatCount(table.doneChunks)} / ${formatCount(table.totalChunks)}`,
                },
                {
                  title: t('pipelines.snapshot.rate'),
                  render: (_, table) =>
                    rate(table)?.toLocaleString(undefined, { maximumFractionDigits: 1 }) ??
                    '—',
                },
                { title: t('pipelines.snapshot.eta'), render: (_, table) => eta(table) },
                {
                  title: t('pipelines.snapshot.state'),
                  render: (_, table) => t(`pipelines.snapshot.tableStates.${table.status}`),
                },
              ]}
            />
          </>
        )}
      </Space>
    </Card>
  )
}
