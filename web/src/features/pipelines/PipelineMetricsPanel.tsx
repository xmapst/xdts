import { useEffect, useState } from 'react'
import { Button, Card, DatePicker, Select, Space, Typography } from 'antd'
import dayjs from 'dayjs'
import { useTranslation } from 'react-i18next'
import { usePipelineMetricsQuery, type MetricHistory, type MetricsQuery } from '@/api/metrics'
import type { PipelineSummary } from '@/api/pipelines'
import { HintIcon } from '@/components/HintIcon'
import { ScrollTable } from '@/components/ScrollTable'
import { formatBytes, formatCount, formatDateTime } from '@/utils/format'

export function PipelineMetricsPanel({
  pipeline,
  sourceNodeName,
  targetNodeName,
}: {
  pipeline: PipelineSummary
  sourceNodeName: string
  targetNodeName: string
}) {
  const { t } = useTranslation()
  const [period, setPeriod] = useState(24)
  const [params, setParams] = useState<MetricsQuery>({
    windowHours: 24,
    activeOnly: true,
    page: 1,
    pageSize: 20,
  })
  const [range, setRange] = useState<[dayjs.Dayjs, dayjs.Dayjs] | null>(null)
  const [now, setNow] = useState(Date.now)
  const query = usePipelineMetricsQuery(pipeline.id, params)
  const currentTime = Math.max(now, query.dataUpdatedAt)
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 5000)
    return () => window.clearInterval(timer)
  }, [])
  const decimal = (value: number | null | undefined) =>
    value == null ? '—' : value.toLocaleString(undefined, { maximumFractionDigits: 2 })
  const roleName = (side: 'source' | 'target' | null) =>
    side ? t(`pipelines.side.${side}`) : '—'
  const nodeName = (side: 'source' | 'target' | null) =>
    side === 'source' ? sourceNodeName : side === 'target' ? targetNodeName : '—'
  const fresh = (observed: number | null | undefined, reference: number) =>
    observed != null && observed <= reference && reference - observed <= 15000
  const live = (side: 'source' | 'target') => {
    const observation = query.data?.latest.find((row) => row.role === side)
    const state = side === 'source' ? pipeline.sourceActualState : pipeline.targetActualState
    const active =
      pipeline.desiredState === 'running' &&
      ['running', 'snapshotting', 'catching_up'].includes(state)
    return {
      side,
      observation,
      metricsKnown:
        !query.isError && active && fresh(observation?.values.metricsObservedTsMs, currentTime),
      queueKnown:
        !query.isError &&
        side === 'target' &&
        active &&
        fresh(observation?.values.queueObservedTsMs, currentTime),
    }
  }
  const metricKnown = (row: MetricHistory) =>
    fresh(row.values?.metricsObservedTsMs, Date.parse(row.observedAt))
  const queueKnown = (row: MetricHistory) =>
    row.role === 'target' && fresh(row.values?.queueObservedTsMs, Date.parse(row.observedAt))
  const scopeHint = (
    <HintIcon
      content={t('pipelines.metrics.hint', {
        seconds: query.data?.sampleIntervalSeconds ?? 5,
      })}
    />
  )
  return (
    <Card id="sync-records" size="small" style={{ marginBottom: 16 }}>
      <Space orientation="vertical" style={{ width: '100%' }} size={8}>
        <Space wrap>
          {scopeHint}
          <Select
            aria-label={t('pipelines.metrics.timeRange')}
            value={period}
            style={{ width: 140 }}
            options={[
              { value: 1, label: t('pipelines.metrics.lastHour') },
              { value: 24, label: t('pipelines.metrics.lastDay') },
              { value: 168, label: t('pipelines.metrics.lastWeek') },
              { value: 0, label: t('pipelines.metrics.custom') },
            ]}
            onChange={(hours) => {
              setPeriod(hours)
              if (hours) {
                setRange(null)
                setParams((previous) => ({
                  activeOnly: previous.activeOnly,
                  pageSize: previous.pageSize,
                  page: 1,
                  windowHours: hours,
                }))
              } else if (query.data) {
                setRange([dayjs(query.data.from), dayjs(query.data.to)])
                setParams((previous) => ({
                  activeOnly: previous.activeOnly,
                  pageSize: previous.pageSize,
                  page: 1,
                  from: query.data!.from,
                  to: query.data!.to,
                }))
              }
            }}
          />
          {period === 0 && (
            <DatePicker.RangePicker
              aria-label={t('pipelines.metrics.customRange')}
              showTime
              allowClear={false}
              style={{ maxWidth: '100%' }}
              value={range}
              disabledDate={(date, info) =>
                date.isAfter(dayjs(), 'day') ||
                (!!info.from && Math.abs(date.diff(info.from, 'day')) > 30)
              }
              onChange={(dates) => {
                if (!dates?.[0] || !dates[1]) return
                setRange([dates[0], dates[1]])
                setParams((previous) => ({
                  activeOnly: previous.activeOnly,
                  pageSize: previous.pageSize,
                  page: 1,
                  from: dates[0]!.toISOString(),
                  to: dates[1]!.toISOString(),
                }))
              }}
            />
          )}
          <Select
            aria-label={t('pipelines.metrics.activityFilter')}
            value={params.activeOnly ? 'active' : 'all'}
            style={{ width: 150 }}
            options={[
              { value: 'active', label: t('pipelines.metrics.activeOnly') },
              { value: 'all', label: t('pipelines.metrics.allSamples') },
            ]}
            onChange={(value) =>
              setParams((previous) => ({
                ...previous,
                activeOnly: value === 'active',
                page: 1,
              }))
            }
          />
          <Button
            loading={query.isFetching}
            onClick={() => {
              if (period && params.windowHours === undefined) {
                setParams((previous) => ({
                  activeOnly: previous.activeOnly,
                  pageSize: previous.pageSize,
                  page: 1,
                  windowHours: period,
                }))
              } else void query.refetch()
            }}
          >
            {t('common.refresh')}
          </Button>
          {query.isError && <HintIcon type="error" content={query.error.message} />}
        </Space>
        <Typography.Text type="secondary">{t('pipelines.metrics.live')}</Typography.Text>
        <ScrollTable
          aria-label={t('pipelines.metrics.live')}
          size="small"
          rowKey="side"
          pagination={false}
          dataSource={['source', 'target'].map((side) => live(side as 'source' | 'target'))}
          columns={[
            { title: t('pipelines.metrics.side'), dataIndex: 'side', render: roleName },
            { title: t('pipelines.metrics.node'), dataIndex: 'side', render: nodeName },
            {
              title: t('pipelines.metrics.observedAt'),
              render: (_, row) => formatDateTime(row.observation?.observedAt),
            },
            {
              title: t('pipelines.metrics.rows'),
              render: (_, row) =>
                decimal(row.metricsKnown ? row.observation?.values.rowsPerSec : null),
            },
            {
              title: t('pipelines.metrics.txns'),
              render: (_, row) =>
                decimal(row.metricsKnown ? row.observation?.values.txnsPerSec : null),
            },
            {
              title: t('pipelines.metrics.bytes'),
              render: (_, row) =>
                row.metricsKnown
                  ? `${formatBytes(row.observation?.values.bytesPerSec)}/s`
                  : '—',
            },
            {
              title: t('pipelines.metrics.queue'),
              render: (_, row) =>
                formatCount(row.queueKnown ? row.observation?.values.dataQueueBacklog : null),
            },
          ]}
        />
        <Typography.Text type="secondary">{t('pipelines.metrics.history')}</Typography.Text>
        <ScrollTable<MetricHistory>
          aria-label={t('pipelines.metrics.history')}
          size="small"
          rowKey={(row) => `${row.observedAt}-${row.role}-${row.nodeId}-${row.runId}`}
          dataSource={query.data?.history.items ?? []}
          loading={query.isLoading}
          locale={{ emptyText: <HintIcon content={t('pipelines.metrics.empty')} /> }}
          pagination={{
            current: params.page,
            pageSize: params.pageSize,
            total: query.data?.history.total ?? 0,
            showSizeChanger: true,
            pageSizeOptions: [20, 50, 100],
            showTotal: (total) => t('pipelines.metrics.total', { total }),
            onChange: (page, pageSize) =>
              setParams((previous) => ({
                ...previous,
                page,
                pageSize,
                ...(query.data && {
                  windowHours: undefined,
                  from: query.data.from,
                  to: query.data.to,
                }),
              })),
          }}
          columns={[
            {
              title: t('pipelines.metrics.observedAt'),
              dataIndex: 'observedAt',
              render: formatDateTime,
            },
            { title: t('pipelines.metrics.side'), dataIndex: 'role', render: roleName },
            { title: t('pipelines.metrics.node'), dataIndex: 'role', render: nodeName },
            {
              title: t('pipelines.metrics.rows'),
              render: (_, row) => decimal(metricKnown(row) ? row.rowsPerSec : null),
            },
            {
              title: t('pipelines.metrics.txns'),
              render: (_, row) => decimal(metricKnown(row) ? row.txnsPerSec : null),
            },
            {
              title: t('pipelines.metrics.bytes'),
              render: (_, row) =>
                metricKnown(row) ? `${formatBytes(row.bytesPerSec)}/s` : '—',
            },
            {
              title: t('pipelines.metrics.delay'),
              render: (_, row) => (metricKnown(row) ? `${decimal(row.delayMs)} ms` : '—'),
            },
            {
              title: t('pipelines.metrics.queue'),
              render: (_, row) => formatCount(queueKnown(row) ? row.queueDepth : null),
            },
            {
              title: t('pipelines.metrics.errors'),
              render: (_, row) => formatCount(metricKnown(row) ? row.errors : null),
            },
            {
              title: t('pipelines.metrics.conflicts'),
              render: (_, row) => formatCount(metricKnown(row) ? row.conflicts : null),
            },
          ]}
        />
      </Space>
    </Card>
  )
}
