import { ScrollTable } from '@/components/ScrollTable'
import { HintIcon } from '@/components/HintIcon'
import { HintAlert as Alert } from '@/components/HintAlert'
import { useState } from 'react'
import { Button, Card, Col, Input, Progress, Row, Space, Statistic, Typography } from 'antd'
import { Link } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import {
  useDashboardQuery,
  type DashboardError,
  type DashboardLag,
  type DashboardSnapshot,
} from '@/api/dashboard'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { eventTypeLabel } from '@/features/logs/eventLabels'

function OverviewTable<T extends object>({
  name,
  rows,
  columns,
  loading,
  rowKey,
  width,
}: {
  name: string
  rows: T[]
  columns: ManagedColumn<T>[]
  loading: boolean
  rowKey: string
  width: number
}) {
  const { t } = useTranslation()
  const [keyword, setKeyword] = useState('')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const filtered = rows.filter((row) =>
    JSON.stringify(row).toLowerCase().includes(keyword.trim().toLowerCase()),
  )
  const visibility = useColumnVisibility(name, columns)
  return (
    <Space orientation="vertical" style={{ width: '100%' }}>
      <Space wrap>
        <Input
          aria-label={t('dashboard.searchLoaded')}
          placeholder={t('dashboard.searchLoaded')}
          allowClear
          value={keyword}
          onChange={(event) => {
            setKeyword(event.target.value)
            setPage(1)
          }}
        />
        <ColumnSettingsButton {...visibility.pickerProps} />
        <ExportButton
          columns={visibility.visibleColumns}
          rows={filtered.slice((page - 1) * pageSize, page * pageSize)}
          filename={name}
          scope="page"
        />
      </Space>
      <ScrollTable<T>
        size="small"
        rowKey={rowKey}
        loading={loading}
        pagination={{
          current: page,
          pageSize,
          total: filtered.length,
          showQuickJumper: true,
          showSizeChanger: true,
          pageSizeOptions: [10, 20],
          onChange: (p, n) => {
            setPage(p)
            setPageSize(n)
          },
        }}
        scroll={{ x: width }}
        dataSource={filtered}
        columns={visibility.visibleColumns}
      />
    </Space>
  )
}

export default function DashboardPage() {
  const { t } = useTranslation()
  const query = useDashboardQuery()
  const data = query.data
  const pipeline = (name: string, id: string) => <Link to={`/pipelines/${id}`}>{name}</Link>
  return (
    <Space orientation="vertical" size="large" style={{ width: '100%' }}>
      <Space wrap style={{ justifyContent: 'space-between', width: '100%' }}>
        <Typography.Title level={3} style={{ margin: 0 }}>
          <span className="section-title">
            {t('nav.dashboard')}
            <HintIcon
              content={
                data
                  ? t('dashboard.asOf', { time: new Date(data.asOf).toLocaleString() })
                  : t('dashboard.loading')
              }
            />
          </span>
        </Typography.Title>
        <Space>
          {query.error && <Alert type="error" title={query.error.message} />}
          <Button
            aria-label={t('dashboard.refresh')}
            loading={query.isFetching}
            onClick={() => void query.refetch()}
          >
            {t('dashboard.refresh')}
          </Button>
        </Space>
      </Space>
      <Row gutter={[16, 16]}>
        {(
          ['onlineNodes', 'runningPipelines', 'errorEvents24h', 'firingAlerts'] as const
        ).map((key) => (
          <Col xs={24} sm={12} xl={6} key={key}>
            <Card loading={query.isLoading}>
              <Statistic title={t(`dashboard.${key}`)} value={data?.[key] ?? '—'} />
            </Card>
          </Col>
        ))}
      </Row>
      <Card
        title={
          <span className="section-title">
            {t('dashboard.lagTop')}
            <HintIcon
              content={t('dashboard.lagHelp', {
                seconds: data?.metricFreshnessSeconds ?? 300,
                count: data?.unknownLagPipelines ?? '—',
              })}
            />
          </span>
        }
        extra={<Link to="/pipelines">{t('dashboard.allPipelines')}</Link>}
      >
        <OverviewTable<DashboardLag>
          name="dashboard-lag"
          rowKey="pipelineId"
          loading={query.isLoading}
          width={480}
          rows={data?.lagTop ?? []}
          columns={[
            {
              key: 'name',
              hideable: false,
              title: t('dashboard.pipeline'),
              dataIndex: 'name',
              render: (name: string, row) => pipeline(name, row.pipelineId),
            },
            {
              key: 'lagMs',
              title: t('dashboard.delay'),
              dataIndex: 'lagMs',
              render: (value: number) => `${value.toLocaleString()} ms`,
            },
            {
              key: 'observedAt',
              title: t('dashboard.observedAt'),
              dataIndex: 'observedAt',
              render: (value: string) => new Date(value).toLocaleString(),
            },
          ]}
        />
      </Card>
      <Card
        title={t('dashboard.recentErrors')}
        extra={<Link to="/events">{t('dashboard.allEvents')}</Link>}
      >
        <OverviewTable<DashboardError>
          name="dashboard-errors"
          rowKey="id"
          loading={query.isLoading}
          width={480}
          rows={data?.recentErrors ?? []}
          columns={[
            {
              key: 'ts',
              hideable: false,
              title: t('dashboard.observedAt'),
              dataIndex: 'ts',
              render: (value: string) => new Date(value).toLocaleString(),
            },
            {
              key: 'type',
              title: t('dashboard.eventType'),
              dataIndex: 'type',
              render: (value: string) => eventTypeLabel(value, t),
            },
            {
              key: 'pipelineId',
              title: t('dashboard.pipeline'),
              dataIndex: 'pipelineId',
              render: (id: string | null) =>
                id ? pipeline(id, id) : t('dashboard.unknown'),
            },
          ]}
        />
      </Card>
      <Card
        title={t('dashboard.snapshots', { count: data?.totalSnapshots ?? '—' })}
        extra={<Link to="/pipelines">{t('dashboard.allPipelines')}</Link>}
      >
        <OverviewTable<DashboardSnapshot>
          name="dashboard-snapshots"
          rowKey="pipelineId"
          loading={query.isLoading}
          width={640}
          rows={data?.snapshots ?? []}
          columns={[
            {
              key: 'name',
              hideable: false,
              title: t('dashboard.pipeline'),
              dataIndex: 'name',
              render: (name: string, row) => pipeline(name, row.pipelineId),
            },
            {
              key: 'status',
              title: t('dashboard.status'),
              dataIndex: 'status',
              render: (status: DashboardSnapshot['status']) =>
                t(`pipelines.snapshot.states.${status}`),
            },
            {
              key: 'chunks',
              title: t('dashboard.chunks'),
              exportValue: (row) =>
                row.totalChunks > 0
                  ? `${row.doneChunks}/${row.totalChunks}`
                  : t('dashboard.unknown'),
              render: (_, row) =>
                row.totalChunks > 0 ? (
                  <Space orientation="vertical" size={0} style={{ minWidth: 120 }}>
                    <Typography.Text>
                      {row.doneChunks.toLocaleString()} / {row.totalChunks.toLocaleString()}
                    </Typography.Text>
                    <Progress
                      percent={Math.min(
                        100,
                        Math.floor((row.doneChunks / row.totalChunks) * 100),
                      )}
                      size="small"
                      status={row.status === 'failed' ? 'exception' : 'normal'}
                    />
                  </Space>
                ) : (
                  t('dashboard.unknown')
                ),
            },
            {
              key: 'catchupRemainingTxns',
              title: t('dashboard.catchup'),
              dataIndex: 'catchupRemainingTxns',
              render: (value: number | null) =>
                value === null ? t('dashboard.unknown') : value.toLocaleString(),
            },
            {
              key: 'updatedAt',
              title: t('dashboard.observedAt'),
              dataIndex: 'updatedAt',
              render: (value: string) => new Date(value).toLocaleString(),
            },
          ]}
        />
      </Card>
    </Space>
  )
}
