import { ActionDrawer } from '@/components/ActionDrawer'
import { ScrollTable } from '@/components/ScrollTable'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { useState } from 'react'
import { Button, DatePicker, Form, Input, Space, Tag, Typography } from 'antd'
import dayjs from 'dayjs'
import { useTranslation } from 'react-i18next'
import { useCurrentUser } from '@/api/auth'
import { useAuditLogsQuery, type AuditLogEntry, type AuditQuery } from '@/api/audit'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'

type AuditFilter = Omit<AuditQuery, 'page' | 'pageSize'>
interface AuditFilterForm extends Omit<AuditFilter, 'since' | 'until'> {
  range?: [dayjs.Dayjs, dayjs.Dayjs]
}

export default function AuditPage() {
  const { t } = useTranslation()
  const { role } = useCurrentUser()
  const [form] = Form.useForm<AuditFilterForm>()
  const [filter, setFilter] = useState<AuditFilter>({})
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [detail, setDetail] = useState<AuditLogEntry>()
  const query = useAuditLogsQuery({ ...filter, page, pageSize })
  const outcome = (value: boolean | null) =>
    t(value === null ? 'audit.unknown' : value ? 'audit.succeeded' : 'audit.failed')
  const columns: ManagedColumn<AuditLogEntry>[] = [
    {
      key: 'ts',
      title: t('audit.ts'),
      hideable: false,
      render: (_, a) => formatDateTime(a.ts),
      exportValue: (a) => formatDateTime(a.ts),
    },
    {
      key: 'actor',
      title: t('audit.actor'),
      render: (_, a) => a.actor || t('audit.unknown'),
      exportValue: (a) => a.actor || t('audit.unknown'),
    },
    {
      key: 'userId',
      title: t('audit.userId'),
      render: (_, a) => a.userId ?? '—',
      exportValue: (a) => a.userId ?? '—',
    },
    { key: 'action', title: t('audit.action'), dataIndex: 'action' },
    { key: 'targetType', title: t('audit.targetType'), dataIndex: 'targetType' },
    {
      key: 'targetId',
      title: t('audit.targetId'),
      render: (_, a) => a.targetId ?? '—',
      exportValue: (a) => a.targetId ?? '—',
    },
    {
      key: 'success',
      title: t('audit.result'),
      render: (_, a) => (
        <Tag color={a.success === null ? 'default' : a.success ? 'green' : 'red'}>
          {outcome(a.success)}
        </Tag>
      ),
      exportValue: (a) => outcome(a.success),
    },
    {
      key: 'detail',
      title: t('common.actions'),
      hideable: false,
      render: (_, a) => (
        <Button type="link" onClick={() => setDetail(a)}>
          {t('audit.detail')}
        </Button>
      ),
    },
  ]
  const visibility = useColumnVisibility('audit-logs', columns)
  if (role !== 'admin')
    return <Alert type="warning" showIcon title={t('audit.adminOnly')} />
  const search = (values: AuditFilterForm) => {
    const next: AuditFilter = {}
    for (const key of ['actor', 'userId', 'action', 'targetType', 'targetId'] as const) {
      const value = values[key]?.trim()
      if (value) next[key] = value
    }
    if (values.range?.[0] && values.range[1]) {
      next.since = values.range[0].toISOString()
      next.until = values.range[1].toISOString()
    }
    setFilter(next)
    setPage(1)
  }
  return (
    <Space orientation="vertical" size="large" style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        <span className="section-title">
          {t('nav.audit')}
          <Alert type="info" title={t('audit.help')} />
        </span>
      </Typography.Title>
      <Form form={form} layout="inline" onFinish={search} style={{ gap: 8 }}>
        {(['actor', 'userId', 'action', 'targetType', 'targetId'] as const).map((key) => (
          <HintForm key={key} name={key}>
            <Input
              aria-label={t(`audit.${key}`)}
              placeholder={t(`audit.${key}`)}
              allowClear
              style={{ width: key.endsWith('Id') ? 240 : 180 }}
            />
          </HintForm>
        ))}
        <HintForm name="range">
          <DatePicker.RangePicker aria-label={t('audit.timeRange')} showTime />
        </HintForm>
        <HintForm>
          <Space>
            <Button type="primary" htmlType="submit">
              {t('audit.search')}
            </Button>
            <Button
              onClick={() => {
                form.resetFields()
                setFilter({})
                setPage(1)
              }}
            >
              {t('audit.reset')}
            </Button>
          </Space>
        </HintForm>
      </Form>
      <Space wrap>
        {query.error && <Alert type="error" title={query.error.message} />}
        <Button loading={query.isFetching} onClick={() => void query.refetch()}>
          {t('audit.refresh')}
        </Button>
        <ColumnSettingsButton {...visibility.pickerProps} />
        <ExportButton
          columns={visibility.visibleColumns}
          rows={query.data?.items ?? []}
          filename="audit-logs"
          scope="page"
        />
      </Space>
      <ScrollTable
        rowKey="id"
        columns={visibility.visibleColumns}
        dataSource={query.data?.items ?? []}
        loading={query.isPending}
        scroll={{ x: 'max-content' }}
        pagination={{
          current: page,
          pageSize,
          total: query.data?.total ?? 0,
          showQuickJumper: true,
          showSizeChanger: true,
          pageSizeOptions: [10, 20, 50, 100, 200],
          onChange: (p, n) => {
            setPage(p)
            setPageSize(n)
          },
        }}
      />
      <ActionDrawer
        open={Boolean(detail)}
        title={t('audit.detail')}
        onClose={() => setDetail(undefined)}
        footer={null}
      >
        <pre style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>
          {JSON.stringify(detail?.detail, null, 2)}
        </pre>
      </ActionDrawer>
    </Space>
  )
}
