import { ActionDrawer } from '@/components/ActionDrawer'
import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { HintAlert as Alert } from '@/components/HintAlert'
import { useState } from 'react'
import { Button, Select, Space, Tabs, Tag, Typography } from 'antd'
import { PlusOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { Link } from 'react-router-dom'
import { useCurrentUser } from '@/api/auth'
import {
  useAlertRulesQuery,
  useAlertsQuery,
  useDeleteAlertRuleMutation,
  useSaveAlertRuleMutation,
  type AlertEntry,
  type AlertRule,
} from '@/api/alerts'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { RowActions } from '@/components/RowActions'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { AlertRuleFormDrawer } from './AlertRuleFormDrawer'

export default function AlertsPage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const { role } = useCurrentUser()
  const [tab, setTab] = useState('entries')
  const [page, setPage] = useState(1)
  const [rulePage, setRulePage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [rulePageSize, setRulePageSize] = useState(20)
  const [status, setStatus] = useState<AlertEntry['status'] | undefined>()
  const [form, setForm] = useState<{ open: boolean; rule?: AlertRule }>({ open: false })
  const [detail, setDetail] = useState<AlertEntry>()
  const rules = useAlertRulesQuery({ page: rulePage, pageSize: rulePageSize })
  const entries = useAlertsQuery({ page, pageSize, status })
  const save = useSaveAlertRuleMutation()
  const remove = useDeleteAlertRuleMutation()
  const reportError = (error: Error) => void message.error(error.message)
  const ruleColumns: ManagedColumn<AlertRule>[] = [
    { key: 'name', title: t('alerts.ruleName'), dataIndex: 'name', hideable: false },
    {
      key: 'dimension',
      title: t('alerts.dimension'),
      render: (_, r) => t(`alerts.dimensions.${r.dimension}`),
      exportValue: (r) => t(`alerts.dimensions.${r.dimension}`),
    },
    {
      key: 'enabled',
      title: t('alerts.enabled'),
      render: (_, r) => (
        <Tag color={r.enabled ? 'green' : 'default'}>
          {t(r.enabled ? 'alerts.on' : 'alerts.off')}
        </Tag>
      ),
      exportValue: (r) => t(r.enabled ? 'alerts.on' : 'alerts.off'),
    },
    {
      key: 'threshold',
      title: t('alerts.threshold'),
      render: (_, r) =>
        ['queue_backlog', 'latency_exceeded'].includes(r.dimension)
          ? r.threshold.value
          : '—',
      exportValue: (r) =>
        ['queue_backlog', 'latency_exceeded'].includes(r.dimension)
          ? r.threshold.value
          : '—',
    },
    {
      key: 'windowSeconds',
      title: t('alerts.windowSeconds'),
      render: (_, r) => r.threshold.windowSeconds,
      exportValue: (r) => r.threshold.windowSeconds,
    },
    {
      key: 'suppressSeconds',
      title: t('alerts.suppressSeconds'),
      dataIndex: 'suppressSeconds',
    },
    {
      key: 'webhookConfigured',
      title: t('alerts.webhookUrl'),
      render: (_, r) =>
        t(r.webhookConfigured ? 'alerts.configured' : 'alerts.unconfigured'),
      exportValue: (r) =>
        t(r.webhookConfigured ? 'alerts.configured' : 'alerts.unconfigured'),
    },
    {
      key: 'actions',
      title: t('common.actions'),
      hideable: false,
      render: (_, r) => (
        <RowActions
          actions={[
            {
              key: 'edit',
              label: t('common.edit'),
              visible: role === 'admin',
              onClick: () => setForm({ open: true, rule: r }),
            },
            {
              key: 'toggle',
              label: t(r.enabled ? 'alerts.disable' : 'alerts.enable'),
              visible: role === 'admin',
              disabled: save.isPending,
              onClick: () =>
                save.mutate(
                  {
                    id: r.id,
                    version: r.version,
                    input: {
                      name: r.name,
                      dimension: r.dimension,
                      enabled: !r.enabled,
                      threshold: r.threshold,
                      suppressSeconds: r.suppressSeconds,
                    },
                  },
                  { onError: reportError },
                ),
            },
            {
              key: 'delete',
              label: t('common.delete'),
              visible: role === 'admin',
              disabled: remove.isPending,
              danger: true,
              confirm: { title: t('alerts.deleteConfirm') },
              onClick: () =>
                remove.mutate({ id: r.id, version: r.version }, { onError: reportError }),
            },
          ]}
        />
      ),
    },
  ]
  const columns: ManagedColumn<AlertEntry>[] = [
    {
      key: 'dimension',
      title: t('alerts.dimension'),
      hideable: false,
      render: (_, a) => t(`alerts.dimensions.${a.dimension}`),
      exportValue: (a) => t(`alerts.dimensions.${a.dimension}`),
    },
    {
      key: 'status',
      title: t('alerts.status'),
      render: (_, a) => (
        <Tag color={a.status === 'firing' ? 'red' : 'green'}>
          {t(`alerts.states.${a.status}`)}
        </Tag>
      ),
      exportValue: (a) => t(`alerts.states.${a.status}`),
    },
    {
      key: 'pipelineId',
      title: t('alerts.pipeline'),
      render: (_, a) =>
        a.pipelineId ? <Link to={`/pipelines/${a.pipelineId}`}>{a.pipelineId}</Link> : '—',
      exportValue: (a) => a.pipelineId ?? '—',
    },
    {
      key: 'nodeId',
      title: t('alerts.node'),
      render: (_, a) =>
        a.nodeId ? <Link to={`/nodes/${a.nodeId}`}>{a.nodeId}</Link> : '—',
      exportValue: (a) => a.nodeId ?? '—',
    },
    {
      key: 'firedAt',
      title: t('alerts.firedAt'),
      render: (_, a) => formatDateTime(a.firedAt),
      exportValue: (a) => formatDateTime(a.firedAt),
    },
    { key: 'hitCount', title: t('alerts.hitCount'), dataIndex: 'hitCount' },
    {
      key: 'lastSeenAt',
      title: t('alerts.lastSeenAt'),
      render: (_, a) => formatDateTime(a.lastSeenAt ?? a.firedAt),
      exportValue: (a) => formatDateTime(a.lastSeenAt ?? a.firedAt),
    },
    {
      key: 'notifiedAt',
      title: t('alerts.notifiedAt'),
      render: (_, a) => (a.notifiedAt ? formatDateTime(a.notifiedAt) : t('alerts.notSent')),
      exportValue: (a) =>
        a.notifiedAt ? formatDateTime(a.notifiedAt) : t('alerts.notSent'),
    },
    {
      key: 'resolvedAt',
      title: t('alerts.resolvedAt'),
      render: (_, a) => (a.resolvedAt ? formatDateTime(a.resolvedAt) : '—'),
      exportValue: (a) => (a.resolvedAt ? formatDateTime(a.resolvedAt) : '—'),
    },
    {
      key: 'detail',
      title: t('common.actions'),
      hideable: false,
      render: (_, a) => (
        <Button type="link" onClick={() => setDetail(a)}>
          {t('alerts.detail')}
        </Button>
      ),
    },
  ]
  const ruleVisibility = useColumnVisibility('alerts-rules', ruleColumns)
  const visibility = useColumnVisibility('alerts-entries', columns)
  return (
    <Space orientation="vertical" size="large" style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        <span className="section-title">
          {t('nav.alerts')}
          <HintIcon content={t('alerts.pageHelp')} />
        </span>
      </Typography.Title>
      <Tabs
        activeKey={tab}
        onChange={setTab}
        items={[
          { key: 'entries', label: t('alerts.entries') },
          { key: 'rules', label: t('alerts.rules') },
        ]}
      />
      {tab === 'rules' ? (
        <>
          <Space wrap>
            {rules.error && <Alert type="error" title={rules.error.message} />}
            {role === 'admin' && (
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={() => setForm({ open: true })}
              >
                {t('alerts.createRule')}
              </Button>
            )}
            <ColumnSettingsButton {...ruleVisibility.pickerProps} />
            <ExportButton
              columns={ruleVisibility.visibleColumns}
              rows={rules.data?.items ?? []}
              filename="alert-rules"
              scope="page"
            />
          </Space>
          <ScrollTable
            rowKey="id"
            columns={ruleVisibility.visibleColumns}
            dataSource={rules.data?.items ?? []}
            loading={rules.isPending}
            pagination={{
              current: rulePage,
              pageSize: rulePageSize,
              total: rules.data?.total ?? 0,
              showQuickJumper: true,
              showSizeChanger: true,
              pageSizeOptions: [10, 20, 50, 100],
              onChange: (p, n) => {
                setRulePage(p)
                setRulePageSize(n)
              },
            }}
            scroll={{ x: 'max-content' }}
          />
        </>
      ) : (
        <>
          <Space wrap>
            {entries.error && <Alert type="error" title={entries.error.message} />}
            <Select
              aria-label={t('alerts.status')}
              allowClear
              placeholder={t('alerts.status')}
              style={{ width: 160 }}
              value={status}
              onChange={(value: AlertEntry['status'] | undefined) => {
                setStatus(value)
                setPage(1)
              }}
              options={(['firing', 'resolved'] as const).map((value) => ({
                value,
                label: t(`alerts.states.${value}`),
              }))}
            />
            <ColumnSettingsButton {...visibility.pickerProps} />
            <ExportButton
              columns={visibility.visibleColumns}
              rows={entries.data?.items ?? []}
              filename="alerts"
              scope="page"
            />
          </Space>
          <ScrollTable
            rowKey="id"
            columns={visibility.visibleColumns}
            dataSource={entries.data?.items ?? []}
            loading={entries.isPending}
            pagination={{
              current: page,
              pageSize,
              total: entries.data?.total ?? 0,
              showQuickJumper: true,
              showSizeChanger: true,
              pageSizeOptions: [10, 20, 50, 100],
              onChange: (p, n) => {
                setPage(p)
                setPageSize(n)
              },
            }}
            scroll={{ x: 'max-content' }}
          />
        </>
      )}
      <AlertRuleFormDrawer
        open={form.open}
        rule={form.rule}
        onClose={() => setForm({ open: false })}
      />
      <ActionDrawer
        open={Boolean(detail)}
        title={t('alerts.detail')}
        footer={null}
        onClose={() => setDetail(undefined)}
      >
        <pre style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>
          {JSON.stringify(detail?.detail, null, 2)}
        </pre>
      </ActionDrawer>
    </Space>
  )
}
