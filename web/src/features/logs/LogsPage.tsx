import { ScrollTable } from '@/components/ScrollTable'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { DatePicker, Input, Select, Space, Tag } from 'antd'
import { useTranslation } from 'react-i18next'
import dayjs, { type Dayjs } from 'dayjs'
import {
  DEFAULT_LOG_WINDOW_HOURS,
  useLogsQuery,
  type LogEntry,
  type LogLevel,
  type LogQuery,
} from '@/api/logs'
import { useNodesQuery } from '@/api/nodes'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime, formatWindowDays } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'

const LOG_LEVELS: LogLevel[] = ['debug', 'info', 'warn', 'error']
const LEVEL_COLOR: Record<LogLevel, string> = {
  debug: 'default',
  info: 'processing',
  warn: 'warning',
  error: 'error',
}
const DEFAULT_PAGE_SIZE = 20
const PAGE_SIZE_OPTIONS = ['10', '20', '50', '100']

const { RangePicker } = DatePicker

export default function LogsPage() {
  const { t } = useTranslation()
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE)
  const [nodeId, setNodeId] = useState<string | undefined>()
  const [level, setLevel] = useState<LogLevel | undefined>()
  const [keyword, setKeyword] = useState('')
  // null 表示用户未显式选择时间范围，交给后端按契约补默认窗口（最近 24
  // 小时）；一旦选择，since/until 成对提交，不支持只给一边——后端虽然支持
  // 只给一边补另一边，但 RangePicker 天然是成对选择的控件，这里不为了那个
  // 边界场景多做一套「只选一边」的交互。
  const [range, setRange] = useState<[Dayjs, Dayjs] | null>(null)

  const query: LogQuery = {
    page,
    pageSize,
    nodeId,
    level,
    keyword: keyword || undefined,
    since: range ? range[0].toISOString() : undefined,
    until: range ? range[1].toISOString() : undefined,
  }
  const logsQuery = useLogsQuery(query)
  // 200 是当前 maxListPageSize（见 internal/manager/http/pagination.go），
  // 足够把节点全量拉进筛选下拉；节点规模一旦远超这个量级，这里需要换成
  // 远程搜索式 Select，现阶段用一次性拉全量足够。
  const nodesQuery = useNodesQuery({ page: 1, pageSize: 200 })
  const nodeNameById = new Map((nodesQuery.data?.items ?? []).map((n) => [n.id, n.name]))

  const handlePageChange = (nextPage: number, nextPageSize: number) => {
    setPage(nextPage)
    setPageSize(nextPageSize)
  }

  // `maxWindowSeconds` 只有在拿到过至少一次响应之后才知道（见组件顶部
  // 注释）；`info.from` 是 RangePicker 正在选第二个日期时，用户已经选定的
  // 第一个日期，只有这时才需要做跨度限制——选第一个日期时不应该被这个上限
  // 挡住任何选项，是选完第一个之后才知道第二个能往前/往后选多远。
  const maxWindowSeconds = logsQuery.data?.maxWindowSeconds
  const disabledDate = (current: Dayjs, info: { from?: Dayjs }) => {
    if (current.isAfter(dayjs().endOf('minute'))) {
      return true
    }
    if (maxWindowSeconds !== undefined && info.from) {
      if (Math.abs(current.diff(info.from, 'second')) > maxWindowSeconds) {
        return true
      }
    }
    return false
  }

  const columns: ManagedColumn<LogEntry>[] = [
    {
      key: 'ts',
      title: t('logs.columns.ts'),
      dataIndex: 'ts',
      width: 200,
      hideable: false,
      render: (value: string) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.ts),
    },
    {
      key: 'level',
      title: t('logs.columns.level'),
      dataIndex: 'level',
      width: 100,
      render: (value: LogLevel) => <Tag color={LEVEL_COLOR[value]}>{value}</Tag>,
    },
    { key: 'module', title: t('logs.columns.module'), dataIndex: 'module', width: 140 },
    {
      key: 'node',
      title: t('logs.columns.node'),
      dataIndex: 'nodeId',
      width: 160,
      render: (value: string | null) => (value ? (nodeNameById.get(value) ?? value) : '—'),
      exportValue: (record) =>
        record.nodeId ? (nodeNameById.get(record.nodeId) ?? record.nodeId) : '—',
    },
    { key: 'message', title: t('logs.columns.message'), dataIndex: 'message' },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('logs', columns)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      <Space
        style={{
          marginBottom: 16,
          width: '100%',
          justifyContent: 'space-between',
          flexShrink: 0,
        }}
        wrap
      >
        <Space wrap>
          <Select
            allowClear
            placeholder={t('logs.filters.node')}
            style={{ width: 200 }}
            value={nodeId}
            onChange={(value: string | undefined) => {
              setNodeId(value)
              setPage(1)
            }}
            loading={nodesQuery.isLoading}
            options={(nodesQuery.data?.items ?? []).map((n) => ({
              value: n.id,
              label: n.name,
            }))}
          />
          <Select
            allowClear
            placeholder={t('logs.filters.level')}
            style={{ width: 140 }}
            value={level}
            onChange={(value: LogLevel | undefined) => {
              setLevel(value)
              setPage(1)
            }}
            options={LOG_LEVELS.map((item) => ({ value: item, label: item }))}
          />
          <Input.Search
            allowClear
            placeholder={t('logs.filters.keyword')}
            style={{ width: 260 }}
            onSearch={(value) => {
              setKeyword(value)
              setPage(1)
            }}
          />
          <RangePicker
            allowClear
            showTime
            placeholder={[t('logs.filters.since'), t('logs.filters.until')]}
            value={range}
            onChange={(value) => {
              setRange(value && value[0] && value[1] ? [value[0], value[1]] : null)
              setPage(1)
            }}
            disabledDate={disabledDate}
          />
        </Space>
        <Space wrap>
          {logsQuery.isError && (
            <HintIcon type="error" content={<> {logsQuery.error.message} </>} />
          )}
          <ExportButton
            filename="logs"
            columns={visibleColumns}
            rows={logsQuery.data?.items ?? []}
            scope="page"
          />
          <ColumnSettingsButton {...pickerProps} />
          <HintIcon
            content={
              <>
                {t('logs.filters.rangeHint', { hours: DEFAULT_LOG_WINDOW_HOURS })}
                {maxWindowSeconds !== undefined &&
                  ` ${t('logs.filters.maxWindowHint', { days: formatWindowDays(maxWindowSeconds) })}`}
              </>
            }
          />

          {}
          <Alert
            type="info"
            showIcon
            title={
              logsQuery.data
                ? `${t('logs.effectiveRange')}${formatDateTime(logsQuery.data.since)} ~ ${formatDateTime(logsQuery.data.until)}`
                : t('logs.effectiveRangeLoading')
            }
            description={range === null ? t('logs.effectiveRangeDefaultHint') : undefined}
          />
        </Space>
      </Space>

      <div
        className="table-fill-container"
        style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
      >
        <ScrollTable<LogEntry>
          rowKey="id"
          loading={logsQuery.isLoading}
          dataSource={logsQuery.data?.items ?? []}
          locale={{ emptyText: t('logs.empty') }}
          pagination={{
            current: page,
            pageSize,
            total: logsQuery.data?.total ?? 0,
            showSizeChanger: true,
            showQuickJumper: true,
            pageSizeOptions: PAGE_SIZE_OPTIONS,
            onChange: handlePageChange,
          }}
          columns={visibleColumns}
        />
      </div>
    </div>
  )
}
