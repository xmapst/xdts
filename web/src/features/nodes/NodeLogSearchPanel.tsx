import { ScrollTable } from '@/components/ScrollTable'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { Input, Select, Space, Tag, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import { useLogsQuery, type LogEntry, type LogLevel } from '@/api/logs'
import { formatDateTime } from '@/utils/format'

const LOG_LEVELS: LogLevel[] = ['debug', 'info', 'warn', 'error']
const LEVEL_COLOR: Record<LogLevel, string> = {
  debug: 'default',
  info: 'processing',
  warn: 'warning',
  error: 'error',
}
const PAGE_SIZE = 20

/* Show the effective query window from the shared logs response. */
export function NodeLogSearchPanel({ nodeId }: { nodeId: string }) {
  const { t } = useTranslation()
  const [page, setPage] = useState(1)
  const [level, setLevel] = useState<LogLevel | undefined>()
  const [keyword, setKeyword] = useState('')

  const logsQuery = useLogsQuery({
    page,
    pageSize: PAGE_SIZE,
    nodeId,
    level,
    keyword: keyword || undefined,
  })

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Select
          allowClear
          placeholder={t('nodes.detail.logSearchLevel')}
          style={{ width: 140 }}
          value={level}
          onChange={(value) => {
            setLevel(value)
            setPage(1)
          }}
          options={LOG_LEVELS.map((item) => ({ value: item, label: item }))}
        />
        <Input.Search
          allowClear
          placeholder={t('nodes.detail.logSearchKeyword')}
          style={{ width: 280 }}
          onSearch={(value) => {
            setKeyword(value)
            setPage(1)
          }}
        />
        {logsQuery.isError && <HintIcon type="error" content={logsQuery.error.message} />}
      </Space>

      {/* 硬性要求：生效的 since/until 必须常驻可见，不能只靠 tooltip——理由
          见组件顶部注释，和 LogsPage.tsx 是同一条原则，这里只是呈现更轻。 */}
      <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
        {logsQuery.data
          ? t('nodes.detail.logSearchEffectiveRange', {
              since: formatDateTime(logsQuery.data.since),
              until: formatDateTime(logsQuery.data.until),
            })
          : t('nodes.detail.logSearchEffectiveRangeLoading')}
      </Typography.Text>

      <ScrollTable<LogEntry>
        rowKey="id"
        size="small"
        loading={logsQuery.isLoading}
        dataSource={logsQuery.data?.items ?? []}
        locale={{ emptyText: t('nodes.detail.logSearchNoResults') }}
        pagination={{
          current: page,
          pageSize: PAGE_SIZE,
          total: logsQuery.data?.total ?? 0,
          onChange: setPage,
        }}
        columns={[
          {
            title: 'ts',
            dataIndex: 'ts',
            width: 200,
            render: (value: string) => formatDateTime(value),
          },
          {
            title: t('nodes.detail.logSearchLevel'),
            dataIndex: 'level',
            width: 100,
            render: (value: LogLevel) => <Tag color={LEVEL_COLOR[value]}>{value}</Tag>,
          },
          { title: 'module', dataIndex: 'module', width: 140 },
          { title: 'message', dataIndex: 'message' },
        ]}
      />
    </div>
  )
}
