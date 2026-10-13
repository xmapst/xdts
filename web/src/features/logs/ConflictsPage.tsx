import { ScrollTable } from '@/components/ScrollTable'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { DatePicker, Input, Popover, Select, Space, Tag, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import type { Dayjs } from 'dayjs'
import {
  useConflictsQuery,
  type ConflictEntry,
  type ConflictQuery,
  type ConflictResolution,
  type ConflictType,
} from '@/api/conflicts'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { enumLabel } from '@/utils/i18nEnum'

const CONFLICT_TYPES: ConflictType[] = [
  'insert_duplicate',
  'update_missing',
  'delete_missing',
  'update_mismatch',
  'delete_mismatch',
  'lww_rejected',
]
const RESOLUTIONS: ConflictResolution[] = [
  'source_wins',
  'target_wins',
  'skipped',
  'stopped',
]
const DEFAULT_PAGE_SIZE = 20
const PAGE_SIZE_OPTIONS = ['10', '20', '50', '100']

const { RangePicker } = DatePicker

/* 因此这个页面不展示那条"本次查询范围"常驻提示——那是 `/logs`、
`/events` 的硬性要求（响应回传了被悄悄补上的范围，必须让用户看到），
这个端点根本不做"悄悄补范围"这件事，伪造一条提示反而是在编造后端没给
过的信息。时间筛选在这里是纯粹可选的普通过滤条件，不选就是看全部
（受分页限制）。 */
export default function ConflictsPage() {
  const { t } = useTranslation()
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE)
  const [pipelineId, setPipelineId] = useState('')
  const [conflictType, setConflictType] = useState<ConflictType | undefined>()
  const [resolution, setResolution] = useState<ConflictResolution | undefined>()
  const [range, setRange] = useState<[Dayjs, Dayjs] | null>(null)

  const query: ConflictQuery = {
    page,
    pageSize,
    pipelineId: pipelineId || undefined,
    conflictType,
    resolution,
    since: range ? range[0].toISOString() : undefined,
    until: range ? range[1].toISOString() : undefined,
  }
  const conflictsQuery = useConflictsQuery(query)

  const handlePageChange = (nextPage: number, nextPageSize: number) => {
    setPage(nextPage)
    setPageSize(nextPageSize)
  }

  const columns: ManagedColumn<ConflictEntry>[] = [
    {
      key: 'ts',
      title: t('conflicts.columns.ts'),
      dataIndex: 'ts',
      width: 200,
      hideable: false,
      render: (value: string) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.ts),
    },
    {
      key: 'pipeline',
      title: t('conflicts.columns.pipeline'),
      dataIndex: 'pipelineId',
      width: 140,
    },
    { key: 'table', title: t('conflicts.columns.table'), dataIndex: 'table', width: 160 },
    {
      key: 'conflictType',
      title: t('conflicts.columns.conflictType'),
      dataIndex: 'conflictType',
      width: 160,
      // conflictType 来自后端 conflicts 表，后端可新增取值；render 与
      // exportValue 两路都要兜，漏掉 exportValue 的话内部 key 会进用户的 CSV。
      render: (value: ConflictType) => enumLabel(t, 'conflicts.conflictTypes', value),
      exportValue: (record) => enumLabel(t, 'conflicts.conflictTypes', record.conflictType),
    },
    {
      key: 'resolution',
      title: t('conflicts.columns.resolution'),
      dataIndex: 'resolution',
      width: 160,
      render: (value: ConflictResolution) => (
        <Tag
          color={
            value === 'target_wins' || value === 'skipped' || value === 'stopped'
              ? 'warning'
              : 'default'
          }
        >
          {enumLabel(t, 'conflicts.resolutions', value)}
        </Tag>
      ),
      // resolution 同样来自后端。
      exportValue: (record) => enumLabel(t, 'conflicts.resolutions', record.resolution),
    },
    {
      key: 'detail',
      title: t('conflicts.columns.pk'),
      render: (_: unknown, record: ConflictEntry) => (
        <Popover
          content={
            <pre style={{ margin: 0 }}>
              {JSON.stringify(
                { pk: record.pk, sourceRow: record.sourceRow, targetRow: record.targetRow },
                null,
                2,
              )}
            </pre>
          }
        >
          <Typography.Link>{JSON.stringify(record.pk)}</Typography.Link>
        </Popover>
      ),
      exportValue: (record) =>
        JSON.stringify({
          pk: record.pk,
          sourceRow: record.sourceRow,
          targetRow: record.targetRow,
        }),
    },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('conflicts', columns)

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
          <Input
            allowClear
            placeholder={t('conflicts.filters.pipeline')}
            style={{ width: 200 }}
            value={pipelineId}
            onChange={(e) => {
              setPipelineId(e.target.value)
              setPage(1)
            }}
          />
          <Select
            allowClear
            placeholder={t('conflicts.filters.conflictType')}
            style={{ width: 200 }}
            value={conflictType}
            onChange={(value: ConflictType | undefined) => {
              setConflictType(value)
              setPage(1)
            }}
            options={CONFLICT_TYPES.map((item) => ({
              value: item,
              label: t(`conflicts.conflictTypes.${item}`),
            }))}
          />
          <Select
            allowClear
            placeholder={t('conflicts.filters.resolution')}
            style={{ width: 180 }}
            value={resolution}
            onChange={(value: ConflictResolution | undefined) => {
              setResolution(value)
              setPage(1)
            }}
            options={RESOLUTIONS.map((item) => ({
              value: item,
              label: t(`conflicts.resolutions.${item}`),
            }))}
          />
          <RangePicker
            allowClear
            showTime
            placeholder={[t('conflicts.filters.since'), t('conflicts.filters.until')]}
            value={range}
            onChange={(value) => {
              setRange(value && value[0] && value[1] ? [value[0], value[1]] : null)
              setPage(1)
            }}
          />
        </Space>
        <Space wrap>
          {conflictsQuery.isError && (
            <HintIcon type="error" content={<> {conflictsQuery.error.message} </>} />
          )}
          <HintIcon content={t('conflicts.filtersHint')} />
          <ExportButton
            filename="conflicts"
            columns={visibleColumns}
            rows={conflictsQuery.data?.items ?? []}
            scope="page"
          />
          <ColumnSettingsButton {...pickerProps} />
        </Space>
      </Space>

      <div
        className="table-fill-container"
        style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
      >
        <ScrollTable<ConflictEntry>
          rowKey="id"
          loading={conflictsQuery.isLoading}
          dataSource={conflictsQuery.data?.items ?? []}
          locale={{ emptyText: t('conflicts.empty') }}
          pagination={{
            current: page,
            pageSize,
            total: conflictsQuery.data?.total ?? 0,
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
