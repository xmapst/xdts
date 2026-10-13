import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import {
  Button,
  Checkbox,
  DatePicker,
  Input,
  Popconfirm,
  Popover,
  Select,
  Space,
  Tag,
  Typography,
} from 'antd'
import { useTranslation } from 'react-i18next'
import { ApiError } from '@/api/client'
import { useCurrentUser } from '@/api/auth'
import dayjs, { type Dayjs } from 'dayjs'
// EVENT_TYPES **直接来自后端金样**（web/src/api/events.ts 从
// internal/manager/model/event_types.golden.json 取），不在本文件再抄一份：
// 这个下拉框漏掉一个后端新增的类型时，按类型筛选会让那些事件整条消失，
// 而界面不会作任何说明——比"类型那一列显示得难看"严重一档。
import {
  EVENT_TYPES,
  useAckEventMutation,
  useEventsQuery,
  type EventEntry,
  type EventLevel,
  type EventQuery,
  type EventType,
} from '@/api/events'
import { useNodesQuery } from '@/api/nodes'
import { ColumnSettingsButton } from '@/components/ColumnSettingsButton'
import { ExportButton } from '@/components/ExportButton'
import { formatDateTime } from '@/utils/format'
import type { ManagedColumn } from '@/utils/tableColumns'
import { ackErrorMessageKey, ackState, ackStateText, eventTypeLabel } from './eventLabels'
import { useColumnVisibility } from '@/utils/useColumnVisibility'
import { enumLabel } from '@/utils/i18nEnum'

const EVENT_LEVELS: EventLevel[] = ['info', 'warning', 'error']
const LEVEL_COLOR: Record<EventLevel, string> = {
  info: 'processing',
  warning: 'warning',
  error: 'error',
}
const DEFAULT_PAGE_SIZE = 20
const PAGE_SIZE_OPTIONS = ['10', '20', '50', '100']

const { RangePicker } = DatePicker

export default function EventsPage() {
  const { t } = useTranslation()
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE)
  const [nodeId, setNodeId] = useState<string | undefined>()
  const [pipelineId, setPipelineId] = useState('')
  const [type, setType] = useState<EventType | undefined>()
  const [level, setLevel] = useState<EventLevel | undefined>()
  const [onlyUnacked, setOnlyUnacked] = useState(false)
  const [range, setRange] = useState<[Dayjs, Dayjs] | null>(null)

  const query: EventQuery = {
    page,
    pageSize,
    nodeId,
    pipelineId: pipelineId || undefined,
    type,
    level,
    onlyUnacked: onlyUnacked || undefined,
    since: range ? range[0].toISOString() : undefined,
    until: range ? range[1].toISOString() : undefined,
  }
  const eventsQuery = useEventsQuery(query)
  const nodesQuery = useNodesQuery({ page: 1, pageSize: 200 })
  const nodeNameById = new Map((nodesQuery.data?.items ?? []).map((n) => [n.id, n.name]))

  const handlePageChange = (nextPage: number, nextPageSize: number) => {
    setPage(nextPage)
    setPageSize(nextPageSize)
  }

  const { message } = useHintFeedback()
  const ackMutation = useAckEventMutation()
  // **只读角色不给入口**：`/api/v1` 组的中间件顺序是 CSRF → 会话鉴权 → 只读闸门，
  // 只读角色发出的确认请求会被闸门挡下。让它点了才报 403，等于把一个**本来就
  // 不该出现的按钮**的后果推给后端去说明。
  const { role } = useCurrentUser()
  const canAck = role === 'admin'

  /* The server acknowledges idempotently for the same user; 409 means another user acknowledged it. Refresh after every result. */
  const handleAck = async (id: string) => {
    try {
      await ackMutation.mutateAsync(id)
      message.success(t('events.ackSuccess'))
    } catch (err) {
      const { key, level } = ackErrorMessageKey(
        err instanceof ApiError ? err.code : undefined,
      )
      message[level](t(key))
    }
  }

  const columns: ManagedColumn<EventEntry>[] = [
    {
      key: 'ts',
      title: t('events.columns.ts'),
      dataIndex: 'ts',
      width: 200,
      hideable: false,
      render: (value: string) => formatDateTime(value),
      exportValue: (record) => formatDateTime(record.ts),
    },
    {
      key: 'type',
      title: t('events.columns.type'),
      dataIndex: 'type',
      width: 160,
      // 走 eventTypeLabel：后端新增的事件类型在这里会显示为后端原值，而不是
      // 一串 `events.types.xxx` 的内部 key（见该函数文档）。
      render: (value: EventType) => eventTypeLabel(value, t),
      exportValue: (record) => eventTypeLabel(record.type, t),
    },
    {
      key: 'level',
      title: t('events.columns.level'),
      dataIndex: 'level',
      width: 100,
      // level 来自后端 events 表。
      render: (value: EventLevel) => (
        <Tag color={LEVEL_COLOR[value]}>{enumLabel(t, 'events.levels', value)}</Tag>
      ),
      exportValue: (record) => enumLabel(t, 'events.levels', record.level),
    },
    {
      key: 'node',
      title: t('events.columns.node'),
      dataIndex: 'nodeId',
      width: 160,
      render: (value: string | null) => (value ? (nodeNameById.get(value) ?? value) : '—'),
      exportValue: (record) =>
        record.nodeId ? (nodeNameById.get(record.nodeId) ?? record.nodeId) : '—',
    },
    {
      key: 'pipeline',
      title: t('events.columns.pipeline'),
      dataIndex: 'pipelineId',
      width: 140,
      render: (value: string | null) => value ?? '—',
      exportValue: (record) => record.pipelineId ?? '—',
    },
    {
      key: 'acked',
      title: t('events.columns.acked'),
      dataIndex: 'ackedBy',
      width: 200,
      // ackedBy 为 null 才是"未确认"，不能用空字符串判断（见
      // web/src/api/events.ts 文件头注释第 2 点）。**确认时间另有三种形态**
      // （其中「ackedBy 有值、ackedAt 为 null」是 v43 迁移不回填留下的
      // 永久存量，不是边界情况），判断收口在 `ackState()`。
      //
      // **渲染与导出共用同一个文本函数**：两处各写一份的话，CSV 与界面
      // 会在存量行上给出不同的说法，而那种不一致没有任何东西会报错。
      render: (_value: string | null, record: EventEntry) => {
        const state = ackState(record.ackedBy, record.ackedAt)
        const text = ackStateText(state, t)
        return state.kind === 'unacked' ? (
          <Tag>{text}</Tag>
        ) : (
          <Tag color="success">{text}</Tag>
        )
      },
      exportValue: (record) => ackStateText(ackState(record.ackedBy, record.ackedAt), t),
    },
    {
      key: 'payload',
      title: t('events.columns.payload'),
      dataIndex: 'payload',
      render: (value: Record<string, unknown> | undefined) =>
        value ? (
          <Popover
            content={<pre style={{ margin: 0 }}>{JSON.stringify(value, null, 2)}</pre>}
          >
            <Typography.Link>{t('events.columns.payload')}</Typography.Link>
          </Popover>
        ) : (
          '—'
        ),
      exportValue: (record) => (record.payload ? JSON.stringify(record.payload) : '—'),
    },
    {
      key: 'action',
      title: t('events.columnsAction'),
      dataIndex: 'id',
      width: 96,
      // **只对未确认行给入口**：已确认的行再点一次只会换来一个 409，
      // 那是把后端的并发保护当成交互反馈用。判据仍收口在 `ackState()`，
      // 不在这里重写一遍 `ackedBy === null`（那正是上面那列注释说的两处各写一份）。
      render: (_value: string, record: EventEntry) => {
        if (!canAck || ackState(record.ackedBy, record.ackedAt).kind !== 'unacked') {
          return null
        }
        return (
          <Popconfirm
            title={t('events.ackConfirmTitle')}
            description={t('events.ackConfirmDesc')}
            onConfirm={() => handleAck(record.id)}
            okButtonProps={{ loading: ackMutation.isPending }}
          >
            <Button type="link" size="small" disabled={ackMutation.isPending}>
              {t('events.ackAction')}
            </Button>
          </Popconfirm>
        )
      },
      // 确认是交互，不是数据：导出里给空串而不是按钮文字，
      // 否则 CSV 里会出现一列全是"确认"的噪声。
      exportValue: () => '',
    },
  ]
  const { visibleColumns, pickerProps } = useColumnVisibility('events', columns)

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
            placeholder={t('events.filters.node')}
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
          <Input
            allowClear
            placeholder={t('events.filters.pipeline')}
            style={{ width: 200 }}
            value={pipelineId}
            onChange={(e) => {
              setPipelineId(e.target.value)
              setPage(1)
            }}
          />
          <Select
            allowClear
            placeholder={t('events.filters.type')}
            style={{ width: 180 }}
            value={type}
            onChange={(value: string | undefined) => {
              // 金样里的取值是 string（JSON 导入推不出字面量，实测确认）。
              // 这里收窄成 EventType：两者一致由 web/src/api/events.golden.test.ts 钉住。
              // 即使某天后端加了取值而 union 暂时落后，这个下拉框照样能用
              // ——选中后发给后端的只是一个查询串，运行时不会出错；而反过来
              // 让下拉框等 union 更新，那些事件在筛选时就是整条消失。
              setType(value as EventType | undefined)
              setPage(1)
            }}
            options={EVENT_TYPES.map((item) => ({
              value: item,
              label: eventTypeLabel(item, t),
            }))}
          />
          <Select
            allowClear
            placeholder={t('events.filters.level')}
            style={{ width: 140 }}
            value={level}
            onChange={(value: EventLevel | undefined) => {
              setLevel(value)
              setPage(1)
            }}
            options={EVENT_LEVELS.map((item) => ({
              value: item,
              label: t(`events.levels.${item}`),
            }))}
          />
          <Checkbox
            checked={onlyUnacked}
            onChange={(e) => {
              setOnlyUnacked(e.target.checked)
              setPage(1)
            }}
          >
            {t('events.filters.onlyUnacked')}
          </Checkbox>
          <RangePicker
            allowClear
            showTime
            placeholder={[t('events.filters.since'), t('events.filters.until')]}
            value={range}
            onChange={(value) => {
              setRange(value && value[0] && value[1] ? [value[0], value[1]] : null)
              setPage(1)
            }}
            // 只挡未来日期——这个端点没有跨度上限，不做任何天数限制（见组件
            // 顶部注释）。
            disabledDate={(current) => current.isAfter(dayjs().endOf('minute'))}
          />
        </Space>
        <Space wrap>
          {eventsQuery.isError && (
            <HintIcon type="error" content={<> {eventsQuery.error.message} </>} />
          )}
          <ExportButton
            filename="events"
            columns={visibleColumns}
            rows={eventsQuery.data?.items ?? []}
            scope="page"
          />
          <ColumnSettingsButton {...pickerProps} />
          <HintIcon content={<>{t('events.noWindowCapHint')}</>} />

          {/* 硬性要求同 LogsPage：后端实际生效的 since/until 必须常驻展示。 */}
          <Alert
            type="info"
            showIcon
            title={
              eventsQuery.data
                ? `${t('events.effectiveRange')}${formatDateTime(eventsQuery.data.since)} ~ ${formatDateTime(eventsQuery.data.until)}`
                : t('events.effectiveRangeLoading')
            }
            description={range === null ? t('events.effectiveRangeDefaultHint') : undefined}
          />
        </Space>
      </Space>

      <div
        className="table-fill-container"
        style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
      >
        <ScrollTable<EventEntry>
          rowKey="id"
          loading={eventsQuery.isLoading}
          dataSource={eventsQuery.data?.items ?? []}
          locale={{ emptyText: t('events.empty') }}
          pagination={{
            current: page,
            pageSize,
            total: eventsQuery.data?.total ?? 0,
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
