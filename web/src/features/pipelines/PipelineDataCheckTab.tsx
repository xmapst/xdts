import { useState } from 'react'
import {
  Button,
  Descriptions,
  Form,
  Input,
  InputNumber,
  Select,
  Space,
  Tag,
  Typography,
} from 'antd'
import { MinusCircleOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { ActionDrawer } from '@/components/ActionDrawer'
import { HintForm } from '@/components/HintForm'
import { HintIcon } from '@/components/HintIcon'
import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import {
  listDataChecks,
  createDataCheck,
  cancelDataCheck,
  type DataCheckRun,
} from '@/api/dataChecks'
import { formatDateTime } from '@/utils/format'

interface CheckForm {
  sampleModulo: number
  tables: Array<{
    sourceSchema: string
    sourceTable: string
    targetSchema: string
    targetTable: string
    columns: string
  }>
}

export function PipelineDataCheckTab({
  pipelineId,
  canWrite,
}: {
  pipelineId: string
  canWrite: boolean
}) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const client = useQueryClient()
  const [open, setOpen] = useState(false)
  const [selected, setSelected] = useState<string>()
  const [form] = Form.useForm<CheckForm>()
  const key = ['pipelines', 'data-checks', pipelineId]
  const query = useQuery({
    queryKey: key,
    queryFn: ({ signal }) => listDataChecks(pipelineId, signal),
    refetchInterval: (q) =>
      q.state.data?.some((r) => ['queued', 'running'].includes(r.status)) ? 1500 : 15000,
  })
  const runs = query.data ?? []
  const current = runs.find((r) => r.id === selected)
  const pending = runs.some((r) => ['queued', 'running'].includes(r.status))
  const refresh = () => client.invalidateQueries({ queryKey: key })
  const status = (run: DataCheckRun) => (
    <Tag
      color={
        run.status === 'equal'
          ? 'success'
          : run.status === 'different'
            ? 'warning'
            : run.status === 'failed'
              ? 'error'
              : 'default'
      }
    >
      {t(`dataChecks.status.${run.status}`)}
    </Tag>
  )
  const cancel = async (run: DataCheckRun) => {
    try {
      await cancelDataCheck(pipelineId, run.id)
      await refresh()
    } catch (error) {
      void message.error(
        error instanceof Error ? error.message : t('common.operationFailed'),
      )
    }
  }
  const submit = async () => {
    const values = await form.validateFields()
    const config = {
      tables: values.tables.map((table) => ({
        sourceSchema: table.sourceSchema.trim(),
        sourceTable: table.sourceTable.trim(),
        targetSchema: table.targetSchema.trim(),
        targetTable: table.targetTable.trim(),
        columns: table.columns.split(',').map((pair) => {
          const parts = pair.trim().split('=')
          const source = parts[0]?.trim() ?? ''
          const target = parts.length === 1 ? source : (parts[1]?.trim() ?? '')
          if (parts.length > 2 || !source || !target)
            throw new Error(t('dataChecks.columnsHint'))
          return { source, target }
        }),
      })),
    }
    await createDataCheck(pipelineId, config, values.sampleModulo)
    setOpen(false)
    await refresh()
  }
  return (
    <div
      className="table-fill-container pipeline-data-check-panel"
      style={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        minHeight: 0,
        overflow: 'hidden',
      }}
    >
      <Space wrap style={{ marginBottom: 8 }}>
        {canWrite && (
          <Button type="primary" disabled={pending} onClick={() => setOpen(true)}>
            {t('dataChecks.create')}
          </Button>
        )}
        <Button
          icon={<ReloadOutlined />}
          loading={query.isFetching}
          onClick={() => void query.refetch()}
        >
          {t('common.refresh')}
        </Button>
        <HintIcon content={t('dataChecks.hint')} />
        {query.error && <HintIcon type="error" content={query.error.message} />}
      </Space>
      <ScrollTable<DataCheckRun>
        rowKey="id"
        dataSource={runs}
        size="small"
        pagination={{ pageSize: 20, showSizeChanger: false }}
        columns={[
          {
            title: t('dataChecks.createdAt'),
            dataIndex: 'created_at',
            render: formatDateTime,
            width: 170,
          },
          { title: t('dataChecks.version'), dataIndex: 'configVersion', width: 90 },
          {
            title: t('dataChecks.statusLabel'),
            render: (_, run) => status(run),
            width: 110,
          },
          {
            title: t('dataChecks.mode'),
            render: (_, run) =>
              run.sampleModulo === 1
                ? t('dataChecks.full')
                : t('dataChecks.sampleValue', { value: run.sampleModulo }),
            width: 130,
          },
          {
            title: t('dataChecks.progress'),
            render: (_, run) => `${run.completedTables}/${run.config.tables.length}`,
            width: 90,
          },
          {
            title: t('dataChecks.result'),
            render: (_, run) =>
              run.error ? (
                <Typography.Text type="danger">{run.error}</Typography.Text>
              ) : run.results.length ? (
                `${run.results.reduce((n, r) => n + r.readSource + r.readTarget, 0)} ${t('dataChecks.rows')}`
              ) : (
                '—'
              ),
          },
          {
            title: t('common.actions'),
            fixed: 'right',
            width: 155,
            render: (_, run) => (
              <Space size={4}>
                <Button type="link" size="small" onClick={() => setSelected(run.id)}>
                  {t('dataChecks.details')}
                </Button>
                {canWrite && ['queued', 'running'].includes(run.status) && (
                  <Button
                    type="link"
                    danger
                    size="small"
                    onClick={() => void cancel(run)}
                  >
                    {t('common.cancel')}
                  </Button>
                )}
              </Space>
            ),
          },
        ]}
      />
      <ActionDrawer
        open={open}
        onClose={() => setOpen(false)}
        title={t('dataChecks.create')}
        size={780}
        onConfirm={submit}
        confirmText={t('dataChecks.start')}
      >
        <Form
          form={form}
          layout="vertical"
          initialValues={{
            sampleModulo: 1,
            tables: [
              {
                sourceSchema: '',
                sourceTable: '',
                targetSchema: '',
                targetTable: '',
                columns: '',
              },
            ],
          }}
        >
          <HintForm
            name="sampleModulo"
            label={t('dataChecks.sampleModulo')}
            rules={[{ required: true }]}
          >
            <InputNumber min={1} max={1000000} precision={0} style={{ width: 180 }} />
          </HintForm>
          <Form.List name="tables">
            {(fields, { add, remove }) => (
              <>
                {fields.map((field) => (
                  <div
                    key={field.key}
                    style={{
                      borderBottom: '1px solid var(--ant-color-border)',
                      marginBottom: 8,
                    }}
                  >
                    <Space wrap align="start" size={8}>
                      {(
                        [
                          'sourceSchema',
                          'sourceTable',
                          'targetSchema',
                          'targetTable',
                        ] as const
                      ).map((name) => (
                        <HintForm
                          key={name}
                          name={[field.name, name]}
                          label={t(`dataChecks.${name}`)}
                          rules={[{ required: true }]}
                        >
                          <Input maxLength={64} style={{ width: 155 }} />
                        </HintForm>
                      ))}
                      <Button
                        type="text"
                        icon={<MinusCircleOutlined />}
                        aria-label={t('dataChecks.removeTable')}
                        disabled={fields.length === 1}
                        onClick={() => remove(field.name)}
                      />
                    </Space>
                    <HintForm
                      name={[field.name, 'columns']}
                      label={
                        <Space size={4}>
                          {t('dataChecks.columns')}
                          <HintIcon content={t('dataChecks.columnsHint')} />
                        </Space>
                      }
                      rules={[{ required: true }]}
                    >
                      <Input maxLength={26000} placeholder="id, name=display_name" />
                    </HintForm>
                  </div>
                ))}
                <Button
                  type="dashed"
                  icon={<PlusOutlined />}
                  disabled={fields.length >= 20}
                  onClick={() =>
                    add({
                      sourceSchema: '',
                      sourceTable: '',
                      targetSchema: '',
                      targetTable: '',
                      columns: '',
                    })
                  }
                >
                  {t('dataChecks.addTable')}
                </Button>
              </>
            )}
          </Form.List>
        </Form>
      </ActionDrawer>
      <ActionDrawer
        open={!!current}
        onClose={() => setSelected(undefined)}
        title={t('dataChecks.details')}
        size={920}
        fillBody
      >
        {current && (
          <>
            <Descriptions
              size="small"
              column={3}
              style={{ marginBottom: 8 }}
              items={[
                {
                  key: 'status',
                  label: t('dataChecks.statusLabel'),
                  children: status(current),
                },
                {
                  key: 'version',
                  label: t('dataChecks.version'),
                  children: current.configVersion,
                },
                {
                  key: 'progress',
                  label: t('dataChecks.progress'),
                  children: `${current.completedTables}/${current.config.tables.length}`,
                },
              ]}
            />
            {current.error && (
              <Typography.Text type="danger" style={{ marginBottom: 8 }}>
                {current.error}
              </Typography.Text>
            )}
            <CheckResults run={current} />
          </>
        )}
      </ActionDrawer>
    </div>
  )
}

function CheckResults({ run }: { run: DataCheckRun }) {
  const { t } = useTranslation()
  const [index, setIndex] = useState(0)
  const result = run.results[index]
  return (
    <>
      <Space wrap style={{ marginBottom: 8 }}>
        <Select
          value={result ? index : undefined}
          style={{ minWidth: 280 }}
          placeholder={t('dataChecks.waiting')}
          options={run.results.map((r, i) => ({
            value: i,
            label: `${r.table.sourceSchema}.${r.table.sourceTable} → ${r.table.targetSchema}.${r.table.targetTable}`,
          }))}
          onChange={setIndex}
        />
        {result && (
          <Tag color={result.equal ? 'success' : 'warning'}>
            {t(`dataChecks.status.${result.equal ? 'equal' : 'different'}`)}
          </Tag>
        )}
        {result && (
          <Typography.Text>
            {t('dataChecks.scanned', {
              source: result.readSource,
              target: result.readTarget,
            })}
          </Typography.Text>
        )}
        <HintIcon content={t('dataChecks.bucketHint')} />
      </Space>
      <ScrollTable
        rowKey="bucket"
        size="small"
        dataSource={result?.differences ?? []}
        pagination={{ pageSize: 20, showSizeChanger: false }}
        columns={[
          { title: t('dataChecks.bucket'), dataIndex: 'bucket', width: 100 },
          { title: t('dataChecks.sourceRows'), dataIndex: 'sourceRows', width: 130 },
          { title: t('dataChecks.targetRows'), dataIndex: 'targetRows', width: 130 },
          {
            title: t('dataChecks.keys'),
            dataIndex: 'keysEqual',
            render: (value: boolean) =>
              t(value ? 'dataChecks.same' : 'dataChecks.different'),
            width: 130,
          },
          {
            title: t('dataChecks.changedColumns'),
            dataIndex: 'columns',
            render: (columns: string[]) => columns.join(', ') || '—',
          },
        ]}
      />
    </>
  )
}
