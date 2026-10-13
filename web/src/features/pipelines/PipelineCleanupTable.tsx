import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { useState } from 'react'
import { Button, Space, Tag } from 'antd'
import { useTranslation } from 'react-i18next'
import { useCurrentUser } from '@/api/auth'
import {
  usePipelineCleanupQuery,
  useRetryPipelineCleanupMutation,
  type CleanupStep,
  type PipelineCleanup,
} from '@/api/pipelineCleanup'
import { formatDateTime } from '@/utils/format'

export default function PipelineCleanupTable() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const { role } = useCurrentUser()
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const query = usePipelineCleanupQuery(page, pageSize)
  const retry = useRetryPipelineCleanupMutation()
  const step = (value: CleanupStep) => (
    <Space size={4}>
      <Tag
        color={
          value.state === 'done'
            ? 'success'
            : value.state === 'failed'
              ? 'error'
              : 'default'
        }
      >
        {t(`pipelines.cleanup.states.${value.state}`)}
      </Tag>
      <HintIcon type="error" content={value.error} />
    </Space>
  )
  return (
    <Space orientation="vertical" style={{ display: 'flex', padding: '0 0 16px' }}>
      <ScrollTable<PipelineCleanup>
        rowKey="id"
        loading={query.isLoading}
        dataSource={query.data?.items ?? []}
        scroll={{ x: 900 }}
        pagination={{
          current: page,
          pageSize,
          total: query.data?.total ?? 0,
          showSizeChanger: true,
          onChange: (next, size) => {
            setPage(next)
            setPageSize(size)
          },
        }}
        columns={[
          {
            title: (
              <span className="section-title">
                {t('pipelines.columns.name')}
                <HintIcon content={t('pipelines.cleanup.description')} />
                {query.isError && <HintIcon type="error" content={query.error.message} />}
              </span>
            ),
            dataIndex: 'name',
          },
          {
            title: t('pipelines.cleanup.status'),
            dataIndex: 'state',
            render: (state: PipelineCleanup['state']) => (
              <Tag color={state === 'failed' ? 'error' : 'processing'}>
                {t(`pipelines.cleanup.states.${state}`)}
              </Tag>
            ),
          },
          { title: t('pipelines.cleanup.source'), dataIndex: 'source', render: step },
          { title: t('pipelines.cleanup.target'), dataIndex: 'target', render: step },
          { title: t('pipelines.cleanup.queue'), dataIndex: 'queue', render: step },
          { title: t('pipelines.cleanup.serverId'), dataIndex: 'serverId', render: step },
          { title: t('pipelines.cleanup.attempts'), dataIndex: 'attempts' },
          {
            title: t('pipelines.cleanup.lastAttempt'),
            dataIndex: 'lastAttemptAt',
            render: (date: string | null) => (date ? formatDateTime(date) : '—'),
          },
          {
            title: t('pipelines.cleanup.error'),
            dataIndex: 'lastError',
            render: (error: string) => <HintIcon type="error" content={error} />,
          },
          ...(role === 'admin'
            ? [
                {
                  title: t('common.actions'),
                  key: 'actions',
                  render: (_: unknown, row: PipelineCleanup) => (
                    <Button
                      loading={retry.isPending && retry.variables === row.id}
                      onClick={() =>
                        retry.mutate(row.id, {
                          onSuccess: () =>
                            void message.success(t('pipelines.cleanup.scheduled')),
                          onError: (error: Error) => void message.error(error.message),
                        })
                      }
                    >
                      {t('pipelines.cleanup.retry')}
                    </Button>
                  ),
                },
              ]
            : []),
        ]}
      />
    </Space>
  )
}
