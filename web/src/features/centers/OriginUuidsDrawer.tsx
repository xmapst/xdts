import { ScrollTable } from '@/components/ScrollTable'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintForm } from '@/components/HintForm'
import { HintIcon } from '@/components/HintIcon'
import { Button, Drawer, Form, Input, Popconfirm, Tag, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import {
  useAddOriginUuidMutation,
  useDeleteOriginUuidMutation,
  useOriginUuidsQuery,
  type CenterSummary,
  type OriginUuid,
  type OriginUuidKind,
} from '@/api/centers'
import { RequireRole } from '@/components/RequireRole'
import { ApiError } from '@/api/client'
import { formatDateTime } from '@/utils/format'
import { enumLabel } from '@/utils/i18nEnum'
import { Link } from 'react-router-dom'

interface AddUuidFormValues {
  uuid: string
}

// Colors cover endpoint, manual and synthetic identities returned by the server.
const KIND_COLOR: Record<OriginUuidKind, string> = {
  server: 'blue',
  group: 'purple',
  manual: 'green',
  synthetic: 'gold',
}

interface OriginUuidsDrawerProps {
  open: boolean
  onClose: () => void
  center: CenterSummary | undefined
}

/** Server uniqueness errors are shown on the manual UUID field. */
export function OriginUuidsDrawer({ open, onClose, center }: OriginUuidsDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<AddUuidFormValues>()
  const centerId = center?.id
  const originUuidsQuery = useOriginUuidsQuery(open ? centerId : undefined)
  const addMutation = useAddOriginUuidMutation(centerId ?? '')
  const deleteMutation = useDeleteOriginUuidMutation(centerId ?? '')

  const handleAdd = (values: AddUuidFormValues) => {
    if (!centerId) {
      return
    }
    addMutation.mutate(
      { uuid: values.uuid.trim() },
      {
        onSuccess: () => {
          form.resetFields()
          void message.success(t('common.operationSucceeded'))
        },
        onError: (error) => {
          const description =
            error instanceof ApiError ? error.message : t('common.operationFailed')
          form.setFields([{ name: 'uuid', errors: [description] }])
        },
      },
    )
  }

  const handleDelete = (uuidId: string) => {
    deleteMutation.mutate(uuidId, {
      onSuccess: () => void message.success(t('common.operationSucceeded')),
      onError: (error) => void message.error(error.message),
    })
  }

  return (
    <Drawer
      title={
        <span className="section-title">
          {t('centers.originUuids.title')}
          <HintIcon content={t('centers.originUuids.hint')} />
        </span>
      }
      open={open}
      onClose={onClose}
      size={960}
      className="origin-uuids-drawer"
      styles={{
        body: {
          display: 'flex',
          flexDirection: 'column',
          minHeight: 0,
          overflow: 'hidden',
          padding: 12,
        },
      }}
    >
      <RequireRole role="admin">
        <Form<AddUuidFormValues>
          form={form}
          layout="inline"
          onFinish={handleAdd}
          style={{ marginBottom: 12, flexShrink: 0, flexWrap: 'nowrap' }}
        >
          <HintForm
            name="uuid"
            rules={[{ required: true, message: t('centers.originUuids.uuidRequired') }]}
            style={{ flex: 1, minWidth: 0 }}
          >
            <Input
              placeholder={t('centers.originUuids.uuidPlaceholder')}
              style={{ fontFamily: 'monospace' }}
            />
          </HintForm>
          <HintForm>
            <Button type="primary" htmlType="submit" loading={addMutation.isPending}>
              {t('centers.originUuids.add')}
            </Button>
          </HintForm>
        </Form>
      </RequireRole>

      <div
        className="table-fill-container"
        style={{ flex: '1 1 auto', minHeight: 0, overflow: 'hidden' }}
      >
        <ScrollTable<OriginUuid>
          rowKey="id"
          size="small"
          loading={originUuidsQuery.isLoading}
          dataSource={originUuidsQuery.data ?? []}
          locale={{ emptyText: t('centers.originUuids.empty') }}
          pagination={{ pageSize: 20, hideOnSinglePage: true, showSizeChanger: false }}
          columns={[
            {
              title: t('centers.originUuids.uuid'),
              dataIndex: 'uuid',
              render: (value: string) => (
                <Typography.Text code copyable>
                  {value}
                </Typography.Text>
              ),
            },
            {
              title: t('centers.originUuids.kind'),
              dataIndex: 'kind',
              // kind 来自后端来源身份登记表。
              render: (kind: OriginUuidKind) => (
                <Tag color={KIND_COLOR[kind]}>
                  {enumLabel(t, 'centers.originUuids.kindLabel', kind)}
                </Tag>
              ),
            },
            {
              title: t('centers.originUuids.refId'),
              dataIndex: 'refId',
              render: (value: string | null, record) => {
                if (!record.refName)
                  return value ? (
                    <Typography.Text code copyable>
                      {value}
                    </Typography.Text>
                  ) : (
                    '—'
                  )
                const url =
                  record.refType === 'pipeline' ? `/pipelines/${value}` : undefined
                return (
                  <Typography.Text title={value ?? undefined}>
                    {url ? <Link to={url}>{record.refName}</Link> : record.refName}
                  </Typography.Text>
                )
              },
            },
            {
              title: t('centers.originUuids.createdAt'),
              dataIndex: 'createdAt',
              render: (value: string) => formatDateTime(value),
            },
            {
              title: t('common.actions'),
              key: 'actions',
              render: (_value, record) => (
                <RequireRole role="admin">
                  <Popconfirm
                    title={t('centers.originUuids.deleteConfirm')}
                    onConfirm={() => handleDelete(record.id)}
                  >
                    {}
                    <Typography.Link type="danger">{t('common.delete')}</Typography.Link>
                  </Popconfirm>
                </RequireRole>
              ),
            },
          ]}
        />
      </div>
    </Drawer>
  )
}
