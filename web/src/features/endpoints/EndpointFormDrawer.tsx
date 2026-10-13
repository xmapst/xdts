import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintForm } from '@/components/HintForm'
import { useEffect } from 'react'
import { Form, Input, InputNumber, Select } from 'antd'
import { useTranslation } from 'react-i18next'
import {
  useCreateEndpointMutation,
  useUpdateEndpointMutation,
  type EndpointSummary,
  type EndpointUpdateResult,
} from '@/api/endpoints'
import { useCentersQuery } from '@/api/centers'

interface EndpointFormValues {
  name: string
  centerId: string
  host: string
  port: number
  username: string
  password?: string
}

interface EndpointFormDrawerProps {
  open: boolean
  onClose: () => void
  /** 传入表示编辑；不传表示新建。 */
  endpoint?: EndpointSummary
  /** 编辑成功后由页面展示后端返回的受影响线路。 */
  onUpdated?: (result: EndpointUpdateResult) => void
}

export function EndpointFormDrawer({
  open,
  onClose,
  endpoint,
  onUpdated,
}: EndpointFormDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<EndpointFormValues>()
  const createMutation = useCreateEndpointMutation()
  const updateMutation = useUpdateEndpointMutation()
  const centersQuery = useCentersQuery(open)
  const isEdit = Boolean(endpoint)
  const pending = createMutation.isPending || updateMutation.isPending

  useEffect(() => {
    if (!open) {
      return
    }
    form.setFieldsValue({
      name: endpoint?.name ?? '',
      centerId: endpoint?.centerId ?? '',
      host: endpoint?.host ?? '',
      port: endpoint?.port ?? 3306,
      username: endpoint?.username ?? '',
      password: undefined,
    })
  }, [open, endpoint, form])

  const handleSubmit = (values: EndpointFormValues) => {
    const onSettled = {
      onSuccess: () => {
        void message.success(t('common.operationSucceeded'))
        onClose()
      },
      onError: (error: Error) => void message.error(error.message),
    }
    if (isEdit && endpoint) {
      updateMutation.mutate(
        {
          id: endpoint.id,
          input: {
            name: values.name,
            host: values.host,
            port: values.port,
            username: values.username,
            ...(values.password ? { password: values.password } : {}),
          },
        },
        {
          onSuccess: (result) => {
            onClose()
            if (
              values.host !== endpoint.host ||
              values.port !== endpoint.port ||
              values.username !== endpoint.username ||
              values.password
            ) {
              onUpdated?.(result)
            } else {
              void message.success(t('common.operationSucceeded'))
            }
          },
          onError: onSettled.onError,
        },
      )
    } else {
      createMutation.mutate(
        {
          name: values.name,
          centerId: values.centerId,
          host: values.host,
          port: values.port,
          username: values.username,
          password: values.password ?? '',
        },
        onSettled,
      )
    }
  }

  return (
    <ActionDrawer
      open={open}
      title={t(isEdit ? 'dataEndpoints.edit' : 'dataEndpoints.create')}
      onClose={onClose}
      onConfirm={() => form.submit()}
      confirmLoading={pending}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      destroyOnHidden
    >
      <Form<EndpointFormValues>
        form={form}
        layout="vertical"
        onFinish={handleSubmit}
        disabled={pending}
        initialValues={{ port: 3306 }}
      >
        <HintForm
          name="name"
          label={t('dataEndpoints.form.name')}
          rules={[
            {
              required: true,
              whitespace: true,
              message: t('dataEndpoints.form.nameRequired'),
            },
            { max: 128 },
          ]}
        >
          <Input maxLength={128} />
        </HintForm>
        <HintForm
          name="centerId"
          label={t('dataEndpoints.form.center')}
          extra={isEdit ? t('dataEndpoints.form.centerHint') : undefined}
          rules={[{ required: true, message: t('dataEndpoints.form.centerRequired') }]}
        >
          <Select
            disabled={isEdit}
            loading={centersQuery.isLoading}
            options={(centersQuery.data ?? []).map((center) => ({
              value: center.id,
              label: `${center.name}（${center.code}）`,
            }))}
          />
        </HintForm>
        <HintForm
          name="host"
          label={t('dataEndpoints.form.host')}
          rules={[{ required: true, message: t('dataEndpoints.form.hostRequired') }]}
        >
          <Input />
        </HintForm>
        <HintForm
          name="port"
          label={t('dataEndpoints.form.port')}
          rules={[{ required: true, message: t('dataEndpoints.form.portRequired') }]}
        >
          <InputNumber min={1} max={65535} style={{ width: '100%' }} />
        </HintForm>
        <HintForm
          name="username"
          label={t('dataEndpoints.form.username')}
          rules={[{ required: true, message: t('dataEndpoints.form.usernameRequired') }]}
        >
          <Input autoComplete="off" />
        </HintForm>
        <HintForm
          name="password"
          label={t('dataEndpoints.form.password')}
          rules={
            isEdit
              ? []
              : [{ required: true, message: t('dataEndpoints.form.passwordRequired') }]
          }
        >
          <Input.Password
            autoComplete="new-password"
            placeholder={isEdit ? t('dataEndpoints.form.passwordPlaceholder') : undefined}
          />
        </HintForm>
      </Form>
    </ActionDrawer>
  )
}
