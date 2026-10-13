import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintForm } from '@/components/HintForm'
import { useEffect } from 'react'
import { Form, Input } from 'antd'
import { useTranslation } from 'react-i18next'
import {
  useCreateCenterMutation,
  useUpdateCenterMutation,
  type CenterSummary,
} from '@/api/centers'

interface CenterFormValues {
  code: string
  name: string
  region?: string
  description?: string
}

interface CenterFormDrawerProps {
  open: boolean
  onClose: () => void
  /** 传入表示编辑；不传表示新建。 */
  center?: CenterSummary
}

/** 中心创建 / 编辑弹窗（MGR-CTR-01）：编码创建后不可修改，唯一性冲突由后端校验并回显。 */
export function CenterFormDrawer({ open, onClose, center }: CenterFormDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<CenterFormValues>()
  const createMutation = useCreateCenterMutation()
  const updateMutation = useUpdateCenterMutation()
  const isEdit = Boolean(center)
  const pending = createMutation.isPending || updateMutation.isPending

  useEffect(() => {
    if (!open) {
      return
    }
    form.setFieldsValue({
      code: center?.code ?? '',
      name: center?.name ?? '',
      region: center?.region ?? undefined,
      description: center?.description ?? undefined,
    })
  }, [open, center, form])

  const handleSubmit = (values: CenterFormValues) => {
    const onSettled = {
      onSuccess: () => {
        void message.success(t('common.operationSucceeded'))
        onClose()
      },
      onError: (error: Error) => void message.error(error.message),
    }
    if (isEdit && center) {
      updateMutation.mutate({ id: center.id, input: values }, onSettled)
    } else {
      createMutation.mutate(values, onSettled)
    }
  }

  return (
    <ActionDrawer
      open={open}
      title={t(isEdit ? 'centers.edit' : 'centers.create')}
      onClose={onClose}
      onConfirm={() => form.submit()}
      confirmLoading={pending}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      destroyOnHidden
    >
      <Form<CenterFormValues>
        form={form}
        layout="vertical"
        onFinish={handleSubmit}
        disabled={pending}
      >
        <HintForm
          name="code"
          label={t('centers.form.code')}
          extra={t('centers.form.codeHint')}
          rules={[{ required: true, message: t('centers.form.codeRequired') }]}
        >
          <Input disabled={isEdit} />
        </HintForm>
        <HintForm
          name="name"
          label={t('centers.form.name')}
          rules={[
            { required: true, whitespace: true, message: t('centers.form.nameRequired') },
            { max: 128 },
          ]}
        >
          <Input maxLength={128} />
        </HintForm>
        <HintForm name="region" label={t('centers.form.region')}>
          <Input />
        </HintForm>
        <HintForm name="description" label={t('centers.form.description')}>
          <Input.TextArea rows={2} />
        </HintForm>
      </Form>
    </ActionDrawer>
  )
}
