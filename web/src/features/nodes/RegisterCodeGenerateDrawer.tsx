import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintForm } from '@/components/HintForm'
import { useEffect } from 'react'
import { Form, Input, InputNumber, Select } from 'antd'
import { useTranslation } from 'react-i18next'
import { useCreateRegisterCodeMutation } from '@/api/nodes'
import type { RegisterCodeCreated } from '@/api/nodes'
import { useCentersQuery } from '@/api/centers'

interface RegisterCodeGenerateFormValues {
  centerId: string
  /** 有效天数；留空表示长期有效（不设过期时间）。 */
  validDays?: number
  presetName?: string
  presetLabels?: string[]
}

interface RegisterCodeGenerateDrawerProps {
  open: boolean
  onClose: () => void
  /** 生成成功后把明文响应交给父组件，由父组件用 RegisterCodeRevealDrawer 一次性展示。 */
  onCreated: (created: RegisterCodeCreated) => void
  defaultCenterId?: string
}

const MS_PER_DAY = 86_400_000

export function RegisterCodeGenerateDrawer({
  open,
  onClose,
  onCreated,
  defaultCenterId,
}: RegisterCodeGenerateDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<RegisterCodeGenerateFormValues>()
  const centersQuery = useCentersQuery(open)
  const createMutation = useCreateRegisterCodeMutation()

  useEffect(() => {
    if (open) {
      form.resetFields()
      if (defaultCenterId) {
        form.setFieldValue('centerId', defaultCenterId)
      }
    }
  }, [open, defaultCenterId, form])

  const handleSubmit = (values: RegisterCodeGenerateFormValues) => {
    const expiresAt =
      values.validDays && values.validDays > 0
        ? new Date(Date.now() + values.validDays * MS_PER_DAY).toISOString()
        : undefined
    createMutation.mutate(
      {
        centerId: values.centerId,
        expiresAt,
        presetName: values.presetName,
        presetLabels: values.presetLabels,
      },
      {
        onSuccess: (created) => {
          onCreated(created)
          onClose()
        },
        onError: (error) => {
          void message.error(error.message)
        },
      },
    )
  }

  return (
    <ActionDrawer
      open={open}
      title={t('nodes.registerCodes.generateTitle')}
      onClose={onClose}
      onConfirm={() => form.submit()}
      confirmLoading={createMutation.isPending}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      destroyOnHidden
    >
      <Form<RegisterCodeGenerateFormValues>
        form={form}
        layout="vertical"
        onFinish={handleSubmit}
        disabled={createMutation.isPending}
      >
        <HintForm
          name="centerId"
          label={t('nodes.registerCodes.center')}
          rules={[{ required: true, message: t('nodes.registerCodes.centerRequired') }]}
        >
          <Select
            loading={centersQuery.isLoading}
            options={(centersQuery.data ?? []).map((center) => ({
              value: center.id,
              label: `${center.name}（${center.code}）`,
            }))}
          />
        </HintForm>
        <HintForm name="validDays" label={t('nodes.registerCodes.expiresAt')}>
          <InputNumber
            min={1}
            style={{ width: '100%' }}
            placeholder={t('nodes.registerCodes.expiresAtPlaceholder')}
          />
        </HintForm>
        <HintForm name="presetName" label={t('nodes.registerCodes.presetName')}>
          <Input placeholder={t('nodes.registerCodes.presetNamePlaceholder')} />
        </HintForm>
        <HintForm name="presetLabels" label={t('nodes.registerCodes.presetLabels')}>
          <Select
            mode="tags"
            open={false}
            placeholder={t('nodes.registerCodes.presetLabelsPlaceholder')}
          />
        </HintForm>
      </Form>
    </ActionDrawer>
  )
}
