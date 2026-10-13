import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintForm } from '@/components/HintForm'
import { HintIcon } from '@/components/HintIcon'
import { useEffect } from 'react'
import { Form, Input } from 'antd'
import { useTranslation } from 'react-i18next'
import {
  useCreateStrategyMutation,
  useUpdateStrategyMetaMutation,
  type StrategySummary,
} from '@/api/strategies'

interface StrategyFormValues {
  name: string
  description?: string
}

interface StrategyFormDrawerProps {
  open: boolean
  onClose: () => void
  /** 传入表示编辑名称 / 描述；不传表示新建。 */
  strategy?: StrategySummary
}

/**
 * 策略创建 / 编辑名称弹窗（MGR-STR-01）。只管 name/description——rules 不在
 * 这里编辑，创建后需要在「版本管理」里单独创建第一个版本（见
 * web/src/api/strategies.ts 文件头注释 5：POST /strategies 只建空壳）。
 */
export function StrategyFormDrawer({ open, onClose, strategy }: StrategyFormDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<StrategyFormValues>()
  const createMutation = useCreateStrategyMutation()
  const updateMutation = useUpdateStrategyMetaMutation()
  const isEdit = Boolean(strategy)
  const pending = createMutation.isPending || updateMutation.isPending

  useEffect(() => {
    if (!open) {
      return
    }
    form.setFieldsValue({
      name: strategy?.name ?? '',
      description: strategy?.description ?? undefined,
    })
  }, [open, strategy, form])

  const handleSubmit = (values: StrategyFormValues) => {
    const onSettled = {
      onSuccess: () => {
        void message.success(t('common.operationSucceeded'))
        onClose()
      },
      onError: (error: Error) => void message.error(error.message),
    }
    if (isEdit && strategy) {
      updateMutation.mutate({ id: strategy.id, input: values }, onSettled)
    } else {
      createMutation.mutate(values, onSettled)
    }
  }

  return (
    <ActionDrawer
      open={open}
      title={
        <span className="section-title">
          {t(isEdit ? 'strategies.editMeta' : 'strategies.create')}
          {!isEdit && <HintIcon content={t('strategies.form.createHint')} />}
        </span>
      }
      onClose={onClose}
      onConfirm={() => form.submit()}
      confirmLoading={pending}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      destroyOnHidden
    >
      <Form<StrategyFormValues>
        form={form}
        layout="vertical"
        onFinish={handleSubmit}
        disabled={pending}
      >
        <HintForm
          name="name"
          label={t('strategies.form.name')}
          rules={[{ required: true, message: t('strategies.form.nameRequired') }]}
        >
          <Input />
        </HintForm>
        <HintForm name="description" label={t('strategies.form.description')}>
          <Input.TextArea rows={2} />
        </HintForm>
      </Form>
    </ActionDrawer>
  )
}
