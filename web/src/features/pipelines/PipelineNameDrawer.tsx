import { ActionDrawer } from '@/components/ActionDrawer'
import { useEffect, useState } from 'react'
import { Form, Input } from 'antd'
import { useTranslation } from 'react-i18next'
import { useUpdatePipelineMutation, type PipelineSummary } from '@/api/pipelines'
import { HintForm } from '@/components/HintForm'
import { HintIcon } from '@/components/HintIcon'

export function PipelineNameDrawer({
  pipeline,
  onClose,
}: {
  pipeline?: PipelineSummary
  onClose: () => void
}) {
  const { t } = useTranslation()
  const [form] = Form.useForm<{ name: string }>()
  const [error, setError] = useState<string>()
  const mutation = useUpdatePipelineMutation()
  useEffect(() => {
    form.setFieldsValue({ name: pipeline?.name })
  }, [pipeline?.id, pipeline?.name, form])
  return (
    <ActionDrawer
      title={<span className="section-title">{t('pipelines.rename.title')}</span>}
      open={!!pipeline}
      onClose={onClose}
      confirmLoading={mutation.isPending}
      confirmText={t('common.save')}
      cancelText={t('common.cancel')}
      onConfirm={() => form.submit()}
    >
      {error && <HintIcon type="error" content={error} />}
      <Form
        form={form}
        layout="vertical"
        onFinish={(values) => {
          if (!pipeline) return
          setError(undefined)
          mutation.mutate(
            { id: pipeline.id, input: { name: values.name.trim() } },
            {
              onSuccess: onClose,
              onError: (failure) => setError(failure.message),
            },
          )
        }}
      >
        <HintForm
          name="name"
          label={t('pipelines.columns.name')}
          extra={t('pipelines.rename.hint')}
          rules={[{ required: true, whitespace: true }, { max: 128 }]}
        >
          <Input
            aria-label={t('pipelines.columns.name')}
            maxLength={128}
            autoFocus
            disabled={mutation.isPending}
          />
        </HintForm>
      </Form>
    </ActionDrawer>
  )
}
