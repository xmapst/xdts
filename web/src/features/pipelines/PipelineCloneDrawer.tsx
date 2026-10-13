import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { HintForm } from '@/components/HintForm'
import { useEffect } from 'react'
import { Form, Input, Select } from 'antd'
import { useTranslation } from 'react-i18next'
import { useEndpointsQuery } from '@/api/endpoints'
import { useNodesQuery } from '@/api/nodes'
import { useClonePipelineMutation, type PipelineCloneInput } from '@/api/pipelineOperations'
import type { PipelineSummary } from '@/api/pipelines'

export function PipelineCloneDrawer({
  pipeline,
  onClose,
}: {
  pipeline?: PipelineSummary
  onClose: () => void
}) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<PipelineCloneInput>()
  const endpoints = useEndpointsQuery({ page: 1, pageSize: 200 }, Boolean(pipeline))
  const nodes = useNodesQuery({ page: 1, pageSize: 200 }, Boolean(pipeline))
  const clone = useClonePipelineMutation()
  const targetEndpoint = Form.useWatch('targetEndpointId', form)
  const center = endpoints.data?.items.find((item) => item.id === targetEndpoint)?.centerId
  const nodeOptions = (nodes.data?.items ?? [])
    .filter((item) => item.centerId === center)
    .map((item) => ({ value: item.id, label: item.name }))

  useEffect(() => {
    if (pipeline) {
      form.resetFields()
      form.setFieldsValue({
        name: `${pipeline.name}-copy`,
        targetEndpointId: pipeline.targetEndpointId,
        targetNodeId: pipeline.targetNodeId,
      })
    }
  }, [pipeline, form])

  const submit = (input: PipelineCloneInput) => {
    if (!pipeline) return
    clone.mutate(
      { id: pipeline.id, input: { ...input, name: input.name.trim() } },
      {
        onSuccess: () => {
          void message.success(t('pipelines.clone.success'))
          onClose()
        },
        onError: (error: Error) => {
          void message.error(error.message)
        },
      },
    )
  }
  const required = [{ required: true, message: t('pipelines.clone.required') }]
  return (
    <ActionDrawer
      open={Boolean(pipeline)}
      title={
        <span className="section-title">
          {t('pipelines.clone.title')}
          <HintIcon content={t('pipelines.clone.description')} />
        </span>
      }
      onClose={() => {
        if (!clone.isPending) onClose()
      }}
      onConfirm={() => form.submit()}
      confirmLoading={clone.isPending}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      destroyOnHidden
    >
      {(endpoints.isError || nodes.isError) && (
        <HintIcon type="error" content={endpoints.error?.message ?? nodes.error?.message} />
      )}
      <Form form={form} layout="vertical" onFinish={submit} disabled={clone.isPending}>
        <HintForm
          name="name"
          label={t('pipelines.columns.name')}
          rules={[
            ...required,
            { whitespace: true, message: t('pipelines.clone.required') },
            { max: 128 },
          ]}
        >
          <Input maxLength={128} />
        </HintForm>
        <HintForm
          name="targetEndpointId"
          label={t('pipelines.detail.targetEndpoint')}
          rules={required}
        >
          <Select
            loading={endpoints.isLoading}
            options={(endpoints.data?.items ?? []).map((item) => ({
              value: item.id,
              label: item.name,
            }))}
            onChange={() => form.setFieldValue('targetNodeId', undefined)}
          />
        </HintForm>
        <HintForm
          name="targetNodeId"
          label={t('pipelines.detail.targetNode')}
          rules={required}
        >
          <Select loading={nodes.isLoading} options={nodeOptions} />
        </HintForm>
      </Form>
    </ActionDrawer>
  )
}
