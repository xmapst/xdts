import { ActionDrawer } from '@/components/ActionDrawer'
import { HintIcon } from '@/components/HintIcon'
import { HintForm } from '@/components/HintForm'
import { Form, Input, InputNumber, Select } from 'antd'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { type NodeSummary, useUpdateNodeMutation } from '@/api/nodes'

interface NodeEditValues {
  name: string
  labels: string[]
  maxPipelines: number
}

export function NodeEditDrawer({
  node,
  onClose,
}: {
  node: NodeSummary | null
  onClose: () => void
}) {
  return node ? <NodeEditForm key={node.id} node={node} onClose={onClose} /> : null
}

function NodeEditForm({ node, onClose }: { node: NodeSummary; onClose: () => void }) {
  const { t } = useTranslation()
  const [form] = Form.useForm<NodeEditValues>()
  const mutation = useUpdateNodeMutation()
  const [error, setError] = useState<string>()
  const save = async () => {
    if (!node) return
    let values: NodeEditValues
    try {
      values = await form.validateFields()
    } catch {
      return
    }
    mutation.mutate(
      { id: node.id, input: values },
      {
        onSuccess: onClose,
        onError: (cause) => setError(cause.message),
      },
    )
  }
  return (
    <ActionDrawer
      open
      title={<span className="section-title">{t('nodes.edit.title')}</span>}
      onClose={onClose}
      onConfirm={() => void save()}
      confirmLoading={mutation.isPending}
      confirmText={t('common.save')}
      cancelText={t('common.cancel')}
      destroyOnHidden
    >
      {error && <HintIcon type="error" content={error} />}
      <Form
        form={form}
        layout="vertical"
        initialValues={{
          name: node.name,
          labels: [...node.labels],
          maxPipelines: node.maxPipelines,
        }}
      >
        <HintForm
          name="name"
          label={t('nodes.columns.name')}
          rules={[{ required: true }, { max: 128 }]}
        >
          <Input maxLength={128} />
        </HintForm>
        <HintForm name="labels" label={t('nodes.columns.labels')}>
          <Select mode="tags" />
        </HintForm>
        <HintForm
          name="maxPipelines"
          label={t('nodes.quota.label')}
          extra={t('nodes.quota.hint')}
          rules={[{ required: true }, { type: 'integer', min: 0, max: 10000 }]}
        >
          <InputNumber min={0} max={10000} precision={0} style={{ width: '100%' }} />
        </HintForm>
      </Form>
    </ActionDrawer>
  )
}
