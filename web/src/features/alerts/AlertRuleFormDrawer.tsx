import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { useEffect } from 'react'
import { Form, Input, InputNumber, Select, Switch } from 'antd'
import { useTranslation } from 'react-i18next'
import {
  ALERT_DIMENSIONS,
  useSaveAlertRuleMutation,
  type AlertDimension,
  type AlertRule,
  type AlertRuleInput,
} from '@/api/alerts'

interface Values {
  name: string
  dimension: AlertDimension
  enabled: boolean
  value: number
  windowSeconds: number
  suppressSeconds: number
  webhookUrl?: string
  clearWebhook?: boolean
}
export function AlertRuleFormDrawer({
  open,
  rule,
  onClose,
}: {
  open: boolean
  rule?: AlertRule
  onClose: () => void
}) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<Values>()
  const mutation = useSaveAlertRuleMutation()
  const dimension = Form.useWatch('dimension', form)
  useEffect(() => {
    if (open) {
      form.resetFields()
      form.setFieldsValue(
        rule
          ? {
              name: rule.name,
              dimension: rule.dimension,
              enabled: rule.enabled,
              value: rule.threshold.value,
              windowSeconds: rule.threshold.windowSeconds,
              suppressSeconds: rule.suppressSeconds,
            }
          : {
              name: '',
              dimension: 'node_offline',
              enabled: true,
              value: 0,
              windowSeconds: 300,
              suppressSeconds: 300,
            },
      )
    }
  }, [open, rule, form])
  const submit = async (values: Values) => {
    const input: AlertRuleInput = {
      name: values.name.trim(),
      dimension: values.dimension,
      enabled: values.enabled,
      threshold: { value: values.value, windowSeconds: values.windowSeconds },
      suppressSeconds: values.suppressSeconds,
    }
    if (values.clearWebhook) input.webhookUrl = ''
    else if (values.webhookUrl?.trim()) input.webhookUrl = values.webhookUrl.trim()
    try {
      await mutation.mutateAsync({ id: rule?.id, version: rule?.version, input })
      void message.success(t('common.operationSucceeded'))
      onClose()
    } catch (err) {
      void message.error(err instanceof Error ? err.message : t('common.operationFailed'))
    }
  }
  return (
    <ActionDrawer
      open={open}
      title={
        <span className="section-title">
          {t(rule ? 'alerts.editRule' : 'alerts.createRule')}
          <Alert type="info" title={t('alerts.ruleHelp')} />
        </span>
      }
      onClose={() => {
        if (!mutation.isPending) onClose()
      }}
      onConfirm={() => form.submit()}
      confirmLoading={mutation.isPending}
      confirmText={t('common.submit')}
      cancelText={t('common.cancel')}
      destroyOnHidden
    >
      <Form
        form={form}
        layout="vertical"
        onFinish={(values: Values) => void submit(values)}
        disabled={mutation.isPending}
      >
        <HintForm
          name="name"
          label={t('alerts.ruleName')}
          rules={[{ required: true, whitespace: true, message: t('alerts.required') }]}
        >
          <Input maxLength={128} />
        </HintForm>
        <HintForm
          name="dimension"
          label={t('alerts.dimension')}
          rules={[{ required: true }]}
        >
          <Select
            options={ALERT_DIMENSIONS.map((value) => ({
              value,
              label: t(`alerts.dimensions.${value}`),
            }))}
          />
        </HintForm>
        <HintForm name="enabled" label={t('alerts.enabled')} valuePropName="checked">
          <Switch />
        </HintForm>
        <HintForm
          name="value"
          label={t(
            dimension === 'latency_exceeded'
              ? 'alerts.latencyThreshold'
              : 'alerts.queueThreshold',
          )}
          rules={[{ required: true, type: 'number', min: 0, max: Number.MAX_SAFE_INTEGER }]}
          hidden={dimension !== 'latency_exceeded' && dimension !== 'queue_backlog'}
        >
          <InputNumber
            min={0}
            max={Number.MAX_SAFE_INTEGER}
            precision={0}
            style={{ width: '100%' }}
          />
        </HintForm>
        <HintForm
          name="windowSeconds"
          label={t('alerts.windowSeconds')}
          rules={[{ required: true, type: 'number', min: 1, max: 86400 }]}
        >
          <InputNumber min={1} max={86400} precision={0} style={{ width: '100%' }} />
        </HintForm>
        <HintForm
          name="suppressSeconds"
          label={t('alerts.suppressSeconds')}
          extra={t('alerts.suppressHelp')}
          rules={[{ required: true, type: 'number', min: 0, max: 86400 }]}
        >
          <InputNumber min={0} max={86400} precision={0} style={{ width: '100%' }} />
        </HintForm>
        <HintForm
          name="webhookUrl"
          label={t('alerts.webhookUrl')}
          extra={t(rule?.webhookConfigured ? 'alerts.webhookKeep' : 'alerts.webhookHelp')}
          rules={[
            {
              validator: (_, value: string | undefined) => {
                if (!value?.trim()) return Promise.resolve()
                try {
                  const url = new URL(value)
                  if (
                    ['http:', 'https:'].includes(url.protocol) &&
                    !url.username &&
                    !url.password &&
                    !url.hash &&
                    value.length <= 255
                  )
                    return Promise.resolve()
                } catch {
                  /* validation below */
                }
                return Promise.reject(new Error(t('alerts.invalidWebhook')))
              },
            },
          ]}
        >
          <Input.Password maxLength={255} autoComplete="off" />
        </HintForm>
        {rule?.webhookConfigured && (
          <HintForm
            name="clearWebhook"
            label={t('alerts.clearWebhook')}
            valuePropName="checked"
          >
            <Switch />
          </HintForm>
        )}
      </Form>
    </ActionDrawer>
  )
}
