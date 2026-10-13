import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintIcon } from '@/components/HintIcon'
import { HintAlert as Alert } from '@/components/HintAlert'
import { HintForm } from '@/components/HintForm'
import { useEffect, useRef, useState } from 'react'
import { Button, Form, InputNumber, Space, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import { useCurrentUser } from '@/api/auth'
import { useSettingsMutation, useSettingsQuery, type SystemSettings } from '@/api/settings'

type SettingsValues = Omit<SystemSettings, 'version'>

export default function SettingsPage() {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const { role } = useCurrentUser()
  const [form] = Form.useForm<SettingsValues>()
  const query = useSettingsQuery()
  const mutation = useSettingsMutation()
  const [pending, setPending] = useState<SystemSettings>()
  const loadedVersion = useRef<string>(undefined)
  useEffect(() => {
    if (query.data && query.data.version !== loadedVersion.current) {
      form.setFieldsValue(query.data)
      loadedVersion.current = query.data.version
    }
  }, [form, query.data])
  if (role !== 'admin')
    return <Alert type="warning" title={t('settings.adminOnly')} showIcon />
  const save = () => {
    if (!pending) return
    mutation.mutate(pending, {
      onSuccess: () => {
        setPending(undefined)
        void message.success(t('settings.saved'))
      },
      onError: (error) => {
        setPending(undefined)
        void message.error(error.message)
      },
    })
  }
  return (
    <Space orientation="vertical" size="large" style={{ width: '100%' }}>
      <Typography.Title level={3} style={{ margin: 0 }}>
        <span className="section-title">
          {t('nav.settings')}
          <HintIcon content={t('settings.help')} />
        </span>
      </Typography.Title>
      <Space wrap>
        <Button loading={query.isFetching} onClick={() => void query.refetch()}>
          {t('settings.refresh')}
        </Button>
        {query.error && <Alert type="error" title={query.error.message} />}
      </Space>
      <Form
        form={form}
        layout="vertical"
        className="compact-form-grid"
        style={{ maxWidth: 800 }}
        onFinish={(values) => {
          if (query.data) setPending({ ...values, version: query.data.version })
        }}
      >
        {(
          [
            'logRetentionSeconds',
            'metricsRetentionSeconds',
            'eventRetentionSeconds',
            'conflictRetentionSeconds',
            'precheckValiditySeconds',
          ] as const
        ).map((key) => (
          <HintForm
            key={key}
            name={key}
            label={t(`settings.${key}`)}
            rules={[{ required: true }]}
          >
            <InputNumber
              precision={0}
              min={1}
              max={key === 'precheckValiditySeconds' ? 86400 : 3650 * 86400}
              style={{ width: '100%' }}
            />
          </HintForm>
        ))}
        <Button
          type="primary"
          className="compact-form-actions"
          htmlType="submit"
          disabled={!query.data || query.isFetching}
          loading={mutation.isPending}
        >
          {t('settings.save')}
        </Button>
      </Form>
      <ActionDrawer
        open={Boolean(pending)}
        title={t('settings.confirmTitle')}
        confirmText={t('common.confirm')}
        cancelText={t('common.cancel')}
        onConfirm={save}
        onClose={() => {
          if (!mutation.isPending) setPending(undefined)
        }}
        confirmLoading={mutation.isPending}
        cancelButtonProps={{ disabled: mutation.isPending }}
      >
        <Typography.Paragraph>{t('settings.confirmBody')}</Typography.Paragraph>
      </ActionDrawer>
    </Space>
  )
}
