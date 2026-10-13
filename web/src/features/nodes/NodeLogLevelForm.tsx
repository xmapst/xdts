import { useHintFeedback } from '@/components/hintFeedback'
import { HintForm } from '@/components/HintForm'
import { useEffect } from 'react'
import { HintIcon } from '@/components/HintIcon'
import { Button, Descriptions, Form, Select, Tag } from 'antd'
import { useTranslation } from 'react-i18next'
import type { LogLevel, NodeActualLogLevel, NodeDetail } from '@/api/nodes'
import { useSetNodeLogLevelMutation } from '@/api/nodes'
import { RequireRole } from '@/components/RequireRole'

const LOG_LEVELS: LogLevel[] = ['debug', 'info', 'warn', 'error']

interface LogLevelFormValues {
  logLevel: LogLevel
  logShipLevel: LogLevel
}

/**
 * 生效状态标签：`'unknown'` 是目前唯一可能的取值（见 `web/src/api/nodes.ts` 里
 * `NodeDetail` 上方的注释——cmd.node 已定义但零生产者，确认不会发生），但按真实值渲染而
 * 不是硬编码"未知"文案，一旦后端将来真的能确认生效值，这里不需要跟着改。
 */
function ActualStatusTag({ value }: { value: NodeActualLogLevel }) {
  const { t } = useTranslation()
  if (value === 'unknown') {
    return <Tag>{t('nodes.detail.actualStatusUnknown')}</Tag>
  }
  return <Tag color="success">{value}</Tag>
}

/* MGR-NODE-07：远程调整节点本地日志级别与日志上报级别。 */
export function NodeLogLevelForm({ node }: { node: NodeDetail }) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [form] = Form.useForm<LogLevelFormValues>()
  const mutation = useSetNodeLogLevelMutation()

  useEffect(() => {
    form.setFieldsValue({
      logLevel: node.desiredLogLevel,
      logShipLevel: node.desiredLogShipLevel,
    })
  }, [node.desiredLogLevel, node.desiredLogShipLevel, form])

  const handleSubmit = (values: LogLevelFormValues) => {
    mutation.mutate(
      { id: node.id, input: values },
      {
        onSuccess: () => void message.success(t('common.operationSucceeded')),
        onError: (error) => void message.error(error.message),
      },
    )
  }

  return (
    <div>
      <RequireRole
        role="admin"
        fallback={
          <Descriptions column={1} bordered size="small">
            <Descriptions.Item
              label={
                <span className="field-label-hint">
                  {t('nodes.detail.desiredLogLevel')}
                  <HintIcon
                    type="warning"
                    content={t('nodes.detail.logLevelActualUnknownHint')}
                  />
                </span>
              }
            >
              {node.desiredLogLevel}
            </Descriptions.Item>
            <Descriptions.Item label={t('nodes.detail.actualLogLevel')}>
              <ActualStatusTag value={node.actualLogLevel} />
            </Descriptions.Item>
            <Descriptions.Item label={t('nodes.detail.desiredLogShipLevel')}>
              {node.desiredLogShipLevel}
            </Descriptions.Item>
            <Descriptions.Item label={t('nodes.detail.actualLogShipLevel')}>
              <ActualStatusTag value={node.actualLogShipLevel} />
            </Descriptions.Item>
          </Descriptions>
        }
      >
        <Form<LogLevelFormValues>
          form={form}
          layout="vertical"
          onFinish={handleSubmit}
          className="compact-form-grid"
          style={{ maxWidth: 640 }}
          disabled={mutation.isPending}
        >
          <HintForm
            name="logLevel"
            label={
              <span className="field-label-hint">
                {t('nodes.detail.desiredLogLevel')}
                <HintIcon
                  type="warning"
                  content={t('nodes.detail.logLevelActualUnknownHint')}
                />
              </span>
            }
          >
            <Select options={LOG_LEVELS.map((level) => ({ value: level, label: level }))} />
          </HintForm>
          <HintForm name="logShipLevel" label={t('nodes.detail.desiredLogShipLevel')}>
            <Select options={LOG_LEVELS.map((level) => ({ value: level, label: level }))} />
          </HintForm>
          <Descriptions className="compact-form-actions" column={2} size="small">
            <Descriptions.Item label={t('nodes.detail.actualLogLevel')}>
              <ActualStatusTag value={node.actualLogLevel} />
            </Descriptions.Item>
            <Descriptions.Item label={t('nodes.detail.actualLogShipLevel')}>
              <ActualStatusTag value={node.actualLogShipLevel} />
            </Descriptions.Item>
          </Descriptions>
          <HintForm className="compact-form-actions">
            <Button type="primary" htmlType="submit" loading={mutation.isPending}>
              {t('nodes.detail.saveLogLevel')}
            </Button>
          </HintForm>
        </Form>
      </RequireRole>
    </div>
  )
}
