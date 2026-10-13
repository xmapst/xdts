import { ActionDrawer } from '@/components/ActionDrawer'
import { useHintFeedback } from '@/components/hintFeedback'
import { HintAlert as Alert } from '@/components/HintAlert'
import { useState } from 'react'
import { Button, Checkbox, Input, Space } from 'antd'
import { CopyOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import type { RegisterCodeCreated } from '@/api/nodes'

interface RegisterCodeRevealDrawerProps {
  /** 生成响应；仅在生成的那一次拿到，关闭弹窗后调用方应立即丢弃引用。 */
  created: RegisterCodeCreated | null
  onClose: () => void
}

/* Registration codes are shown once and require saved consent before closing. Plaintext stays in transient props. */
export function RegisterCodeRevealDrawer({
  created,
  onClose,
}: RegisterCodeRevealDrawerProps) {
  const { t } = useTranslation()
  const { message } = useHintFeedback()
  const [acknowledged, setAcknowledged] = useState(false)

  const handleClose = () => {
    setAcknowledged(false)
    onClose()
  }

  const handleCopy = async () => {
    if (!created) {
      return
    }
    try {
      await navigator.clipboard.writeText(created.code)
      void message.success(t('common.copied'))
    } catch {
      void message.error(t('common.operationFailed'))
    }
  }

  return (
    <ActionDrawer
      open={created !== null}
      title={t('nodes.registerCodes.reveal.title')}
      onClose={handleClose}
      closable={false}
      mask={{ closable: false }}
      keyboard={false}
      confirmText={t('nodes.registerCodes.reveal.close')}
      confirmButtonProps={{ disabled: !acknowledged }}
      onConfirm={handleClose}
      hideCancel
      // 关闭后必须把明文彻底从 DOM 里销毁，而不是只用 CSS 隐藏——它是敏感数据，
      // 不应该在关闭后还能通过 DevTools / 内存查看残留。
      destroyOnHidden
    >
      {created && (
        <Space orientation="vertical" style={{ width: '100%' }} size="middle">
          <Space.Compact style={{ width: '100%' }}>
            <Input
              readOnly
              value={created.code}
              style={{ fontFamily: 'monospace' }}
              onFocus={(event) => event.currentTarget.select()}
            />
            <Button icon={<CopyOutlined />} onClick={() => void handleCopy()}>
              {t('common.copy')}
            </Button>
          </Space.Compact>
          <Checkbox
            aria-label={t('nodes.registerCodes.reveal.ackLabel')}
            checked={acknowledged}
            onChange={(e) => setAcknowledged(e.target.checked)}
          >
            <span className="field-label-hint">
              {t('nodes.registerCodes.reveal.ackLabel')}
              <Alert type="warning" title={t('nodes.registerCodes.reveal.warning')} />
            </span>
          </Checkbox>
        </Space>
      )}
    </ActionDrawer>
  )
}
