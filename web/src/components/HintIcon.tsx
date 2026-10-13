import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  InfoCircleOutlined,
  LoadingOutlined,
  WarningOutlined,
} from '@ant-design/icons'
import { Tooltip, theme } from 'antd'
import type { CSSProperties, ReactNode } from 'react'
import { useTranslation } from 'react-i18next'

export type HintType = 'info' | 'success' | 'warning' | 'error' | 'loading'

export function HintIcon({
  content,
  type = 'info',
  style,
  className,
}: {
  content: ReactNode
  type?: HintType
  style?: CSSProperties
  className?: string
}) {
  const { t } = useTranslation()
  const { token } = theme.useToken()
  if (content === undefined || content === null || content === false || content === '')
    return null
  const Icon = {
    info: InfoCircleOutlined,
    success: CheckCircleOutlined,
    warning: WarningOutlined,
    error: CloseCircleOutlined,
    loading: LoadingOutlined,
  }[type]
  const color = {
    info: token.colorInfo,
    success: token.colorSuccess,
    warning: token.colorWarning,
    error: token.colorError,
    loading: token.colorInfo,
  }[type]
  if (type === 'error') {
    return (
      <span
        role="alert"
        className={className ? `hint-error ${className}` : 'hint-error'}
        style={{ color, ...style }}
      >
        <CloseCircleOutlined aria-hidden />
        <span className="hint-error-content">{content}</span>
      </span>
    )
  }
  return (
    <Tooltip
      title={content}
      trigger={['hover', 'focus']}
      placement="bottomLeft"
      autoAdjustOverflow={{ adjustX: 1, adjustY: 1 }}
      align={{ overflow: { shiftX: true, shiftY: false, adjustX: true, adjustY: true } }}
      mouseEnterDelay={0}
      destroyOnHidden
      classNames={{ root: 'hint-popup' }}
      styles={{
        root: {
          width: 'max-content',
          maxWidth: 'min(640px, calc(100vw - 24px))',
        },
        container: {
          maxWidth: '100%',
          maxHeight: 'min(320px, 40vh)',
          overflowY: 'auto',
          whiteSpace: 'pre-wrap',
          overflowWrap: 'anywhere',
          overscrollBehavior: 'contain',
          scrollbarGutter: 'stable',
        },
      }}
    >
      <span
        role="img"
        aria-label={t(`common.hints.${type}`)}
        tabIndex={0}
        className={className ? `hint-icon ${className}` : 'hint-icon'}
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          color,
          cursor: 'help',
          pointerEvents: 'auto',
          fontSize: 14,
          ...style,
        }}
      >
        <Icon aria-hidden />
      </span>
    </Tooltip>
  )
}
