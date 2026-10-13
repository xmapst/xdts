import { Button, Drawer, Space, type ButtonProps, type DrawerProps } from 'antd'
import { useTranslation } from 'react-i18next'
import { useEffect, useState } from 'react'
import { HintIcon } from './HintIcon'

export interface ActionDrawerProps extends Omit<DrawerProps, 'footer'> {
  onConfirm?: () => void | Promise<void>
  confirmLoading?: boolean
  confirmText?: string
  cancelText?: string
  confirmButtonProps?: ButtonProps
  cancelButtonProps?: ButtonProps
  hideCancel?: boolean
  fillBody?: boolean
  footer?: DrawerProps['footer']
}

export function ActionDrawer({
  onConfirm,
  confirmLoading = false,
  confirmText,
  cancelText,
  confirmButtonProps,
  cancelButtonProps,
  hideCancel = false,
  fillBody = false,
  footer,
  onClose,
  children,
  className,
  styles,
  size = 600,
  open,
  ...props
}: ActionDrawerProps) {
  const { t } = useTranslation()
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string>()
  const busy = confirmLoading || submitting
  useEffect(() => {
    if (open) setError(undefined)
  }, [open])
  const submit = async () => {
    if (!onConfirm || busy) return
    setError(undefined)
    setSubmitting(true)
    try {
      await onConfirm()
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : t('common.operationFailed'))
    } finally {
      setSubmitting(false)
    }
  }
  return (
    <Drawer
      {...props}
      open={open}
      size={size}
      className={`action-drawer ${className ?? ''}`}
      onClose={(event) => {
        if (!busy) onClose?.(event)
      }}
      styles={(info) => {
        const custom = typeof styles === 'function' ? styles(info) : styles
        return {
          ...custom,
          body: {
            padding: 12,
            minHeight: 0,
            overflow: fillBody ? 'hidden' : 'auto',
            ...(fillBody ? { display: 'flex', flexDirection: 'column' } : {}),
            ...custom?.body,
          },
          footer: { padding: '8px 12px', textAlign: 'right', ...custom?.footer },
        }
      }}
      footer={
        footer !== undefined ? (
          footer
        ) : onConfirm ? (
          <Space size={8}>
            {error && <HintIcon type="error" content={error} />}
            {!hideCancel && (
              <Button
                {...cancelButtonProps}
                disabled={busy || cancelButtonProps?.disabled}
                onClick={(event) => onClose?.(event)}
              >
                {cancelText ?? t('common.cancel')}
              </Button>
            )}
            <Button
              type="primary"
              {...confirmButtonProps}
              loading={busy}
              onClick={() => void submit()}
            >
              {confirmText ?? t('common.confirm')}
            </Button>
          </Space>
        ) : undefined
      }
    >
      {fillBody ? (
        <div
          className="table-fill-container"
          style={{
            display: 'flex',
            flexDirection: 'column',
            flex: 1,
            minHeight: 0,
            overflow: 'hidden',
          }}
        >
          {children}
        </div>
      ) : (
        children
      )}
    </Drawer>
  )
}
