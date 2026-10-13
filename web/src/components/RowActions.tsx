import { Divider, Dropdown, Popconfirm, Space, Typography } from 'antd'
import { EllipsisOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { isValidElement, type ReactNode } from 'react'

export interface RowAction {
  key: string
  label: ReactNode
  onClick?: () => void
  /** Hidden actions occupy no spacing slot. */
  visible?: boolean
  disabled?: boolean
  danger?: boolean
  /** Confirmed actions invoke onClick only after confirmation. */
  confirm?: { title: ReactNode; okText?: string; cancelText?: string }
}

export interface RowActionsProps {
  actions: RowAction[]

  maxInline?: number
}

function renderActionNode(action: RowAction): ReactNode {
  if (
    isValidElement(action.label) &&
    !action.onClick &&
    !action.confirm &&
    !action.disabled &&
    !action.danger
  ) {
    return action.label
  }
  if (action.danger) {
    return (
      <Typography.Link
        type="danger"
        disabled={action.disabled}
        onClick={action.confirm ? undefined : action.onClick}
      >
        {action.label}
      </Typography.Link>
    )
  }
  return (
    <a
      aria-disabled={action.disabled || undefined}
      onClick={action.confirm || action.disabled ? undefined : action.onClick}
    >
      {action.label}
    </a>
  )
}

function renderAction(action: RowAction): ReactNode {
  const node = renderActionNode(action)
  if (!action.confirm) {
    return node
  }
  return (
    <Popconfirm
      title={action.confirm.title}
      okText={action.confirm.okText}
      cancelText={action.confirm.cancelText}
      onConfirm={action.onClick}
      disabled={action.disabled}
    >
      {node}
    </Popconfirm>
  )
}

export function RowActions({ actions, maxInline = 2 }: RowActionsProps) {
  const { t } = useTranslation()
  const visible = actions.filter((action) => action.visible !== false)
  const dangerActions = visible.filter((action) => action.danger)
  const normalActions = visible.filter((action) => !action.danger)

  const candidateOverflow = normalActions.slice(maxInline)
  // Confirmed actions stay inline so the confirmation trigger remains accessible.
  const forceInline = candidateOverflow.filter((action) => action.confirm)
  const overflow = candidateOverflow.filter((action) => !action.confirm)
  const inline = [...normalActions.slice(0, maxInline), ...forceInline]

  return (
    <Space size="small">
      {inline.map((action) => (
        <span key={action.key}>{renderAction(action)}</span>
      ))}
      {overflow.length > 0 && (
        <Dropdown
          menu={{
            items: overflow.map((action) => ({
              key: action.key,
              label: action.label,
              disabled: action.disabled,
              onClick: action.onClick,
            })),
          }}
        >
          <a onClick={(event) => event.preventDefault()}>
            {t('common.more')} <EllipsisOutlined />
          </a>
        </Dropdown>
      )}
      {dangerActions.length > 0 && (inline.length > 0 || overflow.length > 0) && (
        <Divider orientation="vertical" style={{ margin: 0 }} />
      )}
      {dangerActions.map((action) => (
        <span key={action.key}>{renderAction(action)}</span>
      ))}
    </Space>
  )
}
