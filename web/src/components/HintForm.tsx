import { Form } from 'antd'
import type { FormItemProps } from 'antd'
import { cloneElement, isValidElement } from 'react'
import { HintIcon } from './HintIcon'

export function HintForm({
  extra,
  help,
  label,
  className,
  children,
  ...props
}: FormItemProps) {
  if (props.noStyle) return <Form.Item {...props}>{children}</Form.Item>
  const control =
    extra &&
    typeof label === 'string' &&
    isValidElement<{ 'aria-label'?: string }>(children)
      ? cloneElement(children, { 'aria-label': children.props['aria-label'] ?? label })
      : children
  return (
    <Form.Item
      {...props}
      label={
        extra && label ? (
          <span className="field-label-hint">
            {label}
            <HintIcon content={extra} />
          </span>
        ) : (
          label
        )
      }
      className={`hint-form-item ${className ?? ''}`}
      extra={extra && !label ? <HintIcon content={extra} /> : undefined}
      help={help}
    >
      {control}
    </Form.Item>
  )
}
