import type { AlertProps } from 'antd'
import { HintIcon } from './HintIcon'

export function HintAlert({
  title,
  description,
  type = 'info',
  action,
  style,
  className,
}: AlertProps) {
  return (
    <span style={style} className={`hint-alert ${className ?? ''}`}>
      <HintIcon
        type={type}
        content={
          <>
            {title}
            {description && <div>{description}</div>}
          </>
        }
      />
      {action}
    </span>
  )
}
