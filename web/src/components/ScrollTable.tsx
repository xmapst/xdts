import { useEffect, useRef } from 'react'
import { Table, type TableProps } from 'antd'

export function ScrollTable<RecordType extends object>({
  className,
  scroll,
  ...props
}: TableProps<RecordType>) {
  const container = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const element = container.current
    if (!element) return
    const owner = element.closest<HTMLElement>(
      '.table-fill-container, .pipeline-tab-pane, .ant-drawer-body, .app-content',
    )
    let frame = 0
    const measure = () => {
      const rect = element.getBoundingClientRect()
      const ownerRect = owner?.getBoundingClientRect()
      const pagination = element.querySelector<HTMLElement>('.ant-table-pagination')
      const title = element.querySelector<HTMLElement>('.ant-table-title')
      const footer = element.querySelector<HTMLElement>('.ant-table-footer')
      const fixedHeight = [pagination, title, footer].reduce((height, part) => {
        if (!part) return height
        const style = getComputedStyle(part)
        return (
          height +
          part.getBoundingClientRect().height +
          (parseFloat(style.marginTop) || 0) +
          (parseFloat(style.marginBottom) || 0)
        )
      }, 0)
      const fill = owner?.classList.contains('table-fill-container')
      const bottom =
        Math.min(ownerRect?.bottom ?? window.innerHeight, window.innerHeight) - 12
      const available = bottom - rect.top - fixedHeight
      const limit = fill ? available : Math.min(420, window.innerHeight * 0.55, available)
      const height = fill
        ? Math.max(0, limit)
        : Math.max(120, limit > 120 ? limit : window.innerHeight * 0.35)
      element.style.setProperty('--table-body-height', `${Math.floor(height)}px`)
    }
    const schedule = () => {
      cancelAnimationFrame(frame)
      frame = requestAnimationFrame(measure)
    }
    const observer = new ResizeObserver(schedule)
    observer.observe(element)
    if (owner) observer.observe(owner)
    window.addEventListener('resize', schedule)
    measure()
    return () => {
      cancelAnimationFrame(frame)
      observer.disconnect()
      window.removeEventListener('resize', schedule)
    }
  }, [])
  return (
    <div ref={container} className="scroll-table-region">
      <Table<RecordType>
        {...props}
        className={`scroll-table ${className ?? ''}`}
        scroll={{ ...scroll, x: scroll?.x ?? 'max-content', y: undefined }}
      />
    </div>
  )
}
