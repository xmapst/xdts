// Export the currently loaded results only. Labels must state the export scope.
import type { ManagedColumn } from './tableColumns'

/** RFC 4180：字段包含逗号、双引号或换行时才需要引号包裹；否则原样输出更可读。 */
function escapeCsvField(value: string): string {
  if (/[",\r\n]/.test(value)) {
    return `"${value.replaceAll('"', '""')}"`
  }
  return value
}

function resolveFieldValue<T>(column: ManagedColumn<T>, record: T): string | null {
  if (column.exportValue) {
    return String(column.exportValue(record))
  }
  // 没有 dataIndex 的列（比如"操作"列，只有 key 没有数据字段）导出时直接跳过
  // ——按钮/链接本来就没有可导出的文本值，不是遗漏。
  if (column.dataIndex === undefined) {
    return null
  }

  const raw: unknown = column.dataIndex
  const path: readonly unknown[] = Array.isArray(raw) ? raw : [raw]
  let value: unknown = record
  for (const key of path) {
    if (value === null || value === undefined) {
      value = undefined
      break
    }
    value = (value as Record<string, unknown>)[key as string]
  }
  return value === null || value === undefined ? '' : String(value)
}

/**
 * 把 `rows` 按 `columns`（已经过列可见性过滤后的那一份）序列化成 CSV 并触发
 * 浏览器下载。`filename` 不需要带 `.csv` 后缀，本函数会补上。
 */
export function exportRowsToCsv<T>(filename: string, columns: ManagedColumn<T>[], rows: T[]): void {
  const exportableColumns = columns.filter(
    (column) => column.exportValue !== undefined || column.dataIndex !== undefined,
  )

  const headerLine = exportableColumns
    .map((column) => escapeCsvField(String(column.title ?? column.key)))
    .join(',')
  const bodyLines = rows.map((record) =>
    exportableColumns
      .map((column) => escapeCsvField(resolveFieldValue(column, record) ?? ''))
      .join(','),
  )
  // 开头加 UTF-8 BOM：Excel（尤其是 Windows 上的版本）没有 BOM 时会按本地
  // 编码猜测文件编码，中文内容会被打开成乱码；加了 BOM 之后 Excel 才会正确
  // 识别为 UTF-8。
  const csvContent = '﻿' + [headerLine, ...bodyLines].join('\r\n')

  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  try {
    const link = document.createElement('a')
    link.href = url
    link.download = filename.endsWith('.csv') ? filename : `${filename}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
  } finally {
    URL.revokeObjectURL(url)
  }
}
