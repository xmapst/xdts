// 跨页面共用的展示格式化工具（日期时间、字节数、百分比）。
// 服务端返回的时间一律为 ISO 字符串，数值字段可能为 null（未采集到）。

export function formatDateTime(value: string | null | undefined): string {
  if (!value) {
    return '—'
  }
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return '—'
  }
  return date.toLocaleString()
}

const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB'] as const

export function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined || Number.isNaN(bytes)) {
    return '—'
  }
  if (bytes <= 0) {
    return '0 B'
  }
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), BYTE_UNITS.length - 1)
  const value = bytes / 1024 ** exponent
  const unit = BYTE_UNITS[exponent] ?? 'B'
  return `${value.toFixed(exponent === 0 ? 0 : 1)} ${unit}`
}

export function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—'
  }
  return `${value.toFixed(1)}%`
}

/* Show absent counts as an em dash and preserve reported zero values. */
export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—'
  }
  return String(value)
}

export function formatWindowDays(totalSeconds: number): string {
  const days = totalSeconds / 86400
  return (Math.round(days * 10) / 10).toString()
}
