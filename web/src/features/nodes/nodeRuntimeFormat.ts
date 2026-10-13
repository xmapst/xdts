import { formatBytes, formatPercent } from '@/utils/format'

export function formatMemoryUsage(
  rssBytes: number | null,
  totalBytes: number | null,
  percent: number | null,
): string {
  const hasRss = rssBytes !== null
  const hasTotal = totalBytes !== null
  const hasPercent = percent !== null

  if (!hasRss && !hasTotal) {
    // 没有 RSS 与总量时，只展示已采集的百分比。
    return hasPercent ? formatPercent(percent) : '—'
  }

  // 至少采到一个数：缺的那个用 `—` 占位，采到的那个**必须显示出来**。
  const pair = `${formatBytes(rssBytes)} / ${formatBytes(totalBytes)}`

  if (hasRss && hasTotal) {
    // 分子分母都在时显式展示百分比，缺席时显示占位值。
    return `${pair} (${formatPercent(percent)})`
  }

  // 只采到一个：另一个数本身就未知，百分比的缺席**由它解释**，不再另加一个
  // `(—)` ——那会让人以为这里少的是三个独立的量。
  return hasPercent ? `${pair} (${formatPercent(percent)})` : pair
}
