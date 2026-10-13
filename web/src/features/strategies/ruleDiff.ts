// 版本对比的纯函数实现（MGR-STR-01：版本对比）。
import type { StrategyDiffEntry } from '@/api/strategies'

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) {
    return true
  }
  try {
    return JSON.stringify(a) === JSON.stringify(b)
  } catch {
    return false
  }
}

/**
 * 逐字段比较两份规则内容。对象递归展开成 "a.b.c" 路径；数组整体按值比较，
 * 不做元素级别的 diff（规则里的数组多是 include/exclude 清单，元素级 diff
 * 的可读性收益不高，整体展示「改成了什么」更直接）。
 */
export function diffRules(
  oldValue: unknown,
  newValue: unknown,
  basePath = '',
): StrategyDiffEntry[] {
  const entries: StrategyDiffEntry[] = []
  const oldObj = isPlainObject(oldValue) ? oldValue : undefined
  const newObj = isPlainObject(newValue) ? newValue : undefined

  if (oldObj && newObj) {
    const keys = new Set([...Object.keys(oldObj), ...Object.keys(newObj)])
    for (const key of keys) {
      const path = basePath ? `${basePath}.${key}` : key
      const hasOld = key in oldObj
      const hasNew = key in newObj
      if (!hasOld) {
        entries.push({ path, changeType: 'added', oldValue: undefined, newValue: newObj[key] })
      } else if (!hasNew) {
        entries.push({
          path,
          changeType: 'removed',
          oldValue: oldObj[key],
          newValue: undefined,
        })
      } else if (isPlainObject(oldObj[key]) && isPlainObject(newObj[key])) {
        entries.push(...diffRules(oldObj[key], newObj[key], path))
      } else if (!deepEqual(oldObj[key], newObj[key])) {
        entries.push({
          path,
          changeType: 'changed',
          oldValue: oldObj[key],
          newValue: newObj[key],
        })
      }
    }
    return entries
  }

  if (!deepEqual(oldValue, newValue)) {
    entries.push({ path: basePath || '(root)', changeType: 'changed', oldValue, newValue })
  }
  return entries
}
