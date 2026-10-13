// 存的是"被隐藏的列 key 集合"而不是"可见的列 key 集合"：以后给某个表格新增
// 一列时，老用户本地存的偏好里不会有这一列，如果存的是"可见集合"，新列会被
// 误判成"用户之前选择隐藏"而默认不显示；存"隐藏集合"则天然默认可见，只有
// 用户明确点掉的列才会消失。
//
// localStorage 读写都包 try/catch：私密模式/存储配额耗尽/内容损坏都视为
// "没有保存过偏好"，退化为本次会话内的普通 state，不影响表格正常渲染。
import { useCallback, useMemo, useState } from 'react'
import type { ManagedColumn } from './tableColumns'

function storageKeyFor(tableId: string): string {
  return `xdts.tableColumns.${tableId}`
}

function readHiddenKeys(tableId: string): Set<string> {
  try {
    const raw = localStorage.getItem(storageKeyFor(tableId))
    if (!raw) {
      return new Set()
    }
    const parsed: unknown = JSON.parse(raw)
    return Array.isArray(parsed)
      ? new Set(parsed.filter((key): key is string => typeof key === 'string'))
      : new Set()
  } catch {
    return new Set()
  }
}

function writeHiddenKeys(tableId: string, hidden: Set<string>): void {
  try {
    localStorage.setItem(storageKeyFor(tableId), JSON.stringify([...hidden]))
  } catch {
    // 写入失败只影响"下次打开是否记得偏好"，不影响本次会话已经生效的选择。
  }
}

export interface ColumnPickerProps<T> {
  columns: ManagedColumn<T>[]
  hiddenKeys: string[]
  onChange: (hiddenKeys: string[]) => void
}

export interface UseColumnVisibilityResult<T> {
  /** 传给 `<Table columns={...}>` 的、已按当前隐藏偏好过滤后的列数组。 */
  visibleColumns: ManagedColumn<T>[]
  /** 传给 `<ColumnSettingsButton {...pickerProps} />` 的属性。 */
  pickerProps: ColumnPickerProps<T>
}

/**
 * `tableId` 必须在全仓唯一且跨版本稳定（它是 localStorage key 的一部分）——
 * 建议用页面/表格的路由路径或功能名，例如 `'nodes'`、`'strategies'`。
 */
export function useColumnVisibility<T>(
  tableId: string,
  columns: ManagedColumn<T>[],
): UseColumnVisibilityResult<T> {
  const [hiddenKeys, setHiddenKeys] = useState<Set<string>>(() => readHiddenKeys(tableId))

  const handleChange = useCallback(
    (nextHidden: string[]) => {
      const nextSet = new Set(nextHidden)
      setHiddenKeys(nextSet)
      writeHiddenKeys(tableId, nextSet)
    },
    [tableId],
  )

  const visibleColumns = useMemo(
    () => columns.filter((column) => column.hideable === false || !hiddenKeys.has(column.key)),
    [columns, hiddenKeys],
  )

  return {
    visibleColumns,
    pickerProps: { columns, hiddenKeys: [...hiddenKeys], onChange: handleChange },
  }
}
