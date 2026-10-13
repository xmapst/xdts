// 两种能力（列可见性、导出）用同一个类型而不是拆两个，是因为目前所有接入方
// 都同时需要这两种能力，拆开只会让每个页面多写一遍 columns 类型标注。
import type { TableColumnType } from 'antd'

export type ManagedColumn<T> = TableColumnType<T> & {
  key: string
  /** 是否允许在"列设置"里隐藏；默认 true。主键列（名称/时间戳等行身份列）
   * 和"操作"列应显式设为 false——隐藏这两类列会让用户找不到行的身份或没法
   * 操作这一行，没有意义。 */
  hideable?: boolean
  /** 导出 CSV 时取该列的文本值；不提供时退化为 `String(record[dataIndex] ?? '')`。
   * `render` 返回的是 ReactNode，不能直接进 CSV，任何 Tag/链接/Popover 之类的
   * 复杂展示列必须显式提供，否则导出结果里这一列会是 "[object Object]" 或空。 */
  exportValue?: (record: T) => string | number
}
