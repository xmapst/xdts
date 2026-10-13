// 跨资源域共用的类型。长列表（日志、事件、冲突等）必须分页或虚拟滚动，
// 不得一次性加载全量（需求 MGR-WEB-05），各资源域的列表查询统一走 Paginated<T>。

export interface PageQuery {
  page: number
  pageSize: number
}

export interface Paginated<T> {
  items: T[]
  total: number
  page: number
  pageSize: number
}
