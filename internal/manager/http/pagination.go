package http

import "strconv"

// paginated 对齐 web/src/api/types.ts 的 Paginated<T>：「分页
// 信封」一行要求所有列表端点统一 `{items, total, page, pageSize}`，本包
// 内任何需要分页的资源列表都应该复用这个类型，不要各自发明形状。
type paginated[T any] struct {
	Items    []T `json:"items"`
	Total    int `json:"total"`
	Page     int `json:"page"`
	PageSize int `json:"pageSize"`
}

// defaultListPageSize/maxListPageSize 是分页查询参数缺省/上限值，供全部
// 走 paginated[T] 的资源列表共用。
const (
	defaultListPageSize = 20
	maxListPageSize     = 200
)

// parsePositiveIntOrDefault 解析查询参数里的正整数，空值或解析失败/非正数
// 都退回 def——分页参数解析失败不应该 400，前端目前也没有对这类错误做
// 特殊处理，静默退回合理默认值更符合"分页控件传参错误不应该整页报错"的
// 体验。
func parsePositiveIntOrDefault(raw string, def int) int {
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return def
	}
	return v
}
