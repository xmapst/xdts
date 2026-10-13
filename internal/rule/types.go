package rule

// 本文件定义 Compile 的输入形状（RuleSet 及其各规则类别）与 Plan 方法签名里出现的
// 值类型（Side/TableRef）。设计取舍见 doc.go；具体匹配/编译逻辑见 internal/rule/compile.go 与
// internal/rule/plan.go。
//
// 当前已实现对象、操作、来源、列过滤与行表达式。映射由实际写入路径执行，
// 清洗转换由 internal/rule/transforms.go 的 CompileTransforms 编译独立计划。

// Side 标识 Compile 是在为源端还是目标端编译执行计划。
//
// 这是 internal/rule 自己的类型，与 internal/manager/model.Side 同名但不共享、不
// 互相依赖——两者是各自模块的私有类型（internal/manager/model/side.go 的文档已经预先写明
// 这一点）。取值特意保持一致（"source"/"target"）只是减少读者的记忆负担，不代表
// 两者有耦合关系。
type Side string

const (
	SideSource Side = "source"
	SideTarget Side = "target"
)

// Valid 校验 side 取值本身合法，不代表某个 RuleSet 与该 side 匹配（那是 Compile 的
// 职责）。
func (s Side) Valid() bool {
	switch s {
	case SideSource, SideTarget:
		return true
	default:
		return false
	}
}

// TableRef 是重叠检测返回的库表标识。
type TableRef struct {
	Schema string
	Table  string
}

// MatchType 是模式串的匹配方式（对象过滤的 schema/table 模式）。显式保留零值
// MatchUnspecified 并在 Compile 里拒绝它，不把"没填"当某个具体匹配方式的隐式默认值。
type MatchType int

const (
	MatchUnspecified MatchType = iota
	MatchExact
	MatchGlob
	MatchRegex
)

// FilterAction 标识一条过滤规则是白名单（放行）还是黑名单（排除）。
type FilterAction int

const (
	FilterActionUnspecified FilterAction = iota
	FilterActionInclude
	FilterActionExclude
)

type ObjectFilterRule struct {
	Action        FilterAction
	SchemaMatch   MatchType
	SchemaPattern string
	TableMatch    MatchType
	TablePattern  string
}

// ColumnFilterRule matches one exact source table. Include lists are unioned;
// matching exclusions win. Column names are always case insensitive.
type ColumnFilterRule struct {
	Schema  string
	Table   string
	Action  FilterAction
	Columns []string
}

// OperationFilter 是 INSERT/UPDATE/DELETE/DDL 的独立开关，仅作用于增量（
// 操作过滤，P0，源端）。true=放行。零值 OperationFilter{} 全部为 false，因此
// RuleSet 用指针字段区分"未配置"（nil，默认放行全部操作）与"显式配置且全部关闭"
// （非 nil 但四个字段都是 false，默认拒绝全部操作）——语义完全不同，不能用零值
// 兼两种含义。
//
// DDL 通过 AllowDDL 查询，不经过行级过滤。
type OperationFilter struct {
	Insert bool
	Update bool
	Delete bool
	DDL    bool
}

// OriginPolicyMode 是来源策略的判定模式（需求 LOOP-03，P0，源端）。
type OriginPolicyMode int

const (
	OriginModeUnspecified OriginPolicyMode = iota
	OriginModeLocalOnly
	OriginModeIncludeOrigins
	OriginModeExcludeOrigins
	OriginModeAll
)

// OriginPolicy 是回环识别的来源策略。Centers 在 Mode 为 OriginModeIncludeOrigins/
// OriginModeExcludeOrigins 时生效，取值为中心 code；MaxHops 为 0 表示不限制跳数。
type OriginPolicy struct {
	Mode    OriginPolicyMode
	Centers []string
	MaxHops uint32
}

// RuleSet contains the supported source and target rules consumed by Compile.
type RuleSet struct {
	RowFilters    []RowFilterRule
	ObjectFilters []ObjectFilterRule
	ColumnFilters []ColumnFilterRule
	Operation     *OperationFilter
	Origin        *OriginPolicy
}
