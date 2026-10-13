package rule

import "slices"

// Plan 是 Compile 的编译产物：一份策略在某一 side 下的可复用执行计划。零值不可用，只能通过 Compile 构造——字段全部未导出，外部只应该通过下面
// 的方法与它交互，不依赖内部字段形状（"编译产物，字段由实现决定"）。
type Plan struct {
	rows      []compiledRowRule
	side      Side
	objects   []compiledObjectRule
	columns   []compiledColumnRule
	operation *OperationFilter
	origin    *OriginPolicy
}

// MatchTable 决定某个库表是否在同步范围内（NODE-TF-02"对象过滤"，P0）。未配置
// object_filters 时放行全部表，与 binlogin.allowAllTableFilter 同一约定。
//
// 多条规则的评估算法（协议本身没有规定，是本包的判断，见 doc.go"未在协议里规定、
// 本包自行判断的语义"一节）：
//  1. 任意一条 EXCLUDE 规则命中，立即拒绝（deny 优先于 include，不看规则顺序）。
//  2. 规则列表里只要存在至少一条 INCLUDE 规则，默认行为变成"拒绝"，必须命中某条
//     INCLUDE 规则才放行。
//  3. 规则列表里一条 INCLUDE 都没有（只有 EXCLUDE，或列表为空）时默认放行。
func (p *Plan) MatchTable(schema, table string) bool {
	if len(p.objects) == 0 {
		return true
	}
	matched := false
	hasInclude := false
	for _, r := range p.objects {
		if r.action == FilterActionInclude {
			hasInclude = true
		}
		if !r.schema.MatchString(schema) {
			continue
		}
		if r.table != nil && !r.table.MatchString(table) {
			continue
		}
		if r.action == FilterActionExclude {
			return false
		}
		matched = true
	}
	if hasInclude {
		return matched
	}
	return true
}

// Allow applies exact source table rules. Includes affect only their own table.
func (p *Plan) Allow(schema, table, column string) bool {
	hasInclude, included := false, false
	for _, r := range p.columns {
		if !r.schema.MatchString(schema) || !r.table.MatchString(table) {
			continue
		}
		if r.action == FilterActionInclude {
			hasInclude = true
		}
		for _, name := range r.columns {
			if !name.MatchString(column) {
				continue
			}
			if r.action == FilterActionExclude {
				return false
			}
			included = true
		}
	}
	return !hasInclude || included
}

// FiltersTable identifies tables whose structural dependency operations need
// projection, including operations naming an index instead of its columns.
func (p *Plan) FiltersTable(schema, table string) bool {
	for _, r := range p.columns {
		if r.schema.MatchString(schema) && r.table.MatchString(table) && (r.action == FilterActionInclude || len(r.columns) != 0) {
			return true
		}
	}
	return false
}

// These switches apply only to incremental events. Snapshot readers consume the
// independent table/column filter interfaces and do not consult them.
func (p *Plan) AllowInsert() bool { return p.operation == nil || p.operation.Insert }
func (p *Plan) AllowUpdate() bool { return p.operation == nil || p.operation.Update }
func (p *Plan) AllowDelete() bool { return p.operation == nil || p.operation.Delete }

// AllowDDL 查询源端操作过滤的 DDL 开关。未配置 operation 时放行。
func (p *Plan) AllowDDL() bool {
	if p.operation == nil {
		return true
	}
	return p.operation.DDL
}

// hops 的含义沿用 binlogin.localOriginResolver 已经确立的约定：事务在本中心原生
// 产生（未经过任何联邦转发）记 hops=1，不是 hops=0——local_only 模式据此判定为
// hops<=1，不需要额外知道"本中心是谁"。max_hops（如果配置）作为叠加在 mode 之上
// 的独立约束，不属于任何一个具体 mode。
func (p *Plan) AllowOrigin(origin string, hops uint32, path []string) bool {
	_ = path

	if p.origin == nil {
		return true
	}
	if p.origin.MaxHops > 0 && hops > p.origin.MaxHops {
		return false
	}
	switch p.origin.Mode {
	case OriginModeAll:
		return true
	case OriginModeLocalOnly:
		return hops <= 1
	case OriginModeIncludeOrigins:
		return containsCenter(p.origin.Centers, origin)
	case OriginModeExcludeOrigins:
		return !containsCenter(p.origin.Centers, origin)
	default:
		// Compile 已经拒绝了 OriginModeUnspecified 与未知取值，运行到这里说明
		// Plan 不是经由 Compile 构造的（比如测试直接摆字段）——保守拒绝，不是
		// 放行：与本包其余"未知输入拒绝而不是放行"的判断一致。
		return false
	}
}

// containsCenter 是否命中 centers 列表；中心 code 是项目自己的短标识
// （如 "c0"），不是 MySQL 标识符，大小写敏感比较，不套用 schema/table 匹配的大小写
// 不敏感约定。
func containsCenter(centers []string, center string) bool {
	return slices.Contains(centers, center)
}
