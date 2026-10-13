package controller

import (
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/rule"
)

// 两个入参都可能是 nil（策略没有配置对应类别）：nil 时对应的 rule.RuleSet 字段留空
// （ObjectFilters 为空切片、Operation/Origin 为 nil），语义与 internal/rule 文档里
// "RuleSet{} 编译出全放行的恒等 Plan"一致，不需要在这里特殊处理成一个空结构体——
// nil 指针与"显式给一个全零结构体"在 rule.Compile 里的含义不同（nil 表示未配置，非
// nil 但字段全零表示"显式配置且全部关闭"，见 rule.OperationFilter 的文档），必须原样
// 保留这个区分，不能把 nil 硬转换成 &rule.OperationFilter{}。
//
// 只做纯粹的字段搬运与类型翻译，不做业务判断——业务语义（多条对象过滤规则如何评估、
// local_only 与 hops 的关系等）完全在 internal/rule 内部，本函数不重复实现、不提前
// 假设。枚举值翻译特意用显式 switch 而不是直接数值转换：两侧枚举当前数值恰好一一
// 对应，但那是巧合而不是契约（两个包分别演进，谁都没有承诺"数值对齐"这件事本身是
// 稳定的），直接转换一旦某一侧未来插入新枚举值就会静默错位，显式 switch 在这种情况下
// 至少会在 default 分支报错，而不是把一个新枚举值悄悄映射成另一个不相关的值。
func ruleSetFromSource(rules *iproto.SourceRuleSet, origin *iproto.OriginPolicy) (rule.RuleSet, error) {
	var rs rule.RuleSet

	if rules != nil {
		rows, err := iproto.RowFiltersFromProtocol(rules.GetRowFilters())
		if err != nil {
			return rule.RuleSet{}, err
		}
		rs.RowFilters = rows
		columns, err := iproto.ColumnFiltersFromProtocol(rules.GetColumnFilters())
		if err != nil {
			return rule.RuleSet{}, fmt.Errorf("controller: %w", err)
		}
		rs.ColumnFilters = columns
		filters := make([]rule.ObjectFilterRule, 0, len(rules.GetObjectFilters()))
		for i, f := range rules.GetObjectFilters() {
			action, err := filterActionFromProto(f.GetAction())
			if err != nil {
				return rule.RuleSet{}, fmt.Errorf("controller: object_filters[%d]: %w", i, err)
			}
			schemaMatch, err := matchTypeFromProto(f.GetSchemaMatch())
			if err != nil {
				return rule.RuleSet{}, fmt.Errorf("controller: object_filters[%d].schema_match: %w", i, err)
			}
			tableMatch, err := matchTypeFromProto(f.GetTableMatch())
			if err != nil {
				return rule.RuleSet{}, fmt.Errorf("controller: object_filters[%d].table_match: %w", i, err)
			}
			filters = append(filters, rule.ObjectFilterRule{
				Action:        action,
				SchemaMatch:   schemaMatch,
				SchemaPattern: f.GetSchemaPattern(),
				TableMatch:    tableMatch,
				TablePattern:  f.GetTablePattern(),
			})
		}
		rs.ObjectFilters = filters

		if op := rules.GetOperationFilter(); op != nil {
			rs.Operation = &rule.OperationFilter{
				Insert: op.GetInsert(),
				Update: op.GetUpdate(),
				Delete: op.GetDelete(),
				DDL:    op.GetDdl(),
			}
		}
	}

	if origin != nil {
		mode, err := originModeFromProto(origin.GetMode())
		if err != nil {
			return rule.RuleSet{}, fmt.Errorf("controller: origin_policy: %w", err)
		}
		rs.Origin = &rule.OriginPolicy{
			Mode:    mode,
			Centers: origin.GetCenters(),
			MaxHops: origin.GetMaxHops(),
		}
	}

	return rs, nil
}

// filterActionFromProto/matchTypeFromProto/originModeFromProto 是 iproto 枚举到
// internal/rule 对应枚举的显式翻译，理由见 ruleSetFromSource 文档"枚举值翻译特意用
// 显式 switch"一节。三个 UNSPECIFIED 分支都原样透传（翻译成 rule 侧对应的
// Unspecified 常量而不是报错）：是否拒绝"未指定"是 rule.Compile 自己的校验职责
// （MGR-STR-02"保存前校验"要求把问题说清楚，翻译层重复这个校验只会在两处维护同一条
// 规则、迟早不一致），本函数只负责把值域搬过去，不越权替 Compile 做决定。
func filterActionFromProto(a iproto.FilterAction) (rule.FilterAction, error) {
	switch a {
	case iproto.FilterAction_FILTER_ACTION_UNSPECIFIED:
		return rule.FilterActionUnspecified, nil
	case iproto.FilterAction_FILTER_ACTION_INCLUDE:
		return rule.FilterActionInclude, nil
	case iproto.FilterAction_FILTER_ACTION_EXCLUDE:
		return rule.FilterActionExclude, nil
	default:
		return 0, fmt.Errorf("未知的 iproto.FilterAction: %d", a)
	}
}

func matchTypeFromProto(t iproto.MatchType) (rule.MatchType, error) {
	switch t {
	case iproto.MatchType_MATCH_TYPE_UNSPECIFIED:
		return rule.MatchUnspecified, nil
	case iproto.MatchType_MATCH_TYPE_EXACT:
		return rule.MatchExact, nil
	case iproto.MatchType_MATCH_TYPE_GLOB:
		return rule.MatchGlob, nil
	case iproto.MatchType_MATCH_TYPE_REGEX:
		return rule.MatchRegex, nil
	default:
		return 0, fmt.Errorf("未知的 iproto.MatchType: %d", t)
	}
}

func originModeFromProto(m iproto.OriginPolicyMode) (rule.OriginPolicyMode, error) {
	switch m {
	case iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_UNSPECIFIED:
		return rule.OriginModeUnspecified, nil
	case iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_LOCAL_ONLY:
		return rule.OriginModeLocalOnly, nil
	case iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_INCLUDE_ORIGINS:
		return rule.OriginModeIncludeOrigins, nil
	case iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_EXCLUDE_ORIGINS:
		return rule.OriginModeExcludeOrigins, nil
	case iproto.OriginPolicyMode_ORIGIN_POLICY_MODE_ALL:
		return rule.OriginModeAll, nil
	default:
		return 0, fmt.Errorf("未知的 iproto.OriginPolicyMode: %d", m)
	}
}
