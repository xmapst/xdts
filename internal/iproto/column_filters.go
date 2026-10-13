package iproto

import (
	"fmt"

	"github.com/xmapst/xdts/internal/rule"
	"google.golang.org/protobuf/proto"
)

// ColumnFiltersFromProtocol converts wire enums explicitly and transfers no
// mutable references into the pure compiler's input.
func ColumnFiltersFromProtocol(filters []*ColumnFilterRule) ([]rule.ColumnFilterRule, error) {
	out := make([]rule.ColumnFilterRule, 0, len(filters))
	for i, f := range filters {
		if f == nil {
			return nil, fmt.Errorf("column_filters[%d]: missing rule", i)
		}
		var action rule.FilterAction
		switch f.GetAction() {
		case FilterAction_FILTER_ACTION_INCLUDE:
			action = rule.FilterActionInclude
		case FilterAction_FILTER_ACTION_EXCLUDE:
			action = rule.FilterActionExclude
		default:
			return nil, fmt.Errorf("column_filters[%d]: invalid action", i)
		}
		out = append(out, rule.ColumnFilterRule{Schema: f.GetSchema(), Table: f.GetTable(), Action: action, Columns: append([]string(nil), f.GetColumns()...)})
	}
	return out, nil
}

func CompileColumnFilters(filters []*ColumnFilterRule, sensitive bool) (*rule.Plan, error) {
	input, err := ColumnFiltersFromProtocol(filters)
	if err != nil {
		return nil, err
	}
	return rule.Compile(rule.RuleSet{ColumnFilters: input}, rule.SideSource, sensitive)
}

// CloneColumnFilters gives each runtime or message independent ownership.
func CloneColumnFilters(filters []*ColumnFilterRule) []*ColumnFilterRule {
	if filters == nil {
		return nil
	}
	out := make([]*ColumnFilterRule, len(filters))
	for i, f := range filters {
		if f != nil {
			out[i] = proto.Clone(f).(*ColumnFilterRule)
		}
	}
	return out
}

func CloneDDLColumnPolicy(policy *DDLColumnPolicy) *DDLColumnPolicy {
	if policy == nil {
		return nil
	}
	return proto.Clone(policy).(*DDLColumnPolicy)
}

func ValidateDDLColumnPolicy(policy *DDLColumnPolicy) error {
	if policy == nil {
		return nil
	}
	_, err := CompileColumnFilters(policy.GetRules(), policy.GetSourceCaseSensitive())
	return err
}
