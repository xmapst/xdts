package iproto

import (
	"errors"
	"strconv"

	"github.com/xmapst/xdts/internal/rule"
)

func RowFiltersFromProtocol(filters []*RowFilterRule) ([]rule.RowFilterRule, error) {
	if len(filters) > 128 {
		return nil, rule.ErrRowExpression
	}
	result := make([]rule.RowFilterRule, 0, len(filters))
	for _, filter := range filters {
		if filter == nil {
			return nil, rule.ErrRowExpression
		}
		result = append(result, rule.RowFilterRule{Schema: filter.GetSchema(), Table: filter.GetTable(), Expression: filter.GetExpression(), Language: filter.GetLanguage(), Columns: append([]string(nil), filter.GetColumns()...)})
	}
	return result, nil
}

func CompileRowFilters(filters []*RowFilterRule, sensitive bool) (*rule.Plan, error) {
	rows, err := RowFiltersFromProtocol(filters)
	if err != nil {
		return nil, err
	}
	return rule.Compile(rule.RuleSet{RowFilters: rows}, rule.SideSource, sensitive)
}

// RowFilterValue exposes the same scalar encoding for snapshot and binlog.
// Decimal and temporal microsecond values remain exact text.
func RowFilterValue(value *ColumnValue) (any, error) {
	if value == nil {
		return nil, errors.New("row filter: missing encoded value")
	}
	if value.GetZeroDate() {
		return "0000-00-00", nil
	}
	switch v := value.Value.(type) {
	case *ColumnValue_IsNull:
		if v.IsNull {
			return nil, nil
		}
	case *ColumnValue_I64:
		return v.I64, nil
	case *ColumnValue_U64:
		return v.U64, nil
	case *ColumnValue_F64:
		return v.F64, nil
	case *ColumnValue_Text:
		return v.Text, nil
	case *ColumnValue_RawBytes:
		return v.RawBytes, nil
	case *ColumnValue_Decimal:
		return v.Decimal, nil
	case *ColumnValue_TemporalUsec:
		return strconv.FormatInt(v.TemporalUsec, 10), nil
	}
	return nil, errors.New("row filter: unsupported encoded value")
}
