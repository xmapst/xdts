package iproto

import (
	"maps"
	"math"
	"strings"

	"github.com/xmapst/xdts/internal/rule"
	"google.golang.org/protobuf/proto"
)

func TransformsFromProtocol(input []*CleanseTransformRule) ([]rule.TransformRule, error) {
	if len(input) > 128 {
		return nil, rule.ErrTransformConfig
	}
	out := make([]rule.TransformRule, len(input))
	for i, item := range input {
		if item == nil || item.GetKind() < CleanseTransformKind_CLEANSE_TRANSFORM_KIND_MASK || item.GetKind() > CleanseTransformKind_CLEANSE_TRANSFORM_KIND_PLUGIN {
			return nil, rule.ErrTransformConfig
		}
		if side := item.GetExecutionSide(); side != TransformExecutionSide_TRANSFORM_EXECUTION_SIDE_UNSPECIFIED && side != TransformExecutionSide_TRANSFORM_EXECUTION_SIDE_SOURCE && side != TransformExecutionSide_TRANSFORM_EXECUTION_SIDE_TARGET {
			return nil, rule.ErrTransformConfig
		}
		out[i] = rule.TransformRule{Schema: item.Schema, Table: item.Table, Column: item.Column, Kind: rule.TransformKind(item.Kind), Params: maps.Clone(item.Params)}
	}
	return out, nil
}

func CompileSourceTransforms(input []*CleanseTransformRule, sensitive bool) (*rule.TransformPlan, error) {
	for _, item := range input {
		if item == nil || item.GetExecutionSide() != TransformExecutionSide_TRANSFORM_EXECUTION_SIDE_SOURCE {
			return nil, rule.ErrTransformConfig
		}
	}
	return CompileTransforms(input, sensitive)
}

func TargetTransformsFromProtocol(input []*CleanseTransformRule) ([]rule.TransformRule, error) {
	for _, item := range input {
		if item == nil || item.GetExecutionSide() == TransformExecutionSide_TRANSFORM_EXECUTION_SIDE_SOURCE {
			return nil, rule.ErrTransformConfig
		}
	}
	return TransformsFromProtocol(input)
}

func CompileTransforms(input []*CleanseTransformRule, sensitive bool) (*rule.TransformPlan, error) {
	rules, err := TransformsFromProtocol(input)
	if err != nil {
		return nil, err
	}
	return rule.CompileTransforms(rules, sensitive)
}

// TransformRow leaves the caller's row untouched and preserves the original
// encoding of columns that are not transformed (decimal and temporal included).
func TransformRow(plan *rule.TransformPlan, schema, table string, columns []string, row *Row) (*Row, error) {
	if plan == nil || !plan.HasTable(schema, table) || row == nil {
		return row, nil
	}
	if len(row.Values) != len(columns) {
		return nil, rule.ErrTransformEvaluation
	}
	values := make([]any, len(columns))
	for i, value := range row.Values {
		var err error
		values[i], err = RowFilterValue(value)
		if err != nil {
			return nil, rule.ErrTransformEvaluation
		}
	}
	result, err := plan.Apply(schema, table, columns, values)
	if err != nil {
		return nil, err
	}
	changed := make(map[string]bool)
	for _, name := range plan.Columns(schema, table) {
		changed[name] = true
	}
	out := proto.Clone(row).(*Row)
	for i, name := range columns {
		if !changed[strings.ToLower(name)] {
			continue
		}
		out.Values[i], err = transformColumnValue(result[i])
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func transformColumnValue(value any) (*ColumnValue, error) {
	out := &ColumnValue{}
	switch value := value.(type) {
	case nil:
		out.Value = &ColumnValue_IsNull{IsNull: true}
	case string:
		out.Value = &ColumnValue_Text{Text: value}
	case []byte:
		out.Value = &ColumnValue_RawBytes{RawBytes: append([]byte(nil), value...)}
	case int64:
		out.Value = &ColumnValue_I64{I64: value}
	case uint64:
		out.Value = &ColumnValue_U64{U64: value}
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, rule.ErrTransformEvaluation
		}
		out.Value = &ColumnValue_F64{F64: value}
	case bool:
		if value {
			out.Value = &ColumnValue_I64{I64: 1}
		} else {
			out.Value = &ColumnValue_I64{I64: 0}
		}
	default:
		return nil, rule.ErrTransformEvaluation
	}
	return out, nil
}
