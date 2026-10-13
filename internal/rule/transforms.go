package rule

import (
	"errors"
	"math"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

var (
	ErrTransformConfig     = errors.New("rule: invalid or unsupported transform configuration")
	ErrTransformEvaluation = errors.New("rule: transform failed or exceeded limits")
)

type TransformKind uint8

const (
	TransformMask TransformKind = iota + 1
	TransformConstFill
	TransformTypeConvert
	TransformLua
	TransformPlugin
)

// TransformRule uses source names. Callers resolve target names after compiling
// their mapping, before constructing the plan used by a target writer.
type TransformRule struct {
	Schema, Table, Column string
	Kind                  TransformKind
	Params                map[string]string
}

type compiledTransform struct {
	schema, table, column string
	kind                  TransformKind
	pattern               *regexp.Regexp
	patternCost           int
	prefix, suffix        int
	targetType            string
	constant              any
	program               IETLProgram
}

// TransformPlan contains no mutable evaluation state. Each call returns a new
// slice and owns any byte values, including columns unaffected by a rule.
type TransformPlan struct {
	rules         []compiledTransform
	caseSensitive bool
	tableAliases  ITableAliasPlan
}

// ITableAliasPlan resolves physical tool tables to their business table. The
// implementation must be immutable and perform no IO during evaluation.
type ITableAliasPlan interface {
	LogicalTable(schema, table string) string
}

// WithTableAliases returns a new immutable view; the original plan continues
// evaluating exact physical names. Compiled rules remain independently owned.
func (p *TransformPlan) WithTableAliases(aliases ITableAliasPlan) *TransformPlan {
	if p == nil {
		return nil
	}
	return &TransformPlan{rules: slices.Clone(p.rules), caseSensitive: p.caseSensitive, tableAliases: aliases}
}

func CompileTransforms(rules []TransformRule, caseSensitive bool) (*TransformPlan, error) {
	registry, _ := NewETLRegistry(nil)
	return CompileTransformsWithRegistry(rules, caseSensitive, registry)
}

func CompileTransformsWithRegistry(rules []TransformRule, caseSensitive bool, registry *ETLRegistry) (*TransformPlan, error) {
	if len(rules) > 128 {
		return nil, ErrTransformConfig
	}
	p := &TransformPlan{caseSensitive: caseSensitive}
	seen := make(map[[3]string]bool, len(rules))
	nameValid := func(name string) bool {
		return name != "" && len(name) <= 256 && utf8.ValidString(name) && !strings.ContainsRune(name, 0)
	}
	for _, r := range rules {
		if !nameValid(r.Schema) || !nameValid(r.Table) || !nameValid(r.Column) {
			return nil, ErrTransformConfig
		}
		c := compiledTransform{schema: r.Schema, table: r.Table, column: strings.ToLower(r.Column), kind: r.Kind}
		if !caseSensitive {
			c.schema, c.table = strings.ToLower(c.schema), strings.ToLower(c.table)
		}
		key := [3]string{c.schema, c.table, c.column}
		if seen[key] {
			return nil, ErrTransformConfig
		}
		seen[key] = true
		for name, value := range r.Params {
			if len(value) > 4096 || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
				return nil, ErrTransformConfig
			}
			allowed := r.Kind == TransformMask && (name == "pattern" || name == "keep_prefix" || name == "keep_suffix") ||
				r.Kind == TransformConstFill && (name == "value" || name == "target_type") ||
				r.Kind == TransformTypeConvert && name == "target_type" ||
				r.Kind == TransformLua && name == "script" || r.Kind == TransformPlugin
			if !allowed {
				return nil, ErrTransformConfig
			}
		}
		switch r.Kind {
		case TransformLua:
			var err error
			c.program, err = CompileLuaETL(r.Params["script"])
			if err != nil {
				return nil, err
			}
		case TransformPlugin:
			params := make(map[string]string, len(r.Params))
			for name, value := range r.Params {
				if name != "plugin" {
					params[name] = value
				}
			}
			var err error
			c.program, err = registry.Compile(r.Params["plugin"], params)
			if err != nil {
				return nil, err
			}
		case TransformMask:
			var err error
			if text := r.Params["pattern"]; text != "" {
				c.patternCost = len(text)
				c.pattern, err = regexp.Compile(text)
				if err != nil {
					return nil, ErrTransformConfig
				}
			}
			for name, dest := range map[string]*int{"keep_prefix": &c.prefix, "keep_suffix": &c.suffix} {
				if text, present := r.Params[name]; present {
					*dest, err = strconv.Atoi(text)
					if err != nil || *dest < 0 || *dest > 1024 {
						return nil, ErrTransformConfig
					}
				}
			}
		case TransformConstFill, TransformTypeConvert:
			c.targetType = r.Params["target_type"]
			if c.targetType == "" && r.Kind == TransformConstFill {
				c.targetType = "text"
			}
			if !validTransformType(c.targetType) {
				return nil, ErrTransformConfig
			}
			if r.Kind == TransformConstFill {
				value, present := r.Params["value"]
				if !present {
					return nil, ErrTransformConfig
				}
				var err error
				c.constant, err = convertTransformScalar(value, c.targetType)
				if err != nil {
					return nil, ErrTransformConfig
				}
			}
		default:
			return nil, ErrTransformConfig
		}
		p.rules = append(p.rules, c)
	}
	return p, nil
}

func validTransformType(kind string) bool {
	switch kind {
	case "text", "int64", "uint64", "float64", "bool", "null":
		return true
	default:
		return false
	}
}

func (p *TransformPlan) HasTable(schema, table string) bool {
	if p == nil {
		return false
	}
	if p.tableAliases != nil {
		table = p.tableAliases.LogicalTable(schema, table)
	}
	if !p.caseSensitive {
		schema, table = strings.ToLower(schema), strings.ToLower(table)
	}
	for _, r := range p.rules {
		if r.schema == schema && r.table == table {
			return true
		}
	}
	return false
}

func (p *TransformPlan) Columns(schema, table string) []string {
	if p == nil {
		return nil
	}
	if p.tableAliases != nil {
		table = p.tableAliases.LogicalTable(schema, table)
	}
	if !p.caseSensitive {
		schema, table = strings.ToLower(schema), strings.ToLower(table)
	}
	var out []string
	for _, r := range p.rules {
		if r.schema == schema && r.table == table {
			out = append(out, r.column)
		}
	}
	return out
}

func (p *TransformPlan) Apply(schema, table string, columns []string, values []any) ([]any, error) {
	if p == nil || len(columns) != len(values) || len(columns) > 4096 {
		return nil, ErrTransformEvaluation
	}
	if p.tableAliases != nil {
		table = p.tableAliases.LogicalTable(schema, table)
	}
	out := make([]any, len(values))
	indexes := make(map[string]int, len(columns))
	work := 0
	for i, value := range values {
		name := strings.ToLower(columns[i])
		if name == "" || len(name) > 256 || !utf8.ValidString(name) {
			return nil, ErrTransformEvaluation
		}
		if _, exists := indexes[name]; exists {
			return nil, ErrTransformEvaluation
		}
		indexes[name] = i
		size, err := transformScalarSize(value)
		if err != nil || size > 1<<20 {
			return nil, ErrTransformEvaluation
		}
		work += size
		if work > 4<<20 {
			return nil, ErrTransformEvaluation
		}
		if raw, ok := value.([]byte); ok {
			out[i] = append([]byte(nil), raw...)
		} else {
			out[i] = value
		}
	}
	if !p.caseSensitive {
		schema, table = strings.ToLower(schema), strings.ToLower(table)
	}
	for _, r := range p.rules {
		if r.schema != schema || r.table != table {
			continue
		}
		i, exists := indexes[r.column]
		if !exists {
			return nil, ErrTransformEvaluation
		}
		size, _ := transformScalarSize(out[i])
		work += size * (1 + r.patternCost)
		if work > 16<<20 {
			return nil, ErrTransformEvaluation
		}
		var err error
		switch r.kind {
		case TransformLua, TransformPlugin:
			row := make(map[string]any, len(columns))
			for index, name := range columns {
				value := out[index]
				if raw, ok := value.([]byte); ok {
					value = append([]byte(nil), raw...)
				}
				row[strings.ToLower(name)] = value
			}
			out[i], err = r.program.Apply(out[i], row)
		case TransformConstFill:
			out[i] = r.constant
		case TransformMask:
			if out[i] == nil {
				continue
			}
			text, ok := out[i].(string)
			if !ok {
				return nil, ErrTransformEvaluation
			}
			if r.pattern == nil {
				out[i] = maskTransformText(text, r.prefix, r.suffix)
			} else {
				out[i] = r.pattern.ReplaceAllStringFunc(text, func(match string) string { return maskTransformText(match, r.prefix, r.suffix) })
			}
		case TransformTypeConvert:
			if out[i] != nil {
				out[i], err = convertTransformScalar(out[i], r.targetType)
			}
		}
		if err != nil {
			return nil, ErrTransformEvaluation
		}
	}
	total := 0
	for _, value := range out {
		size, err := transformScalarSize(value)
		if err != nil || size > 1<<20 {
			return nil, ErrTransformEvaluation
		}
		total += size
		if total > 4<<20 {
			return nil, ErrTransformEvaluation
		}
	}
	return out, nil
}

func maskTransformText(text string, prefix, suffix int) string {
	runes := []rune(text)
	prefix = min(prefix, len(runes))
	suffix = min(suffix, len(runes)-prefix)
	return string(runes[:prefix]) + strings.Repeat("*", len(runes)-prefix-suffix) + string(runes[len(runes)-suffix:])
}

func transformScalarSize(value any) (int, error) {
	switch value := value.(type) {
	case nil, bool, int64, uint64:
		return 8, nil
	case float64:
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			return 8, nil
		}
	case string:
		if utf8.ValidString(value) {
			return len(value), nil
		}
	case []byte:
		return len(value), nil
	}
	return 0, ErrTransformEvaluation
}

func convertTransformScalar(value any, kind string) (any, error) {
	if kind == "null" {
		return nil, nil
	}
	var text string
	switch value := value.(type) {
	case string:
		text = value
	case []byte:
		if !utf8.Valid(value) {
			return nil, ErrTransformEvaluation
		}
		text = string(value)
	case int64:
		text = strconv.FormatInt(value, 10)
	case uint64:
		text = strconv.FormatUint(value, 10)
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, ErrTransformEvaluation
		}
		text = strconv.FormatFloat(value, 'f', -1, 64)
	case bool:
		text = strconv.FormatBool(value)
	default:
		return nil, ErrTransformEvaluation
	}
	if kind == "text" {
		return text, nil
	}
	if text == "" || strings.TrimSpace(text) != text {
		return nil, ErrTransformEvaluation
	}
	switch kind {
	case "int64":
		v, err := strconv.ParseInt(text, 10, 64)
		if err == nil {
			return v, nil
		}
	case "uint64":
		v, err := strconv.ParseUint(text, 10, 64)
		if err == nil {
			return v, nil
		}
	case "float64":
		if integer, ok := new(big.Int).SetString(text, 10); ok && integer.BitLen() > 53 {
			return nil, ErrTransformEvaluation
		}
		switch v := value.(type) {
		case int64:
			if v > 1<<53 || v < -(1<<53) {
				return nil, ErrTransformEvaluation
			}
		case uint64:
			if v > 1<<53 {
				return nil, ErrTransformEvaluation
			}
		}
		v, err := strconv.ParseFloat(text, 64)
		if err == nil && !math.IsNaN(v) && !math.IsInf(v, 0) {
			return v, nil
		}
	case "bool":
		if text == "true" || text == "1" {
			return true, nil
		}
		if text == "false" || text == "0" {
			return false, nil
		}
	}
	return nil, ErrTransformEvaluation
}
