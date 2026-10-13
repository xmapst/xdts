package rule

import (
	"errors"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/conf"
	"github.com/expr-lang/expr/parser"
	"github.com/expr-lang/expr/vm"
)

var (
	ErrRowExpression = errors.New("rule: invalid or unsupported row expression")
	ErrRowEvaluation = errors.New("rule: row expression failed or exceeded limits")
	errRowDeadline   = errors.New("rule: row expression execution deadline exceeded")
)

type RowFilterRule struct {
	Schema     string
	Table      string
	Expression string
	Language   string
	Columns    []string
}

type compiledRowRule struct {
	schema, table iMatcher
	program       *vm.Program
	variables     []string
	lua           IETLProgram
}

type rowSandbox struct {
	err       error
	variables map[string]bool
}

// Static environment access supports column names that are not identifiers.
// It is rewritten to a scalar lookup before validation and compilation.
type rowColumnAccess struct{}

type rowNumericOperations struct{}

func (*rowNumericOperations) Visit(node *ast.Node) {
	n, ok := (*node).(*ast.BinaryNode)
	if ok && strings.Contains("+-*/%", n.Operator) && len(n.Operator) == 1 {
		ast.Patch(node, &ast.CallNode{Callee: &ast.IdentifierNode{Value: "_row_numeric_operation"}, Arguments: []ast.Node{&ast.StringNode{Value: n.Operator}, n.Left, n.Right}})
	}
	if ok && (n.Operator == "==" || n.Operator == "!=" || n.Operator == "<" || n.Operator == "<=" || n.Operator == ">" || n.Operator == ">=") {
		ast.Patch(node, &ast.CallNode{Callee: &ast.IdentifierNode{Value: "_row_scalar_compare"}, Arguments: []ast.Node{&ast.StringNode{Value: n.Operator}, n.Left, n.Right}})
	}
}

func compareRowScalars(args ...any) (any, error) {
	if len(args) != 3 {
		return nil, ErrRowEvaluation
	}
	operator, valid := args[0].(string)
	if !valid {
		return nil, ErrRowEvaluation
	}
	left, right := args[1], args[2]
	if left == nil || right == nil {
		if operator == "==" {
			return left == nil && right == nil, nil
		}
		if operator == "!=" {
			return left != nil || right != nil, nil
		}
		return nil, ErrRowEvaluation
	}
	var equal, less bool
	switch l := left.(type) {
	case string:
		r, ok := right.(string)
		if !ok {
			return nil, ErrRowEvaluation
		}
		equal, less = l == r, l < r
	case bool:
		r, ok := right.(bool)
		if !ok || operator != "==" && operator != "!=" {
			return nil, ErrRowEvaluation
		}
		equal = l == r
	default:
		leftNumber, err := rowNumber(left)
		if err != nil {
			return nil, err
		}
		r, err := rowNumber(right)
		if err != nil {
			return nil, err
		}
		equal, less = leftNumber == r, leftNumber < r
	}
	switch operator {
	case "==":
		return equal, nil
	case "!=":
		return !equal, nil
	case "<":
		return less, nil
	case "<=":
		return less || equal, nil
	case ">":
		return !less && !equal, nil
	case ">=":
		return !less, nil
	}
	return nil, ErrRowEvaluation
}

func numericRowOperation(args ...any) (any, error) {
	if len(args) != 3 {
		return nil, ErrRowEvaluation
	}
	x, err := rowNumber(args[1])
	if err != nil {
		return nil, err
	}
	y, err := rowNumber(args[2])
	if err != nil {
		return nil, err
	}
	operator, valid := args[0].(string)
	if !valid {
		return nil, ErrRowEvaluation
	}
	var result float64
	switch operator {
	case "+":
		result = x + y
	case "-":
		result = x - y
	case "*":
		result = x * y
	case "/":
		if y == 0 {
			return nil, ErrRowEvaluation
		}
		result = x / y
	case "%":
		if y == 0 {
			return nil, ErrRowEvaluation
		}
		result = math.Mod(x, y)
	default:
		return nil, ErrRowEvaluation
	}
	return rowNumber(result)
}

// Numeric values are restricted to the exact integer range of float64.
// This avoids expr's signed integer conversion and unchecked arithmetic wrap.
func rowNumber(value any) (float64, error) {
	var number float64
	switch v := value.(type) {
	case int:
		number = float64(v)
	case int8:
		number = float64(v)
	case int16:
		number = float64(v)
	case int32:
		number = float64(v)
	case int64:
		number = float64(v)
	case uint:
		number = float64(v)
	case uint8:
		number = float64(v)
	case uint16:
		number = float64(v)
	case uint32:
		number = float64(v)
	case uint64:
		number = float64(v)
	case float32:
		number = float64(v)
	case float64:
		number = v
	default:
		return 0, ErrRowEvaluation
	}
	if math.IsNaN(number) || math.IsInf(number, 0) || math.Abs(number) > 9007199254740991 {
		return 0, ErrRowEvaluation
	}
	return number, nil
}

func (*rowColumnAccess) Visit(node *ast.Node) {
	member, ok := (*node).(*ast.MemberNode)
	if !ok || member.Method || member.Optional {
		return
	}
	base, validBase := member.Node.(*ast.IdentifierNode)
	key, validKey := member.Property.(*ast.StringNode)
	if validBase && validKey && base.Value == "$env" && key.Value != "" && len(key.Value) <= 256 {
		ast.Patch(node, &ast.IdentifierNode{Value: strings.ToLower(key.Value)})
	}
}

func (v *rowSandbox) Visit(node *ast.Node) {
	switch n := (*node).(type) {
	case *ast.NilNode, *ast.BoolNode, *ast.StringNode:
	case *ast.IntegerNode:
		if _, err := rowNumber(n.Value); err != nil {
			v.err = ErrRowExpression
		}
	case *ast.FloatNode:
		if _, err := rowNumber(n.Value); err != nil {
			v.err = ErrRowExpression
		}
	case *ast.IdentifierNode:
		if n.Value == "$env" || n.Value == "" {
			v.err = ErrRowExpression
		}
		n.Value = strings.ToLower(n.Value)
		v.variables[n.Value] = true
	case *ast.UnaryNode:
		if n.Operator != "!" && n.Operator != "not" && n.Operator != "+" && n.Operator != "-" {
			v.err = ErrRowExpression
		}
	case *ast.BinaryNode:
		switch n.Operator {
		case "==", "!=", "<", "<=", ">", ">=", "and", "or", "&&", "||", "+", "-", "*", "/", "%", "??":
		default:
			v.err = ErrRowExpression
		}
	default:
		v.err = ErrRowExpression
	}
}

func compileRowFilters(rules []RowFilterRule, caseSensitive bool) ([]compiledRowRule, error) {
	if len(rules) > 128 {
		return nil, ErrRowExpression
	}
	result := make([]compiledRowRule, 0, len(rules))
	for _, rule := range rules {
		if rule.Schema == "" || rule.Table == "" || len(rule.Schema) > 256 || len(rule.Table) > 256 || !utf8.ValidString(rule.Schema) || !utf8.ValidString(rule.Table) || !utf8.ValidString(rule.Expression) || len(rule.Expression) > 4096 || strings.TrimSpace(rule.Expression) == "" {
			return nil, ErrRowExpression
		}
		config := conf.New(nil)
		if rule.Language == "lua" {
			if len(rule.Columns) == 0 || len(rule.Columns) > 200 {
				return nil, ErrRowExpression
			}
			seen := make(map[string]bool, len(rule.Columns))
			variables := make([]string, 0, len(rule.Columns))
			for _, name := range rule.Columns {
				name = strings.ToLower(name)
				if name == "" || len(name) > 256 || !utf8.ValidString(name) || strings.ContainsRune(name, 0) || seen[name] {
					return nil, ErrRowExpression
				}
				seen[name] = true
				variables = append(variables, name)
			}
			program, err := CompileLuaETL(rule.Expression)
			if err != nil {
				return nil, ErrRowExpression
			}
			result = append(result, compiledRowRule{schema: exactMatcher{rule.Schema, caseSensitive}, table: exactMatcher{rule.Table, caseSensitive}, lua: program, variables: variables})
			continue
		}
		if rule.Language != "" && rule.Language != "expr" || len(rule.Columns) != 0 {
			return nil, ErrRowExpression
		}
		config.MaxNodes = 256
		tree, err := parser.ParseWithConfig(rule.Expression, config)
		if err != nil {
			return nil, ErrRowExpression
		}
		sandbox := &rowSandbox{variables: map[string]bool{}}
		ast.Walk(&tree.Node, &rowColumnAccess{})
		ast.Walk(&tree.Node, sandbox)
		if sandbox.err != nil {
			return nil, sandbox.err
		}
		program, err := expr.Compile(rule.Expression, expr.Env(map[string]any{}), expr.AllowUndefinedVariables(), expr.AsBool(), expr.DisableAllBuiltins(), expr.Optimize(false), expr.MaxNodes(256), expr.Patch(&rowColumnAccess{}), expr.Patch(&rowSandbox{variables: map[string]bool{}}), expr.Patch(&rowNumericOperations{}), expr.Function("_row_numeric_operation", numericRowOperation), expr.Function("_row_scalar_compare", compareRowScalars))
		if err != nil || len(program.Bytecode) > 2048 {
			return nil, ErrRowExpression
		}
		variables := make([]string, 0, len(sandbox.variables))
		for name := range sandbox.variables {
			variables = append(variables, name)
		}
		sort.Strings(variables)
		result = append(result, compiledRowRule{schema: exactMatcher{rule.Schema, caseSensitive}, table: exactMatcher{rule.Table, caseSensitive}, program: program, variables: variables})
	}
	return result, nil
}

// MatchRow evaluates scalar values before column projection. The environment
// contains no objects or functions; each call owns its VM and local map.
func (p *Plan) HasRowFilters(schema, table string) bool {
	for _, rule := range p.rows {
		if rule.schema.MatchString(schema) && rule.table.MatchString(table) {
			return true
		}
	}
	return false
}

func (p *Plan) RowFilterColumns(schema, table string) []string {
	var result []string
	seen := map[string]bool{}
	for _, row := range p.rows {
		if !row.schema.MatchString(schema) || !row.table.MatchString(table) {
			continue
		}
		for _, name := range row.variables {
			if !seen[name] {
				result = append(result, name)
				seen[name] = true
			}
		}
	}
	return result
}

func (p *Plan) MatchRow(schema, table string, columns []string, values []any) (bool, error) {
	return p.matchRowWithClock(schema, table, columns, values, time.Now)
}

func (p *Plan) matchRowWithClock(schema, table string, columns []string, values []any, now func() time.Time) (bool, error) {
	if !p.HasRowFilters(schema, table) {
		return true, nil
	}
	if len(columns) != len(values) || len(columns) > 4096 {
		return false, ErrRowEvaluation
	}
	env := make(map[string]any, len(columns))
	size := 0
	for i, name := range columns {
		if len(name) == 0 || len(name) > 256 || !utf8.ValidString(name) {
			return false, ErrRowEvaluation
		}
		name = strings.ToLower(name)
		if _, exists := env[name]; exists {
			return false, ErrRowEvaluation
		}
		value := values[i]
		switch n := value.(type) {
		case nil, bool:
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			if n, ok := value.(float64); ok && (math.IsInf(n, 0) || math.IsNaN(n)) {
				return false, ErrRowEvaluation
			}
			if n, ok := value.(float32); ok && (math.IsInf(float64(n), 0) || math.IsNaN(float64(n))) {
				return false, ErrRowEvaluation
			}
		case string:
			size += len(n)
		case []byte:
			size += len(n)
			if size > 4*1024*1024 {
				return false, ErrRowEvaluation
			}
			value = string(n)
		default:
			return false, ErrRowEvaluation
		}
		size += len(name) + 16
		if size > 4*1024*1024 {
			return false, ErrRowEvaluation
		}
		env[name] = value
	}
	// Evaluation has no side effects. A scheduler or GC pause can consume the
	// wall-clock limit, so allow one fresh attempt on the same scalar environment.
	// All other limits and failures are final; neither attempt shares a VM.
	for attempt := range 2 {
		allowed, err := p.evaluateRowRules(schema, table, env, size, now)
		if !errors.Is(err, errRowDeadline) || attempt == 1 {
			return allowed, err
		}
	}
	return false, ErrRowEvaluation
}

func (p *Plan) evaluateRowRules(schema, table string, env map[string]any, size int, now func() time.Time) (bool, error) {
	deadline := now().Add(5 * time.Millisecond)
	workBudget := 16 * 1024 * 1024
	for _, rule := range p.rows {
		if !rule.schema.MatchString(schema) || !rule.table.MatchString(table) {
			continue
		}
		for _, name := range rule.variables {
			if _, exists := env[name]; !exists {
				return false, ErrRowEvaluation
			}
		}
		// Bound repeated string comparisons even when a large scalar is reused.
		if rule.lua != nil {
			inputs := make(map[string]any, len(rule.variables))
			for _, name := range rule.variables {
				inputs[name] = env[name]
			}
			result, err := rule.lua.Apply(nil, inputs)
			allowed, ok := result.(bool)
			if err != nil || !ok {
				return false, ErrRowEvaluation
			}
			if !allowed {
				return false, nil
			}
			continue
		}
		operations := max(1, len(rule.program.Bytecode))
		if size > workBudget/operations {
			return false, ErrRowEvaluation
		}
		if now().After(deadline) {
			return false, errors.Join(ErrRowEvaluation, errRowDeadline)
		}
		workBudget -= size * operations
		machine := vm.VM{MemoryBudget: 1024}
		normalized := make(map[string]any, len(env))
		for name, value := range env {
			switch value.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
				var err error
				value, err = rowNumber(value)
				if err != nil {
					return false, err
				}
			}
			normalized[name] = value
		}
		result, err := machine.Run(rule.program, normalized)
		if err != nil {
			return false, ErrRowEvaluation
		}
		if now().After(deadline) {
			return false, errors.Join(ErrRowEvaluation, errRowDeadline)
		}
		allowed, valid := result.(bool)
		if !valid {
			return false, ErrRowEvaluation
		}
		if !allowed {
			return false, nil
		}
	}
	return true, nil
}
